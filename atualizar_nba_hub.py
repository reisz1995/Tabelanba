import pandas as pd
from supabase import create_client
import os
import re

# Configuração de acesso
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
db = create_client(URL, KEY)

def rodar():
    try:
        url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
        # Usamos header=None para garantir que o Thunder (1º lugar) não seja tratado como cabeçalho e apagado
        tabs = pd.read_html(url, header=None)
        
        df_nomes_raw = tabs[0] # Tabela de nomes
        df_stats_raw = tabs[1] # Tabela de estatísticas

        # --- PASSO 1: SINCRONIZAÇÃO TOTAL ---
        # Procuramos a primeira linha onde a coluna de vitórias é um número
        # Isso ignora textos como "Leste", "Conferência" ou "V D PCT" que a ESPN coloca no topo
        vitorias_col = pd.to_numeric(df_stats_raw.iloc[:, 0], errors='coerce')
        start_index = vitorias_col.notnull().idxmax()
        
        # Cortamos ambas as tabelas exatamente no mesmo ponto de partida
        df_nomes = df_nomes_raw.iloc[start_index:].reset_index(drop=True)
        df_stats = df_stats_raw.iloc[start_index:].reset_index(drop=True)

        # Juntamos as duas agora que estão garantidamente alinhadas linha a linha
        df = pd.concat([df_nomes, df_stats], axis=1)

        # --- PASSO 2: FILTRAGEM E LIMPEZA ---
        # Mantemos apenas as linhas que são equipas reais (onde vitórias é número)
        df = df[pd.to_numeric(df.iloc[:, 1], errors='coerce').notnull()]
        
        # Selecionamos os 30 clubes da NBA
        df = df.head(30).reset_index(drop=True)

        # [span_3](start_span)Nomeação correta das 13 colunas[span_3](end_span)
        df = df.iloc[:, :13]
        cols = ['time','v','d','pct','ja','casa','visitante','div','conf','pts','pts_contra','dif','strk']
        df.columns = cols

        # --- PASSO 3: CORREÇÃO DE NOMES (Sem estragar o 'San Antonio') ---
        def limpar_nome(nome):
            nome = str(nome)
            # Remove o número da posição inicial (ex: '1', '15')
            nome = re.sub(r'^\d+', '', nome)
            # [span_4](start_span)[span_5](start_span)Remove siglas (OKC, DET, SA) apenas se coladas ao nome (ex: SASan Antonio -> San Antonio)[span_4](end_span)[span_5](end_span)
            nome = re.sub(r'^[A-Z]{2,3}(?=[A-Z])', '', nome)
            return nome.strip()

        df['time'] = df['time'].apply(limpar_nome)
        
        # Logs para conferir no GitHub Actions
        print(f"Primeiro clube: {df.iloc[0]['time']} | Vitórias: {df.iloc[0]['v']}")

        # Envio para o Supabase
        df = df.fillna('0').astype(str)
        dados = df.to_dict(orient='records')

        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} equipas alinhadas e atualizadas.")
        
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
