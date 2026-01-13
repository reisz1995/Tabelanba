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
        # Forçamos header=None para garantir que o Thunder (1º lugar) não seja ignorado
        tabs = pd.read_html(url, header=None)
        
        df_nomes_raw = tabs[0] # Coluna de nomes
        df_stats_raw = tabs[1] # Colunas de estatísticas (V, D, PCT...)

        # --- PASSO 1: SINCRONIZAÇÃO PELO PRIMEIRO NÚMERO ---
        # Procuramos a primeira linha onde a coluna de vitórias é realmente um número
        vitorias_col = pd.to_numeric(df_stats_raw.iloc[:, 0], errors='coerce')
        start_index = vitorias_col.notnull().idxmax()
        
        # Cortamos ambas as tabelas para começarem exatamente no mesmo ponto
        df_nomes = df_nomes_raw.iloc[start_index:].reset_index(drop=True)
        df_stats = df_stats_raw.iloc[start_index:].reset_index(drop=True)

        # Juntamos agora que estão garantidamente alinhadas
        df = pd.concat([df_nomes, df_stats], axis=1)

        # --- PASSO 2: LIMPEZA DE LINHAS EXTRAS ---
        # Mantemos apenas as linhas que têm números (remove cabeçalhos de conferência no meio)
        df = df[pd.to_numeric(df.iloc[:, 1], errors='coerce').notnull()]
        
        # Selecionamos os 30 times da NBA
        df = df.head(30).reset_index(drop=True)

        # Nomeação das 13 colunas
        df = df.iloc[:, :13]
        cols = ['time','v','d','pct','ja','casa','visitante','div','conf','pts','pts_contra','dif','strk']
        df.columns = cols

        # --- PASSO 3: CORREÇÃO DE NOMES ---
        def limpar_nome(nome):
            nome = str(nome)
            # Remove o número da posição (ex: '1', '15')
            nome = re.sub(r'^\d+', '', nome)
            # Remove siglas (OKC, DET) APENAS se houver outra letra maiúscula colada
            # Isso mantém o 'S' de San Antonio mas remove o 'OKC' de OKCOklahoma
            nome = re.sub(r'^[A-Z]{2,3}(?=[A-Z])', '', nome)
            return nome.strip()

        df['time'] = df['time'].apply(limpar_nome)
        
        # Log para conferência no GitHub Actions
        print(f"Primeiro time alinhado: {df.iloc[0]['time']} com {df.iloc[0]['v']} vitórias")

        # Envio para o Supabase
        df = df.fillna('0').astype(str)
        dados = df.to_dict(orient='records')

        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times atualizados e alinhados corretamente.")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
