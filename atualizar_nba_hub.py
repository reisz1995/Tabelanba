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
        # Lê ambas as tabelas SEM processar cabeçalhos
        tabs = pd.read_html(url, header=None)
        
        df_nomes_raw = tabs[0].copy()
        df_stats_raw = tabs[1].copy()

        # --- ETAPA 1: LIMPEZA INICIAL ---
        # Remove linhas que contenham palavras típicas de cabeçalho
        palavras_invalidas = ['TIME', 'RK', 'TEAM', 'Conferência', 'Leste', 'Oeste']
        
        def linha_valida(row):
            texto = ' '.join(row.astype(str).values)
            return not any(palavra in texto for palavra in palavras_invalidas)
        
        df_nomes_raw = df_nomes_raw[df_nomes_raw.apply(linha_valida, axis=1)]
        df_stats_raw = df_stats_raw[df_stats_raw.apply(linha_valida, axis=1)]
        
        # Reset de índices após limpeza
        df_nomes_raw = df_nomes_raw.reset_index(drop=True)
        df_stats_raw = df_stats_raw.reset_index(drop=True)

        # --- ETAPA 2: ALINHAMENTO PELO PRIMEIRO NÚMERO VÁLIDO ---
        # Converte a primeira coluna de estatísticas (vitórias) para numérico
        vitorias_series = pd.to_numeric(df_stats_raw.iloc[:, 0], errors='coerce')
        
        # Encontra a primeira linha com um número >= 20 (todos os times da NBA têm isso)
        indices_validos = vitorias_series[vitorias_series >= 20].index
        
        if len(indices_validos) == 0:
            raise ValueError("Nenhuma linha válida encontrada nas estatísticas")
        
        start_stats = indices_validos[0]
        
        # Para os nomes, procuramos a linha correspondente que tenha texto válido
        # (não seja vazia e tenha pelo menos 5 caracteres)
        start_nomes = None
        for i in range(len(df_nomes_raw)):
            nome = str(df_nomes_raw.iloc[i, 0])
            if len(nome) >= 5 and not nome.isdigit():
                start_nomes = i
                break
        
        if start_nomes is None:
            raise ValueError("Nenhum nome de time válido encontrado")
        
        # Corta ambas as tabelas a partir dos pontos identificados
        df_nomes = df_nomes_raw.iloc[start_nomes:].reset_index(drop=True)
        df_stats = df_stats_raw.iloc[start_stats:].reset_index(drop=True)
        
        # Garante que ambos tenham o mesmo tamanho
        min_len = min(len(df_nomes), len(df_stats))
        df_nomes = df_nomes.iloc[:min_len]
        df_stats = df_stats.iloc[:min_len]

        # --- ETAPA 3: JUNÇÃO E SELEÇÃO ---
        df = pd.concat([df_nomes, df_stats], axis=1)
        
        # Mantém apenas linhas onde a coluna de vitórias é numérica
        df = df[pd.to_numeric(df.iloc[:, 1], errors='coerce').notnull()]
        
        # Seleciona os 30 times
        df = df.head(30).reset_index(drop=True)
        
        # Seleciona as 13 colunas necessárias
        df = df.iloc[:, :13]
        cols = ['time','v','d','pct','ja','casa','visitante','div','conf','pts','pts_contra','dif','strk']
        df.columns = cols

        # --- ETAPA 4: LIMPEZA DE NOMES ---
        def limpar_nome(nome):
            nome = str(nome)
            # Remove números iniciais
            nome = re.sub(r'^\d+', '', nome)
            # Remove siglas de 2-3 letras maiúsculas APENAS se seguidas por maiúscula
            nome = re.sub(r'^[A-Z]{2,3}(?=[A-Z])', '', nome)
            # Remove espaços extras
            nome = ' '.join(nome.split())
            return nome.strip()

        df['time'] = df['time'].apply(limpar_nome)
        
        # --- ETAPA 5: VALIDAÇÃO ---
        primeiro_time = df.iloc[0]['time']
        primeira_vitorias = int(df.iloc[0]['v'])
        
        print(f"🔍 Validação: 1º lugar é '{primeiro_time}' com {primeira_vitorias} vitórias")
        
        # O líder da NBA sempre tem mais de 30 vitórias nesta fase da temporada
        if primeira_vitorias < 30:
            raise ValueError(f"Alerta: O 1º lugar tem apenas {primeira_vitorias} vitórias - pode haver desalinhamento!")
        
        # --- ETAPA 6: ENVIO PARA SUPABASE ---
        df = df.fillna('0').astype(str)
        dados = df.to_dict(orient='records')

        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times atualizados")
        print(f"   Top 3: {df.iloc[0]['time']} ({df.iloc[0]['v']}V), "
              f"{df.iloc[1]['time']} ({df.iloc[1]['v']}V), "
              f"{df.iloc[2]['time']} ({df.iloc[2]['v']}V)")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    rodar()