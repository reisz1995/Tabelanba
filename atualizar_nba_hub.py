import pandas as pd
from supabase import create_client
import os
import re

URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
db = create_client(URL, KEY)

def rodar():
    try:
        url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
        tabs = pd.read_html(url)
        
        # Separamos as tabelas originais
        df_nomes_raw = tabs[0]
        df_stats_raw = tabs[1]

        # --- 1. LIMPEZA DA TABELA DE NOMES ---
        # Remove títulos (Leste, Oeste) e mantém apenas o que tem nome de time
        lixo = "Conferência|Leste|Oeste|CONF|DIV|Divisão"
        df_nomes = df_nomes_raw[~df_nomes_raw.iloc[:, 0].str.contains(lixo, case=False, na=False)].copy()
        
        # --- 2. LIMPEZA DA TABELA DE ESTATÍSTICAS ---
        # Mantém apenas as linhas onde a primeira coluna (Vitórias) é um NÚMERO
        # Isso remove cabeçalhos repetidos (V, D, PCT) que causam o desalinhamento
        df_stats = df_stats_raw[pd.to_numeric(df_stats_raw.iloc[:, 0], errors='coerce').notnull()].copy()

        # --- 3. ALINHAMENTO ---
        # Resetamos os índices para que a Linha 0 de um seja a Linha 0 do outro
        df_nomes = df_nomes.reset_index(drop=True)
        df_stats = df_stats.reset_index(drop=True)

        # Juntamos as duas partes agora que estão garantidamente alinhadas
        df = pd.concat([df_nomes, df_stats], axis=1)
        
        # Pegamos as 13 colunas e os 30 times
        df = df.head(30)
        cols = ['time','v','d','pct','ja','casa','visitante','div','conf','pts','pts_contra','dif','strk']
        df = df.iloc[:, :13]
        df.columns = cols

        # --- 4. CORREÇÃO DOS NOMES (Sem apagar o S de San Antonio) ---
        def limpar_nome(nome):
            nome = str(nome)
            # 1. Remove números da posição (ex: '1', '15')
            nome = re.sub(r'^\d+', '', nome)
            # 2. Remove as siglas grudadas (ex: 'OKC', 'DET') sem estragar o nome real
            # Procuramos letras maiúsculas seguidas por outra maiúscula (início da sigla + início do nome)
            nome = re.sub(r'^[A-Z]{2,3}(?=[A-Z][a-z])', '', nome)
            return nome.strip()

        df['time'] = df['time'].apply(limpar_nome)

        # Converte para string e envia ao Supabase
        df = df.fillna('0').astype(str)
        dados = df.to_dict(orient='records')

        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! Thunder e todos os times alinhados corretamente.")
    except Exception as e:
        print(f"❌ Erro: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
