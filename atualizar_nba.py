
import pandas as pd
from supabase import create_client
import os
import re

# O GitHub vai preencher essas variáveis para nós com segurança
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
db = create_client(URL, KEY)

def rodar():
    try:
        url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
        tabs = pd.read_html(url)
        df = pd.concat([tabs[0], tabs[1]], axis=1)

        # Limpeza para não pular times como o Thunder
        df = df[~df.iloc[:, 0].str.contains("Conferência|Leste|Oeste|CONF", case=False, na=False)]
        
        cols = ['time','v','d','pct','gb','conf','casa','fora','u10','stk']
        df = df.iloc[:, :10]
        df.columns = cols

        # Limpa o nome (remove o '1' de '1Thunder')
        df['time'] = df['time'].astype(str).str.replace(r'^\d+', '', regex=True).str.strip()
        df = df.fillna('0').astype(str)

        dados = df.to_dict(orient='records')

        # Atualiza o banco
        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        print(f"✅ Sucesso! {len(dados)} times atualizados.")
    except Exception as e:
        print(f"❌ Erro: {e}")
        exit(1) # Avisa o GitHub que deu erro

if __name__ == "__main__":
    rodar()
