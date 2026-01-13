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
        
        # Une as tabelas da ESPN
        df = pd.concat([tabs[0], tabs[1]], axis=1)

        # Remove linhas de título para não pular times como o Thunder
        df = df[~df.iloc[:, 0].str.contains("Conferência|Leste|Oeste|CONF", case=False, na=False)]
        
        # SELEÇÃO DAS 13 COLUNAS
        # Aqui pegamos as 13 primeiras e damos exatamente 13 nomes
        df = df.iloc[:, :13]
        cols = ['time','v','d','pct','ja','conf','casa','visitante','div', 'pts', 'pts_contra', 'dif','strk',]
        df.columns = cols
        
        # Resolve erro de JSON transformando tudo em string
        df = df.fillna('0').astype(str)

        dados = df.to_dict(orient='records')

        # Atualiza o banco (Limpa e insere novos)
        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times atualizados.")
    except Exception as e:
        print(f"❌ Erro: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
    
