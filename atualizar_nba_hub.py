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
        
        # Une as tabelas da ESPN (Nomes + Estatísticas)
        df = pd.concat([tabs[0], tabs[1]], axis=1)

        # --- LIMPEZA AVANÇADA PARA O THUNDER NÃO SUMIR ---
        # Remove linhas que contêm títulos de Conferência ou Divisão
        lixo = "Conferência|Leste|Oeste|CONF|DIV|Divisão|Noroeste|Pacífico|Sudoeste|Atlântico|Central|Sudeste"
        df = df[~df.iloc[:, 0].str.contains(lixo, case=False, na=False)]
        
        # Garante que temos exatamente 30 times e remove qualquer linha sobrando
        df = df.head(30).reset_index(drop=True)
        
        # SELEÇÃO DAS 13 COLUNAS EXATAS
        df = df.iloc[:, :13]
        cols = ['time','v','d','pct','ja','conf','casa','visitante','div', 'pts', 'pts_contra', 'dif','strk'']
        df.columns = cols
        
        # LIMPEZA DOS NOMES (Ex: '1Thunder' vira 'Thunder')
        # Remove números e espaços extras no início e fim
        df['time'] = df['time'].astype(str).str.replace(r'^\d+', '', regex=True).str.strip()

        # Resolve erro de JSON transformando tudo em string e limpando nulos
        df = df.fillna('0').astype(str)

        dados = df.to_dict(orient='records')

        # Atualiza o banco: deleta tudo e insere a lista limpa
        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times atualizados, incluindo o Thunder.")
    except Exception as e:
        print(f"❌ Erro: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
    
