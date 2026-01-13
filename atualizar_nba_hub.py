import pandas as pd
from supabase import create_client
import os
import re

# Configuração de segurança via GitHub Secrets
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
db = create_client(URL, KEY)

def rodar():
    try:
        url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
        tabs = pd.read_html(url)
        
        # Une as tabelas (Nomes dos times + Estatísticas)
        df = pd.concat([tabs[0], tabs[1]], axis=1)

        # --- FILTRO DEFINITIVO ---
        # 1. Remove linhas que contêm títulos de texto conhecidos
        lixo = "Conferência|Leste|Oeste|CONF|DIV|Divisão|Noroeste|Pacífico|Sudoeste|Atlântico|Central|Sudeste"
        df = df[~df.iloc[:, 0].str.contains(lixo, case=False, na=False)]
        
        # 2. O PULO DO GATO: Mantém apenas onde a 2ª coluna (Vitórias) é um número.
        # Isso elimina as linhas de cabeçalho repetidas ("V", "D", "PCT")
        df = df[pd.to_numeric(df.iloc[:, 1], errors='coerce').notnull()]
        
        # 3. Garante que temos os 30 times e reseta o índice
        df = df.head(30).reset_index(drop=True)
        
        # Use exatamente esta ordem de 13 colunas no seu script Python           colsb = ['time','v','d','pct','ja','casa','visitante', 'div', 'conf', 'pts', 'pts_contra', 'dif', 'strk',]
        df = df.iloc[:, :13]
        df.columns = cols

        
        # LIMPEZA DOS NOMES (Ex: '1OKCOklahoma City' -> 'Oklahoma City')
        # Remove números no início e siglas de 2 ou 3 letras maiúsculas coladas
        df['time'] = df['time'].astype(str).str.replace(r'^\d+|[A-Z]{2,3}', '', regex=True).str.strip()

        # Converte tudo para string para evitar erros de JSON no Supabase
        df = df.fillna('0').astype(str)

        dados = df.to_dict(orient='records')

        # Atualiza o Supabase: Deleta o antigo e insere o novo
        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times atualizados no banco de dados.")
    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()