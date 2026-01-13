import pandas as pd
from supabase import create_client
import os
import re

# Configuração de conexão
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
db = create_client(URL, KEY)

def rodar():
    try:
        url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
        tabs = pd.read_html(url)
        
        # Separamos as duas tabelas da ESPN
        df_nomes = tabs[0] # Coluna com os nomes dos times
        df_stats = tabs[1] # Colunas com V, D, PCT, etc.

        # --- PASSO 1: LIMPEZA INDEPENDENTE (O Segredo do Alinhamento) ---
        # Definimos o que é "lixo" que a ESPN joga no meio da tabela
        lixo = "Conferência|Leste|Oeste|CONF|DIV|Divisão|Noroeste|Pacífico|Sudoeste|Atlântico|Central|Sudeste"

        # Limpamos a tabela de nomes
        df_nomes = df_nomes[~df_nomes.iloc[:, 0].str.contains(lixo, case=False, na=False)]
        
        # Limpamos a tabela de estatísticas (removemos linhas onde 'V' não é número)
        # Isso remove os cabeçalhos repetidos "V", "D", "PCT"
        df_stats = df_stats[pd.to_numeric(df_stats.iloc[:, 0], errors='coerce').notnull()]

        # --- PASSO 2: RESET DE ÍNDICE E JUNÇÃO ---
        # Forçamos as duas tabelas a começarem do zero para alinharem perfeitamente
        df_nomes = df_nomes.reset_index(drop=True)
        df_stats = df_stats.reset_index(drop=True)

        # Juntamos as duas agora que estão alinhadas
        df = pd.concat([df_nomes, df_stats], axis=1)

        # --- PASSO 3: FORMATAÇÃO FINAL ---
        # Pegamos os 30 times e as 13 colunas do seu banco
        df = df.head(30)
        cols = ['time','v','d','pct','ja','casa','visitante','div','conf','pts','pts_contra','dif','strk',]
        df = df.iloc[:, :13]
        df.columns = cols

        # Limpeza do nome (Remove posição e sigla como '1OKC')
        def limpar_nome(nome):
            nome = re.sub(r'^\d+', '', str(nome)) # Remove número no início
            nome = re.sub(r'^[A-Z]{2,3}', '', nome) # Remove sigla (OKC, DET, etc)
            return nome.strip()

        df['time'] = df['time'].apply(limpar_nome)
        
        # Transforma tudo em String para o Supabase aceitar sem erros
        df = df.fillna('0').astype(str)
        dados = df.to_dict(orient='records')

        # --- PASSO 4: ENVIO ---
        # Limpa a tabela e insere os dados novos e alinhados
        db.table("classificacao_nba").delete().neq("time", "vazio").execute()
        db.table("classificacao_nba").insert(dados).execute()
        
        print(f"✅ Sucesso! {len(dados)} times alinhados e atualizados.")
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        exit(1)

if __name__ == "__main__":
    rodar()
