import requests
import os
from supabase import create_client

# Configurações extraídas do GitHub Secrets
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_SPORTS_KEY = os.environ.get("API_SPORTS_KEY") # Sua chave direta da API-Sports

db = create_client(SUPABASE_URL, SUPABASE_KEY)

def rodar():
    # URL Direta da API-Sports (sem o domínio rapidapi)
    url = "https://v1.nba.api-sports.io/standings"
    
    # Parâmetros: 'standard' para NBA e a temporada 2024 (que cobre 2024-25)
    querystring = {"league": "standard", "season": "2024"}
    
    # Cabeçalho de autenticação direto da API-Sports
    headers = {
        "x-apisports-key": API_SPORTS_KEY
    }

    try:
        print("🚀 Conectando diretamente à API-Sports...")
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        
        # A API-Sports retorna os dados dentro da chave 'response'
        standings = data.get('response', [])
        
        if not standings:
            print("⚠️ Aviso: Nenhum dado retornado. Verifique sua chave ou os parâmetros.")
            return

        lista_times = []

        for item in standings:
            # Mapeamento profissional para suas 13 colunas
            # Aqui os dados de vitórias e nomes estão 'amarrados' no mesmo objeto
            time_data = {
                "time": item['team']['name'],
                "v": str(item['win']['total']),
                "d": str(item['loss']['total']),
                "pct": str(item['win']['percentage']),
                "ja": str(item['gamesBehind'] if item['gamesBehind'] else '-'),
                "casa": f"{item['win']['home']}-{item['loss']['home']}",
                "visitante": f"{item['win']['away']}-{item['loss']['away']}",
                "div": f"{item['division']['name']} ({item['division']['rank']})",
                "conf": f"{item['conference']['name']} ({item['conference']['rank']})",
                "pts": "0", 
                "pts_contra": "0",
                "dif": "0",
                "strk": str(item['streak']) if item['streak'] else '0'
            }
            lista_times.append(time_data)

        # Atualização no Supabase
        if lista_times:
            # Limpa os dados antigos
            db.table("classificacao_nba").delete().neq("time", "vazio").execute()
            # Insere os novos dados alinhados da API oi jc
            db.table("classificacao_nba").insert(lista_times).execute()
            
            print(f"✅ Sucesso! {len(lista_times)} times atualizados diretamente via API-Sports.")
            # O Thunder agora terá os dados dele na linha dele.

    except Exception as e:
        print(f"❌ Erro na integração: {e}")

if __name__ == "__main__":
    rodar()
