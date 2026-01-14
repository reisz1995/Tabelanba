import requests
import os
from supabase import create_client

# Configurações de Ambiente (Puxadas do GitHub Actions)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_SPORTS_KEY = os.environ.get("API_SPORTS_KEY")

# Inicializa o cliente Supabase
db = create_client(SUPABASE_URL, SUPABASE_KEY)

def rodar():
    # URL Correta da API-Sports NBA
    url = "https://v1.basketball.api-sports.io/standings"
    
    # Parâmetros para a NBA temporada 2024-2025
    # Na API-Sports Basketball, a liga NBA tem ID específico
    querystring = {
        "league": "12",  # ID da NBA
        "season": "2024-2025"  # Formato correto da temporada
    }
    
    # Cabeçalho de autenticação
    headers = {
        "x-apisports-key": API_SPORTS_KEY
    }

    try:
        print("🏀 Iniciando busca de dados na API-Sports...")
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"URL requisitada: {response.url}")
        
        response.raise_for_status() 
        
        dados_json = response.json()
        
        # Debug: mostra a estrutura da resposta
        print(f"Resposta da API: {dados_json.keys()}")
        
        standings = dados_json.get('response', [])

        if not standings:
            print("⚠️ A API não retornou dados de classificação.")
            print(f"Resposta completa: {dados_json}")
            return

        lista_times = []

        for item in standings:
            # Extrai os dados de cada time
            team_data = item.get('team', {})
            stats = item.get('games', {})
            
            dados_formatados = {
                "time": team_data.get('name', 'Unknown'),
                "v": str(stats.get('win', {}).get('total', 0)),
                "d": str(stats.get('lose', {}).get('total', 0)),
                "pct": str(stats.get('win', {}).get('percentage', '0.000')),
                "ja": str(item.get('gamesBehind', '-')),
                "casa": f"{stats.get('win', {}).get('home', 0)}-{stats.get('lose', {}).get('home', 0)}",
                "visitante": f"{stats.get('win', {}).get('away', 0)}-{stats.get('lose', {}).get('away', 0)}",
                "div": f"{item.get('division', {}).get('name', 'N/A')} ({item.get('division', {}).get('rank', '-')})",
                "conf": f"{item.get('conference', {}).get('name', 'N/A')} ({item.get('conference', {}).get('rank', '-')})",
                "pts": "0", 
                "pts_contra": "0",
                "dif": "0",
                "strk": str(item.get('streak', '0'))
            }
            lista_times.append(dados_formatados)

        # Atualização no Supabase
        if lista_times:
            # 1. Limpa a tabela atual
            db.table("classificacao_nba").delete().neq("time", "vazio").execute()
            
            # 2. Insere os novos dados
            db.table("classificacao_nba").insert(lista_times).execute()
            
            print(f"✅ Sucesso! {len(lista_times)} times atualizados sem erros.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de requisição: {e}")
    except Exception as e:
        print(f"❌ Erro crítico: {e}")

if __name__ == "__main__":
    rodar()