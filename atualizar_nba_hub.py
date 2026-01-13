import requests
import os
from supabase import create_client

# Configurações de Segurança
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("NBA_API_KEY")

db = create_client(SUPABASE_URL, SUPABASE_KEY)

def rodar():
    url = "https://api-nba-v1.p.rapidapi.com/standings"
    querystring = {"league": "standard", "season": "2023"} # Use a temporada atual
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "api-nba-v1.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        dados_api = response.json()['response']

        lista_times = []
        for item in dados_api:
            # Aqui mapeamos os dados da API exatamente para suas colunas do Supabase
            time_data = {
                "time": item['team']['name'],
                "v": str(item['win']['total']),
                "d": str(item['loss']['total']),
                "pct": str(item['win']['percentage']),
                "ja": str(item['gamesBehind'] if item['gamesBehind'] else '-'),
                "casa": f"{item['win']['home']}-{item['loss']['home']}",
                "visitante": f"{item['win']['away']}-{item['loss']['away']}",
                "div": f"{item['win']['lastTen']}-{item['loss']['lastTen']}", # Exemplo usando Last 10
                "conf": f"{item['conference']['name']} - {item['conference']['rank']}",
                "pts": str(item['win']['percentage']), # API as vezes não dá pontos médios direto
                "pts_contra": "0",
                "dif": "0",
                "strk": str(item['streak'])
            }
            lista_times.append(time_data)

        # Atualiza o Supabase
        if lista_times:
            db.table("classificacao_nba").delete().neq("time", "vazio").execute()
            db.table("classificacao_nba").insert(lista_times).execute()
            print(f"✅ Sucesso! {len(lista_times)} times atualizados via API.")

    except Exception as e:
        print(f"❌ Erro na API: {e}")

if __name__ == "__main__":
    rodar()
