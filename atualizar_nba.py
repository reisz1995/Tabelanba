import requests
import os
from supabase import create_client

# Configurações de Ambiente (Puxadas do GitHub Actions)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BALLDONTLIE_API_KEY = os.environ.get("BALLDONTLIE_API_KEY")

# Inicializa o cliente Supabase
db = create_client(SUPABASE_URL, SUPABASE_KEY)

def calcular_classificacao():
    """
    Ball Don't Lie API - busca jogos da temporada e calcula a classificação
    """
    
    # URL da API Ball Don't Lie v1
    base_url = "https://api.balldontlie.io/v1"
    
    # Headers com autenticação
    headers = {
        "Authorization": BALLDONTLIE_API_KEY
    }
    
    # Temporada atual 2025-2026 (representada como 2024 na API)
    temporada_atual = 2025
    
    try:
        print("🏀 Iniciando busca de dados na Ball Don't Lie API...")
        print(f"📅 Temporada: 2025-2026")
        
        # Primeiro, buscar todos os times
        print("📋 Buscando lista de times...")
        teams_response = requests.get(f"{base_url}/teams", headers=headers, timeout=10)
        teams_response.raise_for_status()
        teams_data = teams_response.json()
        teams = teams_data.get('data', [])
        
        print(f"✅ {len(teams)} times encontrados!")
        
        # Inicializar estatísticas dos times
        stats = {}
        for team in teams:
            team_id = team['id']
            conference = team.get('conference', 'Unknown')
            division = team.get('division', 'Unknown')
            
            # Pular times sem conferência definida
            if not conference or conference == 'Unknown':
                continue
            
            stats[team_id] = {
                'time': team['full_name'],
                'cidade': team['city'],
                'nome': team['name'],
                'conference': conference,
                'division': division,
                'v': 0,
                'd': 0,
                'casa_v': 0,
                'casa_d': 0,
                'fora_v': 0,
                'fora_d': 0,
                'sequencia': []
            }
        
        # Buscar jogos da temporada atual (com paginação)
        print("📊 Buscando resultados dos jogos da temporada 2024-2025...")
        page = 1
        total_jogos = 0
        
        while True:
            games_url = f"{base_url}/games"
            params = {
                'seasons[]': temporada_atual,
                'per_page': 100,
                'page': page
            }
            
            games_response = requests.get(games_url, headers=headers, params=params, timeout=10)
            games_response.raise_for_status()
            games_data = games_response.json()
            
            games = games_data.get('data', [])
            if not games:
                break
            
            # Processar cada jogo finalizado
            for game in games:
                # Só contar jogos finalizados
                if game.get('status') != 'Final':
                    continue
                    
                home_team_id = game['home_team']['id']
                visitor_team_id = game['visitor_team']['id']
                
                # Pular se algum time não está nas estatísticas
                if home_team_id not in stats or visitor_team_id not in stats:
                    continue
                
                home_score = game['home_team_score']
                visitor_score = game['visitor_team_score']
                
                if home_score > visitor_score:
                    # Time da casa venceu
                    stats[home_team_id]['v'] += 1
                    stats[home_team_id]['casa_v'] += 1
                    stats[home_team_id]['sequencia'].append('W')
                    
                    stats[visitor_team_id]['d'] += 1
                    stats[visitor_team_id]['fora_d'] += 1
                    stats[visitor_team_id]['sequencia'].append('L')
                else:
                    # Time visitante venceu
                    stats[visitor_team_id]['v'] += 1
                    stats[visitor_team_id]['fora_v'] += 1
                    stats[visitor_team_id]['sequencia'].append('W')
                    
                    stats[home_team_id]['d'] += 1
                    stats[home_team_id]['casa_d'] += 1
                    stats[home_team_id]['sequencia'].append('L')
                
                total_jogos += 1
            
            print(f"  Página {page} processada ({len(games)} jogos)...")
            
            # Verificar se há mais páginas
            meta = games_data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
        
        print(f"✅ Total de {total_jogos} jogos processados da temporada 2025-2026!")
        
        # Calcular estatísticas finais
        lista_times = []
        
        # Agrupar por conferência para calcular "jogos atrás"
        conferencias = {'East': [], 'West': []}
        
        for team_id, team_stats in stats.items():
            total_jogos_time = team_stats['v'] + team_stats['d']
            
            if total_jogos_time > 0:
                pct = team_stats['v'] / total_jogos_time
            else:
                pct = 0.000
            
            # Pegar últimos 10 jogos para streak
            ultimos_jogos = team_stats['sequencia'][-10:]
            if ultimos_jogos:
                # Calcular streak atual
                streak_char = ultimos_jogos[-1]
                streak_count = 1
                for i in range(len(ultimos_jogos) - 2, -1, -1):
                    if ultimos_jogos[i] == streak_char:
                        streak_count += 1
                    else:
                        break
                streak = f"{streak_char}{streak_count}"
            else:
                streak = "-"
            
            dados_time = {
                'id': team_id,
                'time': team_stats['time'],
                'v': team_stats['v'],
                'd': team_stats['d'],
                'pct': pct,
                'casa': f"{team_stats['casa_v']}-{team_stats['casa_d']}",
                'visitante': f"{team_stats['fora_v']}-{team_stats['fora_d']}",
                'div': team_stats['division'],
                'conf': team_stats['conference'],
                'strk': streak
            }
            
            # Adicionar apenas se a conferência existir no dicionário
            if team_stats['conference'] in conferencias:
                conferencias[team_stats['conference']].append(dados_time)
        
        # Ordenar cada conferência por porcentagem de vitórias
        for conf in conferencias.values():
            conf.sort(key=lambda x: (x['pct'], x['v']), reverse=True)
        
        # Calcular "jogos atrás" e formatar dados finais
        for conf_name, times_conf in conferencias.items():
            if not times_conf:
                continue
                
            lider = times_conf[0]
            lider_v = lider['v']
            lider_d = lider['d']
            
            for i, time in enumerate(times_conf):
                # Calcular jogos atrás do líder
                if i == 0:
                    ja = "-"
                else:
                    ja = ((lider_v - time['v']) + (time['d'] - lider_d)) / 2
                    ja = f"{ja:.1f}"
                
                dados_formatados = {
                    "time": time['time'],
                    "v": str(time['v']),
                    "d": str(time['d']),
                    "pct": f"{time['pct']:.3f}",
                    "ja": ja,
                    "casa": time['casa'],
                    "visitante": time['visitante'],
                    "div": f"{time['div']} ({i+1})",
                    "conf": f"{time['conf']} ({i+1})",
                    "pts": "0",
                    "pts_contra": "0",
                    "dif": "0",
                    "strk": time['strk']
                }
                lista_times.append(dados_formatados)
        
        # Atualização no Supabase
        if lista_times:
            print(f"📊 Atualizando {len(lista_times)} times no Supabase...")
            
            # 1. Limpa a tabela atual
            db.table("classificacao_nba").delete().neq("time", "vazio").execute()
            
            # 2. Insere os novos dados
            db.table("classificacao_nba").insert(lista_times).execute()
            
            print(f"✅ Sucesso! {len(lista_times)} times atualizados sem erros.")
            print(f"📈 Conferência Leste: {len(conferencias['East'])} times")
            print(f"📈 Conferência Oeste: {len(conferencias['West'])} times")
        else:
            print("⚠️ Nenhum time para atualizar.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de requisição: {e}")
    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    calcular_classificacao()