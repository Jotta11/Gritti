#!/usr/bin/env python3
"""
VTurb Data Extractor - Data Personalizada
Uso: python vturb_extract_data.py DD/MM/YYYY
"""

import os
import requests
from datetime import datetime, timedelta, date
from typing import Dict, Optional
from dataclasses import dataclass
import logging

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =====================================================
# CONFIGURAÇÕES
# =====================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database-2.cdg6qmmuuc8e.us-east-2.rds.amazonaws.com"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "Gritti2"),
    "user": os.getenv("DB_USER", "ancher"),
    "password": os.getenv("DB_PASSWORD", "Spirorbis7-Swab7"),
}

VTURB_TOKEN = os.getenv("VTURB_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImU2ZjJkNDcyMCJ9.eyJhdWQiOiI4MmFhZDE0MS03MTdlLTRlNWEtODg2ZS04MDM0OGI0MTQwNmYiLCJleHAiOjE3Njg0NDUwMTAsImlhdCI6MTc2ODQwMTgxMCwiaXNzIjoiaHR0cHM6Ly9hdXRoLnZ0dXJiLmNvbS5iciIsInN1YiI6ImZhMDBhYzM4LTU1N2MtNDA3MS05MmYyLTE2ZGYzNWU5YjY3MyIsImp0aSI6ImQ4NjUxNmNjLWFkZjYtNDdlZC1iN2ZhLTc2MDFhMjY2ODJiOSIsImF1dGhlbnRpY2F0aW9uVHlwZSI6IlJFRlJFU0hfVE9LRU4iLCJlbWFpbCI6ImFuYWNsYXJhYmljaHVldGVAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImFwcGxpY2F0aW9uSWQiOiI4MmFhZDE0MS03MTdlLTRlNWEtODg2ZS04MDM0OGI0MTQwNmYiLCJ0aWQiOiI5ZmI4YTkxYy0wMzA0LWJiYjYtODg2NS0yODU2OTcwY2VkY2YiLCJyb2xlcyI6WyJwcm9kdWN0cyJdLCJhdXRoX3RpbWUiOjE3Njc5OTA1NzMsInNpZCI6ImZmYjU4MDQ4LWUxMDktNDA3OS1iYjQwLTFkZDk3ZjQzZmIzNyIsImNpZCI6ImIwZTQ5MzliLWNjNzctNGUyYS1iNDRlLWNkZDcyNTBjZjcyOSIsInBlcm1pc3Npb24iOnsibmFtZSI6IkFETUlOIiwibGV2ZWwiOjExMjU4OTk5MDY4NDI2MjN9LCJmb2xkZXJfcGVybWlzc2lvbnMiOnt9LCJ1c2VyX2RhdGEiOnsibG9jYWxlIjpudWxsLCJ0aW1lem9uZSI6bnVsbH0sImNvbXBhbnlfZGF0YSI6eyJsb2NhbGUiOiJwdC1CUiIsImJyIjpbImFudGlfZGwiLCJjYWxsX2FjdGlvbnNfdjMiLCJmYWtlX2RsIiwicGxheWVyX3Y0IiwidHJhZmZpY19maWx0ZXIiXSwidGltZXpvbmUiOm51bGwsImhhc192YWxpZF9zdWJzY3JpcHRpb24iOnRydWV9LCJyZWZyZXNoX3Rva2VuIjoiRmZxQTF6cXQ3aTA5d2Zfd09uLXByZzZ3ZjJpWG5RX2dmN1lWUDd1Q1JpaG1YV3FmSUFJZEN3In0.sJhkiKwSRhIcUdFD1KO0Cn4JqHQSOhNExyjnfjuIr_8")

PLAYER_IDS = [
    '693a3e45e891e679e7727765',
    '691b21ad05cbd5105c709802'
]

TIMEZONE = 'America/Sao_Paulo'


# =====================================================
# DATACLASS
# =====================================================

@dataclass
class PlayerStats:
    player_id: str
    stats_date: date
    extraction_timestamp: str
    start_datetime: str
    end_datetime: str
    
    total_views: int
    total_unique_device_views: int
    total_unique_session_views: int
    
    total_plays: int
    total_unique_device_plays: int
    total_unique_session_plays: int
    
    total_finishes: int
    total_unique_device_finishes: int
    total_unique_session_finishes: int
    
    total_clicks: int
    total_unique_device_clicks: int
    total_unique_session_clicks: int
    
    total_conversions: int
    total_unique_device_conversions: int
    total_unique_session_conversions: int
    total_amount_brl: float
    total_amount_usd: float
    total_amount_eur: float
    
    overall_play_rate: float
    overall_conversion_rate: float
    
    average_watched_time: float
    engagement_rate: float
    pitch_time_retention_rate: float


# =====================================================
# FUNÇÕES
# =====================================================

def fetch_player_stats(player_id: str, target_date: date) -> Optional[Dict]:
    """Busca estatísticas do player na API VTurb"""
    
    token = VTURB_TOKEN.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    
    url = f"https://api.vturb.com/vturb/v2/players/{player_id}/analytics_stream/player_stats"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    start_date = f"{target_date.strftime('%Y-%m-%d')} 00:00:00"
    end_date = f"{target_date.strftime('%Y-%m-%d')} 23:59:59"
    
    body = {
        'player_stats': {
            'player_id': player_id,
            'start_date': start_date,
            'end_date': end_date,
            'timezone': TIMEZONE
        }
    }
    
    logger.info(f"🔄 Buscando player {player_id} - {target_date.strftime('%d/%m/%Y')}")
    
    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Dados recebidos para player {player_id}")
            return data.get('stats', data)
        elif response.status_code == 401:
            logger.error("🔐 Token inválido ou expirado!")
            return None
        else:
            logger.error(f"❌ Erro {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"💥 Erro: {e}")
        return None


def parse_stats(raw_data: Dict, player_id: str, target_date: date) -> PlayerStats:
    """Converte dados da API para PlayerStats"""
    
    views = raw_data.get('views', {})
    plays = raw_data.get('plays', {})
    finishes = raw_data.get('finishes', {})
    clicks = raw_data.get('clicks', {})
    conversions = raw_data.get('conversions', {})
    play_rate = raw_data.get('playRate', {})
    conversion_rate = raw_data.get('conversionRate', {})
    engagement = raw_data.get('engagement_stats', {})
    
    return PlayerStats(
        player_id=player_id,
        stats_date=target_date,
        extraction_timestamp=datetime.now().isoformat(),
        start_datetime=f"{target_date.strftime('%Y-%m-%d')} 00:00:00",
        end_datetime=f"{target_date.strftime('%Y-%m-%d')} 23:59:59",
        
        total_views=views.get('totalEvents', 0),
        total_unique_device_views=views.get('totalUniqDeviceEvents', 0),
        total_unique_session_views=views.get('totalUniqSessionEvents', 0),
        
        total_plays=plays.get('totalEvents', 0),
        total_unique_device_plays=plays.get('totalUniqDeviceEvents', 0),
        total_unique_session_plays=plays.get('totalUniqSessionEvents', 0),
        
        total_finishes=finishes.get('totalEvents', 0),
        total_unique_device_finishes=finishes.get('totalUniqDeviceEvents', 0),
        total_unique_session_finishes=finishes.get('totalUniqSessionEvents', 0),
        
        total_clicks=clicks.get('totalEvents', 0),
        total_unique_device_clicks=clicks.get('totalUniqDeviceEvents', 0),
        total_unique_session_clicks=clicks.get('totalUniqSessionEvents', 0),
        
        total_conversions=conversions.get('totalEvents', 0),
        total_unique_device_conversions=conversions.get('totalUniqDeviceEvents', 0),
        total_unique_session_conversions=conversions.get('totalUniqSessionEvents', 0),
        total_amount_brl=conversions.get('totalAmountBrl', 0) or 0,
        total_amount_usd=conversions.get('totalAmountUsd', 0) or 0,
        total_amount_eur=conversions.get('totalAmountEur', 0) or 0,
        
        overall_play_rate=play_rate.get('overallPlayRate', 0) or 0,
        overall_conversion_rate=conversion_rate.get('overallConversionRate', 0) or 0,
        
        average_watched_time=engagement.get('average_watched_time', 0) or 0,
        engagement_rate=engagement.get('engagement_rate', 0) or 0,
        pitch_time_retention_rate=engagement.get('pitch_time_retention_rate', 0) or 0
    )


def save_to_history(stats: PlayerStats) -> bool:
    """Salva em vturb_history (UPSERT)"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
        INSERT INTO vturb_history (
            player_id, stats_date, extraction_timestamp, start_datetime, end_datetime,
            total_views, total_unique_device_views, total_unique_session_views,
            total_plays, total_unique_device_plays, total_unique_session_plays,
            total_finishes, total_unique_device_finishes, total_unique_session_finishes,
            total_clicks, total_unique_device_clicks, total_unique_session_clicks,
            total_conversions, total_unique_device_conversions, total_unique_session_conversions,
            total_amount_brl, total_amount_usd, total_amount_eur,
            overall_play_rate, overall_conversion_rate,
            average_watched_time, engagement_rate, pitch_time_retention_rate
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (player_id, stats_date) DO UPDATE SET
            extraction_timestamp = EXCLUDED.extraction_timestamp,
            total_views = EXCLUDED.total_views,
            total_unique_device_views = EXCLUDED.total_unique_device_views,
            total_plays = EXCLUDED.total_plays,
            total_unique_device_plays = EXCLUDED.total_unique_device_plays,
            total_finishes = EXCLUDED.total_finishes,
            total_clicks = EXCLUDED.total_clicks,
            total_conversions = EXCLUDED.total_conversions,
            total_amount_brl = EXCLUDED.total_amount_brl,
            overall_play_rate = EXCLUDED.overall_play_rate,
            overall_conversion_rate = EXCLUDED.overall_conversion_rate,
            average_watched_time = EXCLUDED.average_watched_time,
            engagement_rate = EXCLUDED.engagement_rate,
            pitch_time_retention_rate = EXCLUDED.pitch_time_retention_rate
    """
    
    values = (
        stats.player_id, stats.stats_date, stats.extraction_timestamp,
        stats.start_datetime, stats.end_datetime,
        stats.total_views, stats.total_unique_device_views, stats.total_unique_session_views,
        stats.total_plays, stats.total_unique_device_plays, stats.total_unique_session_plays,
        stats.total_finishes, stats.total_unique_device_finishes, stats.total_unique_session_finishes,
        stats.total_clicks, stats.total_unique_device_clicks, stats.total_unique_session_clicks,
        stats.total_conversions, stats.total_unique_device_conversions, stats.total_unique_session_conversions,
        stats.total_amount_brl, stats.total_amount_usd, stats.total_amount_eur,
        stats.overall_play_rate, stats.overall_conversion_rate,
        stats.average_watched_time, stats.engagement_rate, stats.pitch_time_retention_rate
    )
    
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ Salvo em vturb_history: {stats.player_id} - {stats.stats_date}")
    return True


def parse_date(date_str: str) -> date:
    """Converte string para date"""
    formats = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"Formato inválido: {date_str}. Use DD/MM/YYYY")


def extract(target_date: date):
    """Extrai e salva dados de uma data específica"""
    
    print("=" * 50)
    print(f"📊 VTURB EXTRACTOR - DATA PERSONALIZADA")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: vturb_history")
    print("=" * 50)
    
    stats_list = []
    
    for player_id in PLAYER_IDS:
        raw_data = fetch_player_stats(player_id, target_date)
        if raw_data:
            stats = parse_stats(raw_data, player_id, target_date)
            save_to_history(stats)
            stats_list.append(stats)
    
    if stats_list:
        total_views = sum(s.total_views for s in stats_list)
        total_plays = sum(s.total_plays for s in stats_list)
        total_clicks = sum(s.total_clicks for s in stats_list)
        
        print("\n" + "=" * 50)
        print("📈 RESUMO")
        print("=" * 50)
        print(f"Players: {len(stats_list)}")
        print(f"Views: {total_views:,}".replace(',', '.'))
        print(f"Plays: {total_plays:,}".replace(',', '.'))
        print(f"Clicks: {total_clicks:,}".replace(',', '.'))
        print("=" * 50)
        print("✅ Extração concluída!")
    else:
        print("\n⚠️ Nenhum dado extraído")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python vturb_extract_data.py DD/MM/YYYY")
        print("")
        print("Exemplos:")
        print("  python vturb_extract_data.py 05/01/2026")
        print("  python vturb_extract_data.py 2026-01-05")
        sys.exit(1)
    
    try:
        target_date = parse_date(sys.argv[1])
        extract(target_date)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)