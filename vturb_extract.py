#!/usr/bin/env python3
"""
VTurb Data Extractor
Comandos: python vturb_extract.py hoje | ontem
"""

import os
import requests
import json
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
# CARREGAR .ENV
# =====================================================

def load_env():
    """Carrega variáveis do arquivo .env"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_file = os.path.join(script_dir, ".env")
    
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key, value = key.strip(), value.strip()
                    if value:
                        os.environ[key] = value

load_env()


# =====================================================
# CONFIGURAÇÕES
# =====================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

VTURB_TOKEN = os.getenv("VTURB_TOKEN", "")

PLAYER_IDS = [
    '693a3e45e891e679e7727765',
    '691b21ad05cbd5105c709802',
    '68d5efd4861b70626857ef1e',
    '68d5f040861b70626857efc5',
    '68d5f0d8b6d4ef5b76bf6e3b',
    '68d5f1141f0c16bccf4f42e1',
    '695bd291707d41fbaa79efa1',
    '6985f7228fd75d51815b9eab',
    '68d7340b232c1a965f3b8b29',
    '68d7367752020545d65d1933',
    '68d738dda4bea31e50e65a62',
    '696efdd3e1aa589ccc4d9028',
    '696efddaa4304f1d777b5f94',
    '696efde0edc67029da1c04a6',
    '696efde6521058214caadf91',
    '696efdeea4304f1d777b5fee',
    '69725ecbe6996b070b27bf65',
    '69725f2ad310ef352e0255e2',
    '6972e6ae938018005141d189',
    '6972e6cdc8196f1982aab633',
    '6972677e109c2c0df2550c91',
    '697267844a89073d7e1f1901',
    '6972678b9cf9fc801ee7b9c8',
    '6938cff1e45bb9548f311ced',
    '6939e2dfe891e679e77211e2',
    '69409ba79ff1b4f2bbc57b76',
    '694ab5f5a54e8f46c18817a3',
    '694ab6cea54e8f46c18818d4',
    '694ab7a171611df8184b1a76',
    '694ab87ca54e8f46c1881b03',
    '694acdb1ed1852c895dadb7d',
    '694ace8390b70171e37c03c8',
    '694acf5aa54e8f46c18834a3',
    '694ad02f71611df8184b3543',
    '694ad10963476f09ce028807'
]

TIMEZONE = 'America/Sao_Paulo'

logger.info(f"🔑 Token: {'✅ Definido' if VTURB_TOKEN else '❌ Não definido'}")


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
# API
# =====================================================

def fetch_player_stats(player_id: str, target_date: date) -> Optional[Dict]:
    """Busca estatísticas do player na API VTurb"""
    
    token = VTURB_TOKEN.strip()
    if not token:
        logger.error("❌ VTURB_TOKEN não definido!")
        return None
    
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
            logger.error(f"❌ Erro {response.status_code}: {response.text[:200]}")
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


# =====================================================
# DATABASE
# =====================================================

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


def save_to_today(stats_list: list) -> bool:
    """Salva em vturb_today (TRUNCATE + INSERT)"""
    
    if not stats_list:
        return False
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Limpa tabela
    cursor.execute("TRUNCATE TABLE vturb_today")
    logger.info("🗑️ Tabela vturb_today limpa")
    
    query = """
        INSERT INTO vturb_today (
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
    """
    
    for stats in stats_list:
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
    
    logger.info(f"✅ {len(stats_list)} players salvos em vturb_today")
    return True


# =====================================================
# EXTRAÇÃO
# =====================================================

def print_summary(stats_list: list):
    """Imprime resumo"""
    total_views = sum(s.total_views for s in stats_list)
    total_plays = sum(s.total_plays for s in stats_list)
    total_clicks = sum(s.total_clicks for s in stats_list)
    total_conversions = sum(s.total_conversions for s in stats_list)
    
    print("\n" + "=" * 50)
    print("📈 RESUMO")
    print("=" * 50)
    print(f"Players: {len(stats_list)}")
    print(f"Views: {total_views:,}".replace(',', '.'))
    print(f"Plays: {total_plays:,}".replace(',', '.'))
    print(f"Clicks: {total_clicks:,}".replace(',', '.'))
    print(f"Conversões: {total_conversions:,}".replace(',', '.'))
    print("=" * 50)
    print("✅ Extração concluída!")


def extract_today():
    """Extrai dados de HOJE → vturb_today"""
    
    target_date = date.today()
    
    print("=" * 50)
    print(f"📊 VTURB EXTRACTOR - HOJE")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: vturb_today")
    print(f"🔑 Token: {'✅ Definido' if VTURB_TOKEN else '❌ Não definido'}")
    print("=" * 50)
    
    if not VTURB_TOKEN:
        print("\n❌ Execute: python3 auto_extract.py vturb")
        return
    
    stats_list = []
    
    for player_id in PLAYER_IDS:
        raw_data = fetch_player_stats(player_id, target_date)
        if raw_data:
            stats = parse_stats(raw_data, player_id, target_date)
            stats_list.append(stats)
    
    if stats_list:
        save_to_today(stats_list)
        print_summary(stats_list)
    else:
        print("\n⚠️ Nenhum dado extraído")


def extract_yesterday():
    """Extrai dados de ONTEM → vturb_history"""
    
    target_date = date.today() - timedelta(days=1)
    
    print("=" * 50)
    print(f"📊 VTURB EXTRACTOR - ONTEM")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: vturb_history")
    print(f"🔑 Token: {'✅ Definido' if VTURB_TOKEN else '❌ Não definido'}")
    print("=" * 50)
    
    if not VTURB_TOKEN:
        print("\n❌ Execute: python3 auto_extract.py vturb")
        return
    
    stats_list = []
    
    for player_id in PLAYER_IDS:
        raw_data = fetch_player_stats(player_id, target_date)
        if raw_data:
            stats = parse_stats(raw_data, player_id, target_date)
            save_to_history(stats)
            stats_list.append(stats)
    
    if stats_list:
        print_summary(stats_list)
    else:
        print("\n⚠️ Nenhum dado extraído")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python vturb_extract.py [hoje|ontem]")
        print("")
        print("Comandos:")
        print("  hoje   - Extrai dados de hoje (salva em vturb_today)")
        print("  ontem  - Extrai dados de ontem (salva em vturb_history)")
        sys.exit(1)
    
    comando = sys.argv[1].lower()
    
    if comando == "hoje":
        extract_today()
    elif comando == "ontem":
        extract_yesterday()
    else:
        print(f"❌ Comando inválido: {comando}")
        print("   Use: hoje ou ontem")
        sys.exit(1)