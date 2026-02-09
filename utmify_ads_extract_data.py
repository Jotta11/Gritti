#!/usr/bin/env python3
"""
Utmify Ads/Criativos Extractor - Data Personalizada
Uso: python utmify_ads_extract_data.py DD/MM/YYYY
"""

import os
import requests
from datetime import datetime, timedelta, date
from typing import Dict, Any
import logging

import psycopg2
from psycopg2.extras import execute_values

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

UTMIFY_TOKEN = os.getenv("UTMIFY_TOKEN", "SEU_TOKEN_AQUI")
UTMIFY_DASHBOARD_ID = os.getenv("UTMIFY_DASHBOARD_ID", "66668acc6670e6d0c7a17699")


# =====================================================
# FUNÇÕES
# =====================================================

def cents_to_decimal(value):
    return round(value / 100, 2) if value else None


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except:
        return None


def parse_date(date_str: str) -> date:
    """Converte string para date"""
    formats = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Formato inválido: {date_str}. Use DD/MM/YYYY")


def fetch_ads(target_date: date) -> Dict[str, Any]:
    """Busca anúncios da API"""
    
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {UTMIFY_TOKEN}",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://app.utmify.com.br",
        "referer": "https://app.utmify.com.br/",
    }
    
    date_from = target_date.strftime("%Y-%m-%dT03:00:00.000Z")
    date_to = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT02:59:59.999Z")
    
    payload = {
        "level": "ad",
        "dateRange": {"from": date_from, "to": date_to},
        "nameContains": None,
        "productNames": None,
        "orderBy": "greater_profit",
        "adObjectStatuses": None,
        "accountStatuses": None,
        "metaAdAccountIds": None,
        "dashboardId": UTMIFY_DASHBOARD_ID
    }
    
    logger.info(f"🔄 Buscando ads de {target_date.strftime('%d/%m/%Y')}...")
    
    response = requests.post(
        "https://server.utmify.com.br/orders/search-objects",
        headers=headers,
        json=payload,
        timeout=60
    )
    
    response.raise_for_status()
    data = response.json()
    
    logger.info(f"✅ {len(data.get('results', []))} anúncios encontrados")
    return data


def prepare_ad_values(ads: list, report_date: date) -> list:
    values = []
    for a in ads:
        values.append((
            a.get("adId") or a.get("id"),
            report_date,
            a.get("campaignId"),
            a.get("adsetId"),
            a.get("accountId"),
            a.get("profileId"),
            a.get("ca"),
            a.get("name"),
            a.get("level", "ad"),
            a.get("status"),
            a.get("effectiveStatus"),
            cents_to_decimal(a.get("spend")),
            cents_to_decimal(a.get("revenue")),
            cents_to_decimal(a.get("grossRevenue")),
            cents_to_decimal(a.get("profit")),
            cents_to_decimal(a.get("fees")),
            cents_to_decimal(a.get("tax")),
            cents_to_decimal(a.get("productCosts")),
            a.get("roas", 0),
            a.get("roi", 0),
            a.get("profitMargin", 0),
            cents_to_decimal(a.get("cpa")),
            cents_to_decimal(a.get("cpm")),
            cents_to_decimal(a.get("costPerInlineLinkClick")),
            a.get("inlineLinkClickCtr", 0),
            a.get("impressions", 0),
            a.get("inlineLinkClicks", 0),
            a.get("frequency", 0),
            a.get("totalOrdersCount", 0),
            a.get("approvedOrdersCount", 0),
            a.get("pendingOrdersCount", 0),
            a.get("refundedOrdersCount", 0),
            a.get("refusedOrdersCount", 0),
            a.get("salesFromFacebook", 0),
            cents_to_decimal(a.get("pendingRevenue")),
            cents_to_decimal(a.get("refundedRevenue")),
            a.get("initiateCheckout", 0),
            cents_to_decimal(a.get("costPerInitiateCheckout")),
            a.get("checkoutConversion", 0),
            a.get("clickConversion", 0),
            a.get("landingPageViews", 0),
            a.get("leads", 0),
            cents_to_decimal(a.get("costPerLead")),
            a.get("videoViews", 0),
            a.get("video75Watched", 0),
            a.get("videoViews3Seconds", 0),
            a.get("hook", 0),
            a.get("retention", 0),
            a.get("hookPlayRate", 0),
            a.get("conversations", 0),
            cents_to_decimal(a.get("costPerConversation")),
            parse_datetime(a.get("createdTime")),
        ))
    return values


def save_to_history(ads: list, report_date: date) -> int:
    """Salva em ads_history (UPSERT)"""
    
    if not ads:
        logger.warning("⚠️ Nenhum anúncio para salvar")
        return 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    values = prepare_ad_values(ads, report_date)
    
    query = """
        INSERT INTO ads_history (
            ad_id, report_date, campaign_id, adset_id, account_id, profile_id, ca,
            name, level, status, effective_status,
            spend, revenue, gross_revenue, profit, fees, tax, product_costs,
            roas, roi, profit_margin, cpa, cpm, cpc, ctr,
            impressions, clicks, frequency,
            total_orders, approved_orders, pending_orders, refunded_orders,
            refused_orders, sales_from_facebook, pending_revenue, refunded_revenue,
            initiate_checkout, cost_per_checkout, checkout_conversion, click_conversion,
            landing_page_views, leads, cost_per_lead,
            video_views, video_75_watched, video_3s_views, hook_rate, retention, hook_play_rate,
            conversations, cost_per_conversation, created_time
        ) VALUES %s
        ON CONFLICT (ad_id, report_date) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            effective_status = EXCLUDED.effective_status,
            spend = EXCLUDED.spend,
            revenue = EXCLUDED.revenue,
            gross_revenue = EXCLUDED.gross_revenue,
            profit = EXCLUDED.profit,
            fees = EXCLUDED.fees,
            roas = EXCLUDED.roas,
            roi = EXCLUDED.roi,
            approved_orders = EXCLUDED.approved_orders,
            video_views = EXCLUDED.video_views,
            hook_rate = EXCLUDED.hook_rate,
            retention = EXCLUDED.retention
    """
    
    execute_values(cursor, query, values)
    count = cursor.rowcount
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ {count} anúncios salvos em ads_history")
    return count


def extract(target_date: date):
    """Extrai e salva dados"""
    
    print("=" * 50)
    print(f"📊 UTMIFY ADS EXTRACTOR - DATA PERSONALIZADA")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: ads_history")
    print("=" * 50)
    
    try:
        data = fetch_ads(target_date)
        ads = data.get("results", [])
        
        if not ads:
            print("\n⚠️ Nenhum anúncio encontrado")
            return
        
        save_to_history(ads, target_date)
        
        # Resumo
        total_spend = sum(a.get("spend", 0) / 100 for a in ads)
        total_revenue = sum(a.get("revenue", 0) / 100 for a in ads)
        total_profit = sum(a.get("profit", 0) / 100 for a in ads)
        
        print("\n" + "=" * 50)
        print("📈 RESUMO")
        print("=" * 50)
        print(f"Anúncios: {len(ads)}")
        print(f"Spend: R$ {total_spend:,.2f}")
        print(f"Revenue: R$ {total_revenue:,.2f}")
        print(f"Profit: R$ {total_profit:,.2f}")
        if total_spend > 0:
            print(f"ROAS: {total_revenue/total_spend:.2f}x")
        print("=" * 50)
        print("✅ Extração concluída!")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("\n❌ Token expirado!")
        else:
            print(f"\n❌ Erro HTTP: {e}")
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        raise


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python utmify_ads_extract_data.py DD/MM/YYYY")
        print("")
        print("Exemplos:")
        print("  python utmify_ads_extract_data.py 14/01/2026")
        print("  python utmify_ads_extract_data.py 2026-01-14")
        sys.exit(1)
    
    try:
        target_date = parse_date(sys.argv[1])
        extract(target_date)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)