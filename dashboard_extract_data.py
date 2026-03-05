#!/usr/bin/env python3
"""
Utmify Dashboard Extractor - Data Personalizada
Extrai dados consolidados do dashboard por fonte de tráfego para uma data específica
Uso: python3 dashboard_extract_data.py DD/MM/YYYY
"""

import os
import requests
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List
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

UTMIFY_TOKEN = os.getenv("UTMIFY_TOKEN", "")

# Dashboard IDs
DEFAULT_DASHBOARD_IDS = [
    os.getenv("UTMIFY_DASHBOARD_ID", "66668acc6670e6d0c7a17699"),
    "697179f7cfa58e5d2a21afdf",
    "6972dc7473de5f488a3aee2b",
]
UTMIFY_DASHBOARD_IDS = [
    d.strip()
    for d in os.getenv("UTMIFY_DASHBOARD_IDS", ",".join(DEFAULT_DASHBOARD_IDS)).split(",")
    if d.strip()
]

# Fontes de tráfego para extrair
TRAFFIC_SOURCES = [None, "Meta", "Google", "Kwai", "TikTok"]  # None = todas

logger.info(f"🔑 Token: {'✅ Definido' if UTMIFY_TOKEN else '❌ Não definido'}")


# =====================================================
# API
# =====================================================

def fetch_dashboard(target_date: date, dashboard_id: str, traffic_source: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Busca dados do dashboard na API do Utmify"""
    
    if not UTMIFY_TOKEN:
        logger.error("❌ UTMIFY_TOKEN não definido!")
        return None
    
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {UTMIFY_TOKEN}",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://app.utmify.com.br",
        "referer": "https://app.utmify.com.br/",
    }
    
    # Ajusta para timezone UTC-3
    date_from = target_date.strftime("%Y-%m-%dT03:00:00.000Z")
    date_to = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT02:59:59.999Z")
    
    payload = {
        "dateRange": {"from": date_from, "to": date_to},
        "dashboardId": dashboard_id,
        "trafficSource": traffic_source,
        "metaAdAccountIds": None,
        "googleAdAccountIds": None,
        "kwaiAdAccountIds": None,
        "tikTokAdAccountIds": None,
        "platforms": None,
        "productNames": None,
    }
    
    source_name = traffic_source or "Todas"
    logger.info(f"🔄 Dashboard {dashboard_id[:8]}... | Fonte: {source_name}")
    
    try:
        response = requests.post(
            "https://server.utmify.com.br/orders/dashboard-info",
            headers=headers,
            json=payload,
            timeout=120
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            logger.error("🔐 Token inválido ou expirado!")
            return None
        else:
            logger.error(f"❌ Erro {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        logger.error(f"💥 Erro: {e}")
        return None


def cents_to_decimal(value):
    """Converte centavos para decimal"""
    if value is None:
        return 0
    return round(value / 100, 2)


# =====================================================
# CONSOLIDAÇÃO
# =====================================================

def consolidate_dashboards(dashboards: List[Dict]) -> Dict[str, Any]:
    """Consolida dados de múltiplos dashboards"""
    
    if not dashboards:
        return {}
    
    if len(dashboards) == 1:
        return dashboards[0]
    
    # Começa com o primeiro
    result = dashboards[0].copy()
    
    for dash in dashboards[1:]:
        # Orders
        for key in ["total", "approved", "pending", "refunded", "chargedback",
                    "totalCreditCard", "approvedCreditCard", "refusedCreditCard"]:
            result.setdefault("ordersCount", {})[key] = \
                result.get("ordersCount", {}).get(key, 0) + dash.get("ordersCount", {}).get(key, 0)
        
        # Comissions
        for key in ["net", "gross", "pendingGrossRevenue", "refundedGrossRevenue", "chargebackGrossRevenue"]:
            result.setdefault("comissions", {})[key] = \
                result.get("comissions", {}).get(key, 0) + dash.get("comissions", {}).get(key, 0)
        
        # Ads
        for key in ["spent", "clicks", "pageViews", "initiateCheckouts", "leads"]:
            result.setdefault("ads", {})[key] = \
                result.get("ads", {}).get(key, 0) + dash.get("ads", {}).get(key, 0)
        
        # Analytics (soma profit, fees, taxes)
        for key in ["profit", "fees", "taxes"]:
            result.setdefault("analytics", {})[key] = \
                result.get("analytics", {}).get(key, 0) + dash.get("analytics", {}).get(key, 0)
        
        # Statistics - PIX
        pix = result.setdefault("statistics", {}).setdefault("pix", {})
        dash_pix = dash.get("statistics", {}).get("pix", {})
        
        for status in ["approved", "pending"]:
            pix.setdefault(status, {})["ordersCount"] = \
                pix.get(status, {}).get("ordersCount", 0) + dash_pix.get(status, {}).get("ordersCount", 0)
            pix.setdefault(status, {})["comission"] = \
                pix.get(status, {}).get("comission", 0) + dash_pix.get(status, {}).get("comission", 0)
        
        # Statistics - Card
        card = result.setdefault("statistics", {}).setdefault("card", {})
        dash_card = dash.get("statistics", {}).get("card", {})
        
        for status in ["approved", "refused"]:
            card.setdefault(status, {})["ordersCount"] = \
                card.get(status, {}).get("ordersCount", 0) + dash_card.get(status, {}).get("ordersCount", 0)
            card.setdefault(status, {})["comission"] = \
                card.get(status, {}).get("comission", 0) + dash_card.get(status, {}).get("comission", 0)
    
    # Recalcula métricas
    comissions = result.get("comissions", {})
    ads = result.get("ads", {})
    analytics = result.get("analytics", {})
    orders = result.get("ordersCount", {})
    
    gross = comissions.get("gross", 0)
    spent = ads.get("spent", 0)
    approved = orders.get("approved", 0)
    
    if spent > 0:
        analytics["roi"] = (analytics.get("profit", 0) / spent)
        analytics["roas"] = (gross / spent)
    
    if gross > 0:
        analytics["profitMargin"] = (analytics.get("profit", 0) / gross)
    
    if approved > 0:
        analytics["cpa"] = (spent / approved)
        analytics["avgTicket"] = (gross / approved)
    
    leads = ads.get("leads", 0)
    if leads > 0:
        analytics["costPerLead"] = (spent / leads)
    
    result["analytics"] = analytics
    
    return result


# =====================================================
# DATABASE
# =====================================================

def prepare_values(data: Dict, report_date: date, traffic_source: str) -> tuple:
    """Prepara valores para inserção"""
    
    orders = data.get("ordersCount", {})
    comissions = data.get("comissions", {})
    ads = data.get("ads", {})
    analytics = data.get("analytics", {})
    statistics = data.get("statistics", {})
    pix = statistics.get("pix", {})
    card = statistics.get("card", {})
    
    return (
        report_date,
        traffic_source,
        # Orders
        orders.get("total", 0),
        orders.get("approved", 0),
        orders.get("pending", 0),
        orders.get("refunded", 0),
        orders.get("chargedback", 0),
        # Credit Card
        orders.get("totalCreditCard", 0),
        orders.get("approvedCreditCard", 0),
        orders.get("refusedCreditCard", 0),
        # Faturamento
        cents_to_decimal(comissions.get("gross", 0)),
        cents_to_decimal(comissions.get("net", 0)),
        cents_to_decimal(comissions.get("pendingGrossRevenue", 0)),
        cents_to_decimal(comissions.get("refundedGrossRevenue", 0)),
        cents_to_decimal(comissions.get("chargebackGrossRevenue", 0)),
        # Ads
        cents_to_decimal(ads.get("spent", 0)),
        ads.get("clicks", 0),
        ads.get("pageViews", 0),
        ads.get("initiateCheckouts", 0),
        ads.get("leads", 0),
        # Métricas
        cents_to_decimal(analytics.get("profit", 0)),
        analytics.get("roi", 0),
        analytics.get("roas", 0),
        analytics.get("profitMargin", 0),
        cents_to_decimal(analytics.get("cpa", 0)),
        cents_to_decimal(analytics.get("avgTicket", 0)),
        cents_to_decimal(analytics.get("costPerLead", 0)),
        # Fees/Taxes
        cents_to_decimal(analytics.get("fees", 0)),
        cents_to_decimal(analytics.get("taxes", 0)),
        # PIX
        pix.get("approved", {}).get("ordersCount", 0),
        cents_to_decimal(pix.get("approved", {}).get("comission", 0)),
        pix.get("pending", {}).get("ordersCount", 0),
        cents_to_decimal(pix.get("pending", {}).get("comission", 0)),
        # Card
        card.get("approved", {}).get("ordersCount", 0),
        cents_to_decimal(card.get("approved", {}).get("comission", 0)),
        card.get("refused", {}).get("ordersCount", 0),
        cents_to_decimal(card.get("refused", {}).get("comission", 0)),
    )


def save_to_history(data: Dict, report_date: date, traffic_source: str):
    """Salva em dashboard_history"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    source_key = traffic_source or "all"
    
    query = """
        INSERT INTO dashboard_history (
            report_date, traffic_source,
            total_orders, approved_orders, pending_orders, refunded_orders, chargedback_orders,
            total_credit_card, approved_credit_card, refused_credit_card,
            gross_revenue, net_revenue, pending_revenue, refunded_revenue, chargeback_revenue,
            ads_spent, ads_clicks, ads_page_views, ads_initiate_checkouts, ads_leads,
            profit, roi, roas, profit_margin, cpa, avg_ticket, cost_per_lead,
            fees, taxes,
            pix_approved_orders, pix_approved_revenue, pix_pending_orders, pix_pending_revenue,
            card_approved_orders, card_approved_revenue, card_refused_orders, card_refused_revenue
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (report_date, traffic_source) DO UPDATE SET
            extraction_timestamp = NOW(),
            total_orders = EXCLUDED.total_orders,
            approved_orders = EXCLUDED.approved_orders,
            pending_orders = EXCLUDED.pending_orders,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            ads_spent = EXCLUDED.ads_spent,
            profit = EXCLUDED.profit,
            roi = EXCLUDED.roi,
            roas = EXCLUDED.roas
    """
    
    values = prepare_values(data, report_date, source_key)
    cursor.execute(query, values)
    
    conn.commit()
    cursor.close()
    conn.close()


# =====================================================
# EXTRAÇÃO
# =====================================================

def extract_date(target_date: date):
    """Extrai dados de uma data específica"""
    
    print("=" * 60)
    print(f"📊 DASHBOARD EXTRACTOR - DATA PERSONALIZADA")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"🔑 Token: {'✅ Definido' if UTMIFY_TOKEN else '❌ Não definido'}")
    print(f"📡 Fontes: all, Meta, Google, Kwai, TikTok")
    print(f"💾 Destino: dashboard_history")
    print("=" * 60)
    
    if not UTMIFY_TOKEN:
        print("\n❌ Token não definido! Atualize o .env")
        return
    
    for traffic_source in TRAFFIC_SOURCES:
        source_name = traffic_source or "all"
        logger.info(f"\n📊 Fonte: {source_name}")
        
        # Busca de todos os dashboards para esta fonte
        dashboards = []
        for dashboard_id in UTMIFY_DASHBOARD_IDS:
            data = fetch_dashboard(target_date, dashboard_id, traffic_source)
            if data:
                dashboards.append(data)
        
        if not dashboards:
            logger.warning(f"⚠️ Nenhum dado para fonte {source_name}")
            continue
        
        # Consolida e salva
        consolidated = consolidate_dashboards(dashboards)
        save_to_history(consolidated, target_date, traffic_source)
        
        # Print resumo
        orders = consolidated.get("ordersCount", {})
        comissions = consolidated.get("comissions", {})
        ads = consolidated.get("ads", {})
        analytics = consolidated.get("analytics", {})
        
        print(f"   ✅ Pedidos: {orders.get('approved', 0)} | "
              f"Faturamento: R$ {cents_to_decimal(comissions.get('gross', 0)):,.2f} | "
              f"Gasto: R$ {cents_to_decimal(ads.get('spent', 0)):,.2f} | "
              f"Lucro: R$ {cents_to_decimal(analytics.get('profit', 0)):,.2f}")
    
    print("\n" + "=" * 60)
    print("✅ Extração concluída!")
    print("=" * 60)


def parse_date(date_str: str) -> date:
    """Converte string para date"""
    formats = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"Formato inválido: {date_str}. Use DD/MM/YYYY")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python3 dashboard_extract_data.py DD/MM/YYYY")
        print("")
        print("Exemplos:")
        print("  python3 dashboard_extract_data.py 25/02/2026")
        print("  python3 dashboard_extract_data.py 2026-02-25")
        print("")
        print("Extrai dados para todas as fontes de tráfego:")
        print("  - all (todas)")
        print("  - Meta")
        print("  - Google")
        print("  - Kwai")
        print("  - TikTok")
        sys.exit(1)
    
    try:
        target_date = parse_date(sys.argv[1])
        extract_date(target_date)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)