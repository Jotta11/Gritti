#!/usr/bin/env python3
"""
Utmify Data Extractor - Versão Simplificada
Comandos: python utmify_extract.py hoje | ontem
"""

import os
import requests
import json
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
import logging

import psycopg2
from psycopg2.extras import execute_values

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurar logging
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

# Banco de dados PostgreSQL (lê do .env)
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Utmify
UTMIFY_TOKEN = os.getenv("UTMIFY_TOKEN", "")

# Timeout/retry configuráveis por env
UTMIFY_TIMEOUT = int(os.getenv("UTMIFY_TIMEOUT", "180"))
UTMIFY_RETRIES = int(os.getenv("UTMIFY_RETRIES", "3"))
UTMIFY_BACKOFF = float(os.getenv("UTMIFY_BACKOFF", "1.0"))

# Múltiplos dashboards
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

logger.info(f"🧩 Dashboards ativos: {UTMIFY_DASHBOARD_IDS}")
logger.info(f"⏱️ Timeout: {UTMIFY_TIMEOUT}s | Retries: {UTMIFY_RETRIES} | Backoff: {UTMIFY_BACKOFF}")
logger.info(f"🔑 Token: {'✅ Definido' if UTMIFY_TOKEN else '❌ Não definido'}")


# =====================================================
# HTTP SESSION COM RETRY
# =====================================================

def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=UTMIFY_RETRIES,
        connect=UTMIFY_RETRIES,
        read=UTMIFY_RETRIES,
        status=UTMIFY_RETRIES,
        backoff_factor=UTMIFY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


# =====================================================
# EXTRATOR
# =====================================================

def fetch_campaigns(target_date: date) -> Dict[str, Any]:
    """Busca campanhas da API do Utmify para múltiplos dashboards e consolida os resultados"""

    if not UTMIFY_TOKEN:
        logger.error("❌ UTMIFY_TOKEN não definido!")
        return {"results": []}

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

    all_results = []
    seen_campaign_ids = set()

    logger.info(
        f"🔄 Buscando campaigns de {target_date.strftime('%d/%m/%Y')} em {len(UTMIFY_DASHBOARD_IDS)} dashboards..."
    )

    for dashboard_id in UTMIFY_DASHBOARD_IDS:
        payload = {
            "level": "campaign",
            "dateRange": {"from": date_from, "to": date_to},
            "nameContains": None,
            "productNames": None,
            "orderBy": "greater_profit",
            "adObjectStatuses": None,
            "accountStatuses": None,
            "metaAdAccountIds": None,
            "dashboardId": dashboard_id
        }

        logger.info(f"➡️ Dashboard {dashboard_id}: buscando...")

        try:
            response = SESSION.post(
                "https://server.utmify.com.br/orders/search-objects",
                headers=headers,
                json=payload,
                timeout=UTMIFY_TIMEOUT
            )

            if not (200 <= response.status_code < 300):
                snippet = ""
                try:
                    snippet = str(response.text)[:300]
                except Exception:
                    pass
                logger.warning(f"⚠️ Dashboard {dashboard_id}: HTTP {response.status_code} | {snippet}")
                continue

            data = response.json()
            results = data.get("results", []) or []
            logger.info(f"✅ Dashboard {dashboard_id}: {len(results)} campanhas encontradas")

            # Dedup por campaignId (id)
            for c in results:
                cid = c.get("id")
                if not cid:
                    continue
                if cid in seen_campaign_ids:
                    continue
                seen_campaign_ids.add(cid)
                all_results.append(c)

        except requests.exceptions.ReadTimeout:
            logger.error(f"⏳ Timeout no dashboard {dashboard_id} (>{UTMIFY_TIMEOUT}s). Pulando para o próximo...")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erro de rede no dashboard {dashboard_id}: {e}. Pulando para o próximo...")
            continue
        except Exception as e:
            logger.error(f"❌ Erro inesperado no dashboard {dashboard_id}: {e}. Pulando para o próximo...")
            continue

    logger.info(f"📦 Consolidado: {len(all_results)} campanhas únicas (dedup por id)")
    return {"results": all_results}


def cents_to_decimal(value):
    """Converte centavos para decimal"""
    return round(value / 100, 2) if value else None


def parse_datetime(value):
    """Parse de datetime string"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("-0300", "-03:00").replace("-0200", "-02:00"))
    except Exception:
        return None


def prepare_campaign_values(campaigns: list, report_date: date) -> list:
    """Prepara valores das campanhas para inserção"""
    values = []
    for c in campaigns:
        values.append((
            c.get("id"),
            report_date,
            c.get("name"),
            c.get("level", "campaign"),
            c.get("status"),
            c.get("effectiveStatus"),
            c.get("accountId"),
            c.get("ca"),
            c.get("profileId"),
            cents_to_decimal(c.get("dailyBudget")),
            cents_to_decimal(c.get("lifetimeBudget")),
            cents_to_decimal(c.get("spend")),
            cents_to_decimal(c.get("revenue")),
            cents_to_decimal(c.get("grossRevenue")),
            cents_to_decimal(c.get("profit")),
            cents_to_decimal(c.get("fees")),
            cents_to_decimal(c.get("tax")),
            cents_to_decimal(c.get("productCosts")),
            c.get("roas", 0),
            c.get("roi", 0),
            c.get("profitMargin", 0),
            cents_to_decimal(c.get("cpa")),
            cents_to_decimal(c.get("cpm")),
            cents_to_decimal(c.get("costPerInlineLinkClick")),
            c.get("inlineLinkClickCtr", 0),
            c.get("impressions", 0),
            c.get("inlineLinkClicks", 0),
            c.get("frequency", 0),
            c.get("totalOrdersCount", 0),
            c.get("approvedOrdersCount", 0),
            c.get("pendingOrdersCount", 0),
            c.get("refundedOrdersCount", 0),
            c.get("refusedOrdersCount", 0),
            c.get("salesFromFacebook", 0),
            cents_to_decimal(c.get("pendingRevenue")),
            cents_to_decimal(c.get("refundedRevenue")),
            c.get("initiateCheckout", 0),
            cents_to_decimal(c.get("costPerInitiateCheckout")),
            c.get("checkoutConversion", 0),
            c.get("clickConversion", 0),
            c.get("landingPageViews", 0),
            c.get("leads", 0),
            cents_to_decimal(c.get("costPerLead")),
            c.get("videoViews", 0),
            c.get("video75Watched", 0),
            c.get("videoViews3Seconds", 0),
            c.get("hook", 0),
            c.get("retention", 0),
            c.get("hookPlayRate", 0),
            c.get("conversations", 0),
            cents_to_decimal(c.get("costPerConversation")),
            parse_datetime(c.get("createdTime")),
        ))
    return values


def save_to_history(campaigns: list, report_date: date) -> int:
    """Salva campanhas na tabela de histórico (ontem e anteriores)"""

    if not campaigns:
        logger.warning("⚠️ Nenhuma campanha para salvar")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    values = prepare_campaign_values(campaigns, report_date)

    query = """
        INSERT INTO campaigns_history (
            campaign_id, report_date, name, level, status, effective_status,
            account_id, ca, profile_id, daily_budget, lifetime_budget,
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
        ON CONFLICT (campaign_id, report_date) DO UPDATE SET
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
            approved_orders = EXCLUDED.approved_orders
    """

    execute_values(cursor, query, values)
    count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"✅ {count} campanhas salvas em campaigns_history")
    return count


def save_to_today(campaigns: list, report_date: date) -> int:
    """Salva campanhas na tabela de hoje (limpa e insere)"""

    if not campaigns:
        logger.warning("⚠️ Nenhuma campanha para salvar")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Limpa tabela de hoje antes de inserir
    cursor.execute("TRUNCATE TABLE campaigns_today")
    logger.info("🗑️ Tabela campaigns_today limpa")

    values = prepare_campaign_values(campaigns, report_date)

    query = """
        INSERT INTO campaigns_today (
            campaign_id, report_date, name, level, status, effective_status,
            account_id, ca, profile_id, daily_budget, lifetime_budget,
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
    """

    execute_values(cursor, query, values)
    count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"✅ {count} campanhas salvas em campaigns_today")
    return count


def extract_today():
    """Extrai dados de HOJE e salva em campaigns_today"""

    target_date = date.today()

    print("=" * 50)
    print(f"📊 UTMIFY EXTRACTOR - HOJE")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"🔑 Token: {'✅ Definido' if UTMIFY_TOKEN else '❌ Não definido'}")
    print("=" * 50)

    if not UTMIFY_TOKEN:
        print("\n❌ Execute: python3 auto_extract.py utmify")
        return

    try:
        data = fetch_campaigns(target_date)
        campaigns = data.get("results", [])

        if not campaigns:
            print("\n⚠️ Nenhuma campanha encontrada para esta data")
            return

        save_to_today(campaigns, target_date)
        print_summary(campaigns)

    except requests.exceptions.HTTPError as e:
        handle_http_error(e)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        raise


def extract_yesterday():
    """Extrai dados de ONTEM e salva em campaigns_history"""

    target_date = date.today() - timedelta(days=1)

    print("=" * 50)
    print(f"📊 UTMIFY EXTRACTOR - ONTEM")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"🔑 Token: {'✅ Definido' if UTMIFY_TOKEN else '❌ Não definido'}")
    print("=" * 50)

    if not UTMIFY_TOKEN:
        print("\n❌ Execute: python3 auto_extract.py utmify")
        return

    try:
        data = fetch_campaigns(target_date)
        campaigns = data.get("results", [])

        if not campaigns:
            print("\n⚠️ Nenhuma campanha encontrada para esta data")
            return

        save_to_history(campaigns, target_date)
        print_summary(campaigns)

    except requests.exceptions.HTTPError as e:
        handle_http_error(e)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        raise


def print_summary(campaigns: list):
    """Imprime resumo das campanhas"""
    total_spend = sum(c.get("spend", 0) / 100 for c in campaigns)
    total_revenue = sum(c.get("revenue", 0) / 100 for c in campaigns)
    total_profit = sum(c.get("profit", 0) / 100 for c in campaigns)
    total_orders = sum(c.get("approvedOrdersCount", 0) for c in campaigns)

    print("\n" + "=" * 50)
    print("📈 RESUMO")
    print("=" * 50)
    print(f"Campanhas: {len(campaigns)}")
    print(f"Spend: R$ {total_spend:,.2f}")
    print(f"Revenue: R$ {total_revenue:,.2f}")
    print(f"Profit: R$ {total_profit:,.2f}")
    print(f"Vendas: {total_orders}")
    if total_spend > 0:
        print(f"ROAS: {total_revenue/total_spend:.2f}x")
    print("=" * 50)
    print("✅ Extração concluída!")


def handle_http_error(e):
    """Trata erros HTTP"""
    if e.response.status_code == 401:
        print("\n❌ ERRO: Token expirado ou inválido!")
        print("   Execute: python3 auto_extract.py utmify")
    else:
        print(f"\n❌ Erro HTTP: {e}")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python utmify_extract.py [hoje|ontem]")
        print("")
        print("Comandos:")
        print("  hoje   - Extrai dados do dia atual (salva em campaigns_today)")
        print("  ontem  - Extrai dados do dia anterior (salva em campaigns_history)")
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