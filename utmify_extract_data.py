#!/usr/bin/env python3
"""
Utmify Data Extractor - Data Personalizada (multi-dashboard + retry/timeout)
Uso: python utmify_extract_data.py DD/MM/YYYY
"""

import os
import requests
from datetime import datetime, timedelta, date
from typing import Dict, Any
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
# CONFIGURAÇÕES
# =====================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database-2.cdg6qmmuuc8e.us-east-2.rds.amazonaws.com"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "Gritti2"),
    "user": os.getenv("DB_USER", "ancher"),
    "password": os.getenv("DB_PASSWORD", "Spirorbis7-Swab7"),
}

UTMIFY_TOKEN = os.getenv(
    "UTMIFY_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpczJGQSI6dHJ1ZSwiaXNNb2JpbGVXaXRoT2xkVmVyc2lvbiI6ZmFsc2UsImV4cCI6MTc3MDY1NDQ5MCwiaWF0IjoxNzcwNjUwODkwLCJzdWIiOiI2NjY2OGFjYzY2NzBlNmQwYzdhMTc2OTcifQ.m5A4FI-WRcCYXjKlXraJk1Xij-3iYQUUhy5oiGsQbZ4"
)

# Timeout/retry configuráveis por env (recomendado por causa de múltiplos dashboards)
UTMIFY_TIMEOUT = int(os.getenv("UTMIFY_TIMEOUT", "180"))        # antes era 60
UTMIFY_RETRIES = int(os.getenv("UTMIFY_RETRIES", "3"))          # tentativas
UTMIFY_BACKOFF = float(os.getenv("UTMIFY_BACKOFF", "1.0"))      # backoff base

# ✅ Multi dashboards (os mesmos que você já usou)
# Você pode definir por env também:
# export UTMIFY_DASHBOARD_IDS="66668acc6670e6d0c7a17699,697179f7cfa58e5d2a21afdf,6972dc7473de5f488a3aee2b"
DEFAULT_DASHBOARD_IDS = [
    os.getenv("UTMIFY_DASHBOARD_ID", "66668acc6670e6d0c7a17699"),  # mantém compatibilidade
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
# FUNÇÕES
# =====================================================

def fetch_campaigns(target_date: date) -> Dict[str, Any]:
    """Busca campanhas da API do Utmify (multi-dashboard) e consolida os resultados"""

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {UTMIFY_TOKEN}",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://app.utmify.com.br",
        "referer": "https://app.utmify.com.br/",
    }

    date_from = target_date.strftime("%Y-%m-%dT03:00:00.000Z")
    date_to = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT02:59:59.999Z")

    all_results = []
    seen_campaign_ids = set()

    logger.info(f"🔄 Buscando campaigns de {target_date.strftime('%d/%m/%Y')} em {len(UTMIFY_DASHBOARD_IDS)} dashboards...")

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

            # Dedup por campaign_id (id) para evitar duplicações entre dashboards
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
    return round(value / 100, 2) if value else None


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("-0300", "-03:00").replace("-0200", "-02:00"))
    except Exception:
        return None


def prepare_campaign_values(campaigns: list, report_date: date) -> list:
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
    """Salva campanhas na tabela de histórico"""

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


def parse_date(date_str: str) -> date:
    """Converte string para date (DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD)"""
    formats = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Formato de data inválido: {date_str}. Use DD/MM/YYYY")


def extract(target_date: date):
    """Extrai e salva dados de uma data específica"""

    print("=" * 50)
    print(f"📊 UTMIFY EXTRACTOR - DATA PERSONALIZADA")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: campaigns_history")
    print("=" * 50)

    try:
        data = fetch_campaigns(target_date)
        campaigns = data.get("results", [])

        if not campaigns:
            print("\n⚠️ Nenhuma campanha encontrada para esta data")
            return

        save_to_history(campaigns, target_date)

        # Resumo
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

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            print("\n❌ ERRO: Token expirado ou inválido!")
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
        print("Uso: python utmify_extract_data.py DD/MM/YYYY")
        print("")
        print("Exemplos:")
        print("  python utmify_extract_data.py 05/01/2026")
        print("  python utmify_extract_data.py 01-01-2026")
        print("  python utmify_extract_data.py 2026-01-05")
        sys.exit(1)

    try:
        target_date = parse_date(sys.argv[1])
        extract(target_date)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)