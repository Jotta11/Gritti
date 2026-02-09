#!/usr/bin/env python3
"""
Utmify Ads/Criativos Extractor
Comandos: python utmify_ads_extract.py hoje | ontem
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
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpczJGQSI6dHJ1ZSwiaXNNb2JpbGVXaXRoT2xkVmVyc2lvbiI6ZmFsc2UsImV4cCI6MTc3MDY0ODQ0NSwiaWF0IjoxNzcwNjQ0ODQ1LCJzdWIiOiI2NjY2OGFjYzY2NzBlNmQwYzdhMTc2OTcifQ.ol0Vg0S8E10i1iFtG96WknzYonIdZKZ7eFNbkCrpCh4"
)

# Timeout/retry configuráveis por env
UTMIFY_TIMEOUT = int(os.getenv("UTMIFY_TIMEOUT", "180"))        # antes era 60
UTMIFY_RETRIES = int(os.getenv("UTMIFY_RETRIES", "3"))          # tentativas
UTMIFY_BACKOFF = float(os.getenv("UTMIFY_BACKOFF", "1.0"))      # backoff base

# ✅ Agora suporta múltiplos dashboards
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
        allowed_methods=frozenset(["POST"]),  # urllib3 >= 1.26
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


# =====================================================
# FUNÇÕES AUXILIARES
# =====================================================

def cents_to_decimal(value):
    return round(value / 100, 2) if value else None


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


# =====================================================
# API
# =====================================================

def fetch_ads(target_date: date) -> Dict[str, Any]:
    """Busca anúncios/criativos da API do Utmify para múltiplos dashboards e consolida os resultados"""

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
    seen_ad_ids = set()

    logger.info(f"🔄 Buscando ads de {target_date.strftime('%d/%m/%Y')} em {len(UTMIFY_DASHBOARD_IDS)} dashboards...")

    for dashboard_id in UTMIFY_DASHBOARD_IDS:
        payload = {
            "level": "ad",
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

            # Se retornou algo fora de 2xx, tenta logar o conteúdo pra debug
            if not (200 <= response.status_code < 300):
                snippet = ""
                try:
                    snippet = str(response.text)[:300]
                except Exception:
                    pass
                logger.warning(f"⚠️ Dashboard {dashboard_id}: HTTP {response.status_code} | {snippet}")

                # Se falhou, segue para o próximo dashboard
                continue

            data = response.json()
            results = data.get("results", []) or []
            logger.info(f"✅ Dashboard {dashboard_id}: {len(results)} anúncios encontrados")

            # Dedup por ID do anúncio (caso apareça em mais de um dashboard)
            for a in results:
                ad_id = a.get("adId") or a.get("id")
                if not ad_id:
                    continue
                if ad_id in seen_ad_ids:
                    continue
                seen_ad_ids.add(ad_id)
                all_results.append(a)

        except requests.exceptions.ReadTimeout:
            logger.error(f"⏳ Timeout no dashboard {dashboard_id} (>{UTMIFY_TIMEOUT}s). Pulando para o próximo...")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erro de rede no dashboard {dashboard_id}: {e}. Pulando para o próximo...")
            continue
        except Exception as e:
            logger.error(f"❌ Erro inesperado no dashboard {dashboard_id}: {e}. Pulando para o próximo...")
            continue

    logger.info(f"📦 Consolidado: {len(all_results)} anúncios únicos (dedup por adId/id)")
    return {"results": all_results}


# =====================================================
# PREPARAÇÃO DOS DADOS
# =====================================================

def prepare_ad_values(ads: list, report_date: date) -> list:
    """Prepara valores dos anúncios para inserção"""
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


# =====================================================
# DATABASE
# =====================================================

def save_to_history(ads: list, report_date: date) -> int:
    """Salva anúncios na tabela de histórico (UPSERT)"""

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
            profit_margin = EXCLUDED.profit_margin,
            cpa = EXCLUDED.cpa,
            impressions = EXCLUDED.impressions,
            clicks = EXCLUDED.clicks,
            total_orders = EXCLUDED.total_orders,
            approved_orders = EXCLUDED.approved_orders,
            pending_orders = EXCLUDED.pending_orders,
            initiate_checkout = EXCLUDED.initiate_checkout,
            video_views = EXCLUDED.video_views,
            hook_rate = EXCLUDED.hook_rate,
            retention = EXCLUDED.retention,
            hook_play_rate = EXCLUDED.hook_play_rate
    """

    execute_values(cursor, query, values)
    count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"✅ {count} anúncios salvos em ads_history")
    return count


def save_to_today(ads: list, report_date: date) -> int:
    """Salva anúncios na tabela de hoje (TRUNCATE + INSERT)"""

    if not ads:
        logger.warning("⚠️ Nenhum anúncio para salvar")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE ads_today")
    logger.info("🗑️ Tabela ads_today limpa")

    values = prepare_ad_values(ads, report_date)

    query = """
        INSERT INTO ads_today (
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
    """

    execute_values(cursor, query, values)
    count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"✅ {count} anúncios salvos em ads_today")
    return count


# =====================================================
# EXTRAÇÃO
# =====================================================

def print_summary(ads: list):
    """Imprime resumo dos anúncios"""
    total_spend = sum(a.get("spend", 0) / 100 for a in ads)
    total_revenue = sum(a.get("revenue", 0) / 100 for a in ads)
    total_profit = sum(a.get("profit", 0) / 100 for a in ads)
    total_orders = sum(a.get("approvedOrdersCount", 0) for a in ads)

    # Agrupar por nome do criativo
    by_name = {}
    for a in ads:
        name = a.get("name", "Sem nome")
        if name not in by_name:
            by_name[name] = {"count": 0, "spend": 0, "profit": 0}
        by_name[name]["count"] += 1
        by_name[name]["spend"] += a.get("spend", 0) / 100
        by_name[name]["profit"] += a.get("profit", 0) / 100

    print("\n" + "=" * 50)
    print("📈 RESUMO")
    print("=" * 50)
    print(f"Total de anúncios: {len(ads)}")
    print(f"Criativos únicos: {len(by_name)}")
    print(f"Spend: R$ {total_spend:,.2f}")
    print(f"Revenue: R$ {total_revenue:,.2f}")
    print(f"Profit: R$ {total_profit:,.2f}")
    print(f"Vendas: {total_orders}")
    if total_spend > 0:
        print(f"ROAS: {total_revenue/total_spend:.2f}x")

    print("\n📊 TOP 5 CRIATIVOS POR PROFIT:")
    sorted_names = sorted(by_name.items(), key=lambda x: x[1]["profit"], reverse=True)[:5]
    for name, data in sorted_names:
        print(f"  • {name}: R$ {data['profit']:,.2f} ({data['count']} anúncios)")

    print("=" * 50)
    print("✅ Extração concluída!")


def extract_today():
    """Extrai dados de HOJE → ads_today"""

    target_date = date.today()

    print("=" * 50)
    print(f"📊 UTMIFY ADS EXTRACTOR - HOJE")
    print(f"📅 Data: {target_date.strftime('%d/%m/%Y')}")
    print(f"💾 Destino: ads_today")
    print("=" * 50)

    try:
        data = fetch_ads(target_date)
        ads = data.get("results", [])

        if not ads:
            print("\n⚠️ Nenhum anúncio encontrado")
            return

        save_to_today(ads, target_date)
        print_summary(ads)

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("\n❌ Token expirado ou inválido!")
        else:
            print(f"\n❌ Erro HTTP: {e}")
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        raise


def extract_yesterday():
    """Extrai dados de ONTEM → ads_history"""

    target_date = date.today() - timedelta(days=1)

    print("=" * 50)
    print(f"📊 UTMIFY ADS EXTRACTOR - ONTEM")
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
        print_summary(ads)

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("\n❌ Token expirado ou inválido!")
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
        print("Uso: python utmify_ads_extract.py [hoje|ontem]")
        print("")
        print("Comandos:")
        print("  hoje   - Extrai anúncios de hoje (salva em ads_today)")
        print("  ontem  - Extrai anúncios de ontem (salva em ads_history)")
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