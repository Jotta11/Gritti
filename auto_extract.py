#!/usr/bin/env python3
"""
Auto Extractor - Automação com Playwright
Faz login, captura token JWT, salva no .env e executa extrações
"""

import os
import subprocess
import time
import re
import json
import base64
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright
import logging

try:
    import pyotp
except ImportError:
    pyotp = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")


def read_env_file() -> dict:
    env_vars = {}
    if not os.path.exists(ENV_FILE):
        return env_vars
    with open(ENV_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars


ENV_VARS = read_env_file()


def config_value(key: str, default: str = "") -> str:
    value = os.getenv(key, ENV_VARS.get(key, default))
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def decode_jwt_payload(token: str) -> dict:
    """Decodifica payload JWT sem validar assinatura."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
    except Exception:
        return {}


def extract_bearer_token(auth_header: str) -> str:
    """Extrai token de um header Authorization Bearer."""
    if not auth_header or not auth_header.startswith("Bearer "):
        return ""
    token = auth_header.replace("Bearer ", "", 1).strip()
    if token.count(".") != 2:
        return ""
    return token


# =====================================================
# CONFIGURAÇÕES
# =====================================================

UTMIFY_URL = "https://app.utmify.com.br/dashboards/66668acc6670e6d0c7a17699/campanhas/"
UTMIFY_LOGIN_URL = "https://app.utmify.com.br/login"
UTMIFY_EMAIL = config_value("UTMIFY_EMAIL", "grupogritt@gmail.com")
UTMIFY_PASSWORD = config_value("UTMIFY_PASSWORD", "Projeto8d@")
UTMIFY_TOTP_SECRET = config_value("UTMIFY_TOTP_SECRET", "")
UTMIFY_TOKEN = config_value("UTMIFY_TOKEN", "")

VTURB_URL = "https://app.vturb.com/folders"
VTURB_LOGIN_URL = "https://app.vturb.com/login"
VTURB_EMAIL = config_value("VTURB_EMAIL", "anaclarabichuete@gmail.com")
VTURB_PASSWORD = config_value("VTURB_PASSWORD", "Projeto8d@")
VTURB_TARGET_ORG_EMAIL = config_value("VTURB_TARGET_ORG_EMAIL", "suportebumbashop@gmail.com")
VTURB_TOKEN = config_value("VTURB_TOKEN", "")
VTURB_HEALTHCHECK_PLAYER_ID = config_value("VTURB_HEALTHCHECK_PLAYER_ID", "693a3e45e891e679e7727765")
UTMIFY_SCRIPT = os.path.join(SCRIPT_DIR, "utmify_extract.py")
VTURB_SCRIPT = os.path.join(SCRIPT_DIR, "vturb_extract.py")

# Timeouts aumentados
PAGE_TIMEOUT = 120000  # 2 minutos
WAIT_AFTER_LOGIN = 15  # segundos
HEADLESS = config_value("PLAYWRIGHT_HEADLESS", "true").lower() in ("1", "true", "yes", "on")
TOKEN_EXPIRY_MARGIN_SECONDS = 300


# =====================================================
# FUNÇÕES DE TOKEN
# =====================================================

def save_token_to_env(token_name: str, token_value: str):
    """Salva o token no arquivo .env"""
    
    if not os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'w') as f:
            f.write("# Tokens\nUTMIFY_TOKEN=\nVTURB_TOKEN=\n")
    
    with open(ENV_FILE, 'r') as f:
        content = f.read()
    
    pattern = rf'^{token_name}=.*$'
    replacement = f'{token_name}={token_value}'
    
    if re.search(pattern, content, re.MULTILINE):
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        content += f'\n{replacement}'
    
    with open(ENV_FILE, 'w') as f:
        f.write(content)
    
    logger.info(f"💾 Token {token_name} salvo no .env ({len(token_value)} chars)")


def load_env():
    """Carrega variáveis do arquivo .env"""
    env_vars = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars


def get_runtime_token(token_name: str, fallback: str = "") -> str:
    token = os.getenv(token_name, "").strip()
    if token:
        return token
    env_token = load_env().get(token_name, "").strip()
    if env_token:
        return env_token
    return fallback.strip()


def is_token_not_expired(token: str, margin_seconds: int = TOKEN_EXPIRY_MARGIN_SECONDS) -> bool:
    if not token:
        return False
    payload = decode_jwt_payload(token)
    exp = payload.get("exp")
    if not exp:
        return False
    return int(time.time()) < (int(exp) - margin_seconds)


def is_utmify_token_active(token: str) -> bool:
    if not token or not is_token_not_expired(token):
        return False
    try:
        dashboard_id = re.search(r"/dashboards/([^/]+)/", UTMIFY_URL).group(1)
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {token}",
            "content-type": "application/json; charset=UTF-8",
            "origin": "https://app.utmify.com.br",
            "referer": "https://app.utmify.com.br/",
        }
        payload = {
            "level": "campaign",
            "dateRange": {"from": datetime.utcnow().strftime("%Y-%m-%dT00:00:00.000Z"), "to": datetime.utcnow().strftime("%Y-%m-%dT23:59:59.999Z")},
            "nameContains": None,
            "productNames": None,
            "orderBy": "greater_profit",
            "adObjectStatuses": None,
            "accountStatuses": None,
            "metaAdAccountIds": None,
            "dashboardId": dashboard_id,
        }
        response = requests.post(
            "https://server.utmify.com.br/orders/search-objects",
            headers=headers,
            json=payload,
            timeout=12,
        )
        return 200 <= response.status_code < 300
    except Exception:
        return False


def is_vturb_token_active(token: str) -> bool:
    if not token or not is_token_not_expired(token):
        return False
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        today_str = datetime.now().strftime("%Y-%m-%d")
        body = {
            "player_stats": {
                "player_id": VTURB_HEALTHCHECK_PLAYER_ID,
                "start_date": f"{today_str} 00:00:00",
                "end_date": f"{today_str} 23:59:59",
                "timezone": "America/Sao_Paulo",
            }
        }
        response = requests.post(
            f"https://api.vturb.com/vturb/v2/players/{VTURB_HEALTHCHECK_PLAYER_ID}/analytics_stream/player_stats",
            headers=headers,
            json=body,
            timeout=12,
        )
        return response.status_code == 200
    except Exception:
        return False


def ensure_utmify_token(playwright) -> str:
    current_token = get_runtime_token("UTMIFY_TOKEN", UTMIFY_TOKEN)
    if is_utmify_token_active(current_token):
        logger.info("✅ UTMIFY_TOKEN ativo. Pulando login.")
        return current_token
    logger.info("🔐 UTMIFY_TOKEN inválido/expirado. Fazendo novo login...")
    return get_utmify_token(playwright)


def ensure_vturb_token(playwright) -> str:
    current_token = get_runtime_token("VTURB_TOKEN", VTURB_TOKEN)
    if is_vturb_token_active(current_token):
        logger.info("✅ VTURB_TOKEN ativo. Pulando login.")
        return current_token
    logger.info("🔐 VTURB_TOKEN inválido/expirado. Fazendo novo login...")
    return get_vturb_token(playwright)


# =====================================================
# UTMIFY
# =====================================================

def get_utmify_token(playwright) -> str:
    logger.info("🚀 Iniciando captura do token Utmify...")
    
    browser = playwright.firefox.launch(headless=HEADLESS)
    context = browser.new_context()
    page = context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT)
    
    captured_token = None
    token_capture_enabled = False
    two_factor_submitted = False
    
    def handle_request(request):
        nonlocal captured_token
        if not token_capture_enabled:
            return
        auth = request.headers.get("authorization", "")
        token = extract_bearer_token(auth)
        if token and len(token) > 50:
            captured_token = token
    
    page.on("request", handle_request)

    def handle_utmify_2fa_if_needed() -> bool:
        nonlocal two_factor_submitted
        if two_factor_submitted:
            return True

        is_2fa_screen = (
            "verify-two-factor" in page.url.lower()
            or page.locator('input[name="code"], input#numeric-input').count() > 0
        )
        if not is_2fa_screen:
            return True

        if not UTMIFY_TOTP_SECRET:
            logger.error("❌ UTMIFY_TOTP_SECRET não configurado no .env")
            return False
        if pyotp is None:
            logger.error("❌ Biblioteca pyotp não instalada. Execute: pip3 install pyotp")
            return False

        try:
            secret = UTMIFY_TOTP_SECRET.replace(" ", "")
            code = pyotp.TOTP(secret).now()
            logger.info("🔐 Preenchendo código 2FA da UTMify...")

            code_input = page.locator('input[name="code"], input#numeric-input').first
            code_input.fill(code)
            page.click('button[type="submit"], button:has-text("Verificar"), button:has-text("Verify")')
            time.sleep(4)
            two_factor_submitted = True
            logger.info("✅ Código 2FA enviado")
            return True
        except Exception as e:
            logger.error(f"❌ Falha ao enviar 2FA da UTMify: {e}")
            return False
    
    try:
        # Vai direto para a página de login
        logger.info("📍 Acessando página de login Utmify...")
        page.goto(UTMIFY_LOGIN_URL, wait_until="networkidle", timeout=PAGE_TIMEOUT)
        time.sleep(3)
        
        logger.info(f"📍 URL atual: {page.url}")
        
        # Verifica se já está logado (redirecionou para dashboard)
        current_url = page.url.lower()
        already_logged = "dashboard" in current_url or "/dashboards/" in current_url
        if already_logged:
            logger.info("✅ Sessão UTMify já autenticada (dashboard detectado).")
        else:
            # Faz login
            logger.info("🔐 Fazendo login...")
            
            # Espera o campo de email aparecer
            email_selector = 'input[type="email"], input[name="email"], input[placeholder*="mail"], input[id*="email"]'
            page.wait_for_selector(email_selector, timeout=30000)
            page.fill(email_selector, UTMIFY_EMAIL)
            logger.info("✅ Email preenchido")
            
            # Preenche senha
            password_selector = 'input[type="password"]'
            page.fill(password_selector, UTMIFY_PASSWORD)
            logger.info("✅ Senha preenchida")
            
            # Clica no botão de login
            button_selector = 'button[type="submit"], button:has-text("Entrar"), button:has-text("Login"), button:has-text("Acessar")'
            page.click(button_selector)
            logger.info("✅ Botão clicado")
            
            # Aguarda autenticação
            logger.info(f"⏳ Aguardando autenticação ({WAIT_AFTER_LOGIN}s)...")
            time.sleep(WAIT_AFTER_LOGIN)

            if not handle_utmify_2fa_if_needed():
                return None

        # Caso já esteja na tela de 2FA por sessão anterior
        if not handle_utmify_2fa_if_needed():
            return None

        # Captura token somente depois do login + 2FA.
        token_capture_enabled = True

        logger.info("🔄 Navegando para campanhas para iniciar sessão autenticada...")
        page.goto(UTMIFY_URL, wait_until="networkidle", timeout=PAGE_TIMEOUT)
        time.sleep(2)

        logger.info("🔄 Recarregando página (F5) para gerar request com Bearer...")
        page.reload(wait_until="networkidle", timeout=PAGE_TIMEOUT)

        # Aguarda o Bearer aparecer; força novos F5 se necessário.
        for attempt in range(1, 4):
            if captured_token:
                break
            logger.info(f"⏳ Aguardando requisição autenticada ({attempt}/3)...")
            time.sleep(3)
            if not captured_token:
                page.reload(wait_until="networkidle", timeout=PAGE_TIMEOUT)
            
    except Exception as e:
        logger.error(f"❌ Erro no Utmify: {e}")
    finally:
        browser.close()
    
    if captured_token:
        save_token_to_env("UTMIFY_TOKEN", captured_token)
        logger.info("✅ Token Utmify capturado com sucesso!")
    else:
        logger.error("❌ Não foi possível capturar o token Utmify")
    
    return captured_token


# =====================================================
# VTURB
# =====================================================

def get_vturb_token(playwright) -> str:
    logger.info("🚀 Iniciando captura do token VTurb...")
    
    browser = playwright.firefox.launch(headless=HEADLESS)
    context = browser.new_context()
    page = context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT)
    
    preferred_token = None
    subscribed_token = None
    api_token = None
    fallback_token = None
    
    def handle_request(request):
        nonlocal preferred_token, subscribed_token, api_token, fallback_token

        auth = request.headers.get("authorization", "")
        token = extract_bearer_token(auth)
        if not token:
            return

        payload = decode_jwt_payload(token)
        if not payload:
            return

        req_url = request.url.lower()
        is_api = "api.vturb.com" in req_url
        is_player_analytics = "/vturb/v2/players/" in req_url and "analytics_stream" in req_url
        has_valid_subscription = bool((payload.get("company_data") or {}).get("has_valid_subscription"))

        if is_player_analytics:
            preferred_token = token
            return

        if is_api and has_valid_subscription and not subscribed_token:
            subscribed_token = token

        if is_api and not api_token:
            api_token = token

        if not fallback_token:
            fallback_token = token
    
    page.on("request", handle_request)

    def switch_vturb_organization():
        """Troca para a organização alvo no menu de perfil."""
        target = VTURB_TARGET_ORG_EMAIL
        if not target:
            return

        logger.info(f"🏢 Garantindo organização VTurb: {target}")
        
        def has_change_org_option() -> bool:
            options = [
                page.get_by_text("Trocar organização", exact=False),
                page.get_by_text("Change Organization", exact=False),
            ]
            return any(opt.count() > 0 for opt in options)

        def open_profile_menu() -> bool:
            profile_selectors = [
                'button[aria-haspopup="menu"]',
                '[data-testid*="profile"]',
                '[data-testid*="avatar"]',
                'img[alt*="avatar"]',
                'button:has-text("Conta")',
            ]
            for selector in profile_selectors:
                try:
                    page.locator(selector).first.click(timeout=3000)
                    time.sleep(0.8)
                    if has_change_org_option():
                        return True
                except Exception:
                    continue

            # Fallback direto: clica na "bolinha" do perfil (canto superior direito)
            try:
                vp = page.viewport_size or {"width": 1280, "height": 720}
                page.mouse.click(vp["width"] - 30, 40)
                time.sleep(0.8)
                if has_change_org_option():
                    return True
            except Exception:
                pass

            return False

        def click_change_organization() -> bool:
            candidates = [
                page.get_by_role("menuitem", name=re.compile("Change Organization|Trocar organização", re.I)),
                page.locator("button, div, a, li").filter(has_text=re.compile("Change Organization|Trocar organização", re.I)),
                page.get_by_text("Change Organization", exact=False),
                page.get_by_text("Trocar organização", exact=False),
            ]
            for loc in candidates:
                if loc.count() == 0:
                    continue
                try:
                    loc.first.click(timeout=5000, force=True)
                    time.sleep(1.0)
                    return True
                except Exception:
                    continue

            # Fallback por coordenada: linha 1 de ações logo abaixo do cabeçalho do perfil
            try:
                panel = page.locator("body").first
                box = panel.bounding_box()
                if box:
                    # área aproximada do primeiro item ("Change Organization")
                    page.mouse.click(box["width"] - 330, 245)
                    time.sleep(1.0)
                    return True
            except Exception:
                pass
            return False

        def click_target_organization(target_email: str) -> bool:
            # Primeiro tenta por role/container com nome completo
            primary_candidates = [
                page.get_by_role("menuitem", name=re.compile(re.escape(target_email), re.I)),
                page.locator("button, div, a, li").filter(has_text=re.compile(re.escape(target_email), re.I)),
            ]
            for loc in primary_candidates:
                if loc.count() == 0:
                    continue
                try:
                    loc.first.scroll_into_view_if_needed(timeout=3000)
                    loc.first.click(timeout=5000, force=True)
                    time.sleep(1.5)
                    return True
                except Exception:
                    continue

            # Fallback robusto: busca parcial e clica no centro do elemento visível
            partial = target_email.split("@")[0]
            text_candidates = [
                page.get_by_text(target_email, exact=False),
                page.get_by_text(partial, exact=False),
                page.locator("div, span").filter(has_text=re.compile(re.escape(partial), re.I)),
            ]
            for loc in text_candidates:
                if loc.count() == 0:
                    continue
                candidate = loc.first
                try:
                    candidate.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                try:
                    box = candidate.bounding_box()
                    if box:
                        x = box["x"] + (box["width"] / 2)
                        y = box["y"] + (box["height"] / 2)
                        page.mouse.click(x, y)
                        time.sleep(1.5)
                        return True
                except Exception:
                    pass
                try:
                    candidate.click(timeout=5000, force=True)
                    time.sleep(1.5)
                    return True
                except Exception:
                    pass
                try:
                    candidate.dispatch_event("click")
                    time.sleep(1.5)
                    return True
                except Exception:
                    pass

            return False

        for attempt in range(1, 4):
            if not open_profile_menu():
                logger.warning(f"⚠️ Tentativa {attempt}: não consegui abrir o menu de perfil")
                continue

            if not click_change_organization():
                logger.warning(f"⚠️ Tentativa {attempt}: não consegui clicar em Change/Trocar Organization")
                continue

            try:
                if click_target_organization(target):
                    logger.info(f"✅ Organização selecionada: {target}")
                    return
            except Exception:
                pass

            # fallback por coordenada aproximada da 2a organização da lista
            try:
                vp = page.viewport_size or {"width": 1280, "height": 720}
                page.mouse.click(vp["width"] - 260, 542)
                time.sleep(2)
                logger.info(f"✅ Organização selecionada: {target}")
                return
            except Exception:
                logger.warning(f"⚠️ Tentativa {attempt}: não consegui clicar em {target}")

        logger.warning(f"⚠️ Não foi possível trocar para a organização {target}")
    
    try:
        logger.info("📍 Acessando página de login VTurb...")
        page.goto(VTURB_LOGIN_URL, wait_until="networkidle", timeout=PAGE_TIMEOUT)
        time.sleep(3)
        
        logger.info(f"📍 URL atual: {page.url}")
        
        # Verifica se já está logado
        if "folders" in page.url.lower() and (preferred_token or subscribed_token or api_token or fallback_token):
            logger.info("✅ Já estava logado, token capturado!")
        else:
            # Faz login
            logger.info("🔐 Fazendo login...")
            
            # Espera o campo de email
            email_selector = 'input[type="email"], input[name="email"], input[id="email"], input[placeholder*="mail"]'
            page.wait_for_selector(email_selector, timeout=30000)
            page.fill(email_selector, VTURB_EMAIL)
            logger.info("✅ Email preenchido")
            
            # Preenche senha
            password_selector = 'input[type="password"]'
            page.fill(password_selector, VTURB_PASSWORD)
            logger.info("✅ Senha preenchida")
            
            # Clica no botão
            button_selector = 'button[type="submit"], button:has-text("Entrar"), button:has-text("Login")'
            page.click(button_selector)
            logger.info("✅ Botão clicado")
            
            # Aguarda
            logger.info(f"⏳ Aguardando autenticação ({WAIT_AFTER_LOGIN}s)...")
            time.sleep(WAIT_AFTER_LOGIN)

        switch_vturb_organization()
        
        # Se não capturou, navega para folders
        if not preferred_token:
            logger.info("🔄 Navegando para folders...")
            page.goto(VTURB_URL, wait_until="networkidle", timeout=PAGE_TIMEOUT)
            time.sleep(5)
            switch_vturb_organization()
        
        # Se ainda não capturou, recarrega
        if not preferred_token:
            logger.info("🔄 Recarregando página...")
            page.reload(wait_until="networkidle", timeout=PAGE_TIMEOUT)
            time.sleep(5)
            switch_vturb_organization()
            
    except Exception as e:
        logger.error(f"❌ Erro no VTurb: {e}")
    finally:
        browser.close()
    
    captured_token = preferred_token or subscribed_token or api_token or fallback_token

    if captured_token:
        payload = decode_jwt_payload(captured_token)
        cid = payload.get("cid", "desconhecido")
        has_sub = (payload.get("company_data") or {}).get("has_valid_subscription")
        logger.info(f"🔎 Token VTurb selecionado: cid={cid} has_valid_subscription={has_sub}")
        logger.info("✅ Token VTurb capturado com sucesso!")
        save_token_to_env("VTURB_TOKEN", captured_token)
    else:
        logger.error("❌ Não foi possível capturar o token VTurb")
    
    return captured_token


# =====================================================
# EXTRAÇÃO
# =====================================================

def run_extraction(script_path: str, command: str = "hoje", return_output: bool = False):
    if not os.path.exists(script_path):
        logger.error(f"❌ Script não encontrado: {script_path}")
        if return_output:
            return False, "", ""
        return False
    
    logger.info(f"🔄 Executando: {os.path.basename(script_path)} {command}")
    
    env = os.environ.copy()
    env.update(load_env())
    
    try:
        result = subprocess.run(
            ["python3", script_path, command],
            env=env,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        if result.returncode == 0:
            logger.info("✅ Extração concluída!")
            if return_output:
                return True, result.stdout, result.stderr
            return True
        else:
            logger.error("❌ Erro na extração")
            if return_output:
                return False, result.stdout, result.stderr
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        if return_output:
            return False, "", str(e)
        return False


def extract_summary_block(output: str) -> str:
    """Extrai o bloco de resumo (📈 RESUMO ... ✅ Extração concluída!) do stdout."""
    if not output:
        return ""
    lines = output.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if "📈 RESUMO" in line or line.strip() == "RESUMO":
            start_idx = i
            break
    if start_idx is None:
        return ""

    end_idx = None
    for i in range(start_idx, len(lines)):
        if "✅ Extração concluída!" in lines[i]:
            end_idx = i
            break
    if end_idx is None:
        end_idx = min(start_idx + 12, len(lines) - 1)

    block = "\n".join(lines[start_idx:end_idx + 1]).strip()
    return block


# =====================================================
# MAIN
# =====================================================

def extract_hoje():
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - DADOS DE HOJE")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)
    
    utmify_summary = ""
    utmify_ok = False

    with sync_playwright() as playwright:
        
        print("\n" + "=" * 60)
        print("📊 UTMIFY")
        print("=" * 60)
        
        if ensure_utmify_token(playwright):
            utmify_ok, utmify_stdout, _ = run_extraction(UTMIFY_SCRIPT, "hoje", return_output=True)
            utmify_summary = extract_summary_block(utmify_stdout)
        else:
            logger.error("❌ Falha ao capturar token Utmify")
        
        print("\n" + "=" * 60)
        print("📊 VTURB")
        print("=" * 60)
        
        if ensure_vturb_token(playwright):
            run_extraction(VTURB_SCRIPT, "hoje")
        else:
            logger.error("❌ Falha ao capturar token VTurb")
    
    print("\n" + "=" * 60)
    print("✅ EXTRAÇÃO AUTOMÁTICA CONCLUÍDA!")
    print("=" * 60)
    print("\n" + "=" * 60)
    print("📋 RESUMO UTMIFY EXTRAÍDO")
    print("=" * 60)
    print(f"📤 Extração UTMIFY: {'✅ Sim' if utmify_ok else '❌ Não'}")
    if utmify_summary:
        print(utmify_summary)
    else:
        print("⚠️ Resumo de métricas da UTMify não encontrado na saída.")


def extract_utmify_hoje():
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - UTMIFY HOJE")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)

    started_at = datetime.now()
    token_ok = False
    extraction_ok = False
    token_exp = None
    extracted_summary = ""

    with sync_playwright() as playwright:
        token = ensure_utmify_token(playwright)
        token_ok = bool(token)
        if token_ok:
            payload = decode_jwt_payload(token)
            exp_ts = payload.get("exp")
            if exp_ts:
                try:
                    token_exp = datetime.fromtimestamp(exp_ts).strftime("%d/%m/%Y %H:%M:%S")
                except Exception:
                    token_exp = None
            extraction_ok, stdout, _ = run_extraction(UTMIFY_SCRIPT, "hoje", return_output=True)
            extracted_summary = extract_summary_block(stdout)
        else:
            logger.error("❌ Falha ao capturar token Utmify")

    elapsed = datetime.now() - started_at
    print("\n" + "=" * 60)
    print("📋 RESUMO UTMIFY")
    print("=" * 60)
    print(f"🔑 Token capturado: {'✅ Sim' if token_ok else '❌ Não'}")
    if token_exp:
        print(f"⏰ Expiração do token: {token_exp}")
    print(f"📤 Extração executada: {'✅ Sim' if extraction_ok else '❌ Não'}")
    print(f"⏱️ Duração: {str(elapsed).split('.')[0]}")
    print("=" * 60)
    if extracted_summary:
        print(extracted_summary)
    else:
        print("⚠️ Resumo de métricas não encontrado na saída da extração.")


def extract_vturb_hoje():
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - VTURB HOJE")
    print("=" * 60)
    
    with sync_playwright() as playwright:
        if ensure_vturb_token(playwright):
            run_extraction(VTURB_SCRIPT, "hoje")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python3 auto_extract.py [hoje|utmify|vturb]")
        sys.exit(1)
    
    cmd = sys.argv[1].lower()
    
    if cmd == "hoje":
        extract_hoje()
    elif cmd == "utmify":
        extract_utmify_hoje()
    elif cmd == "vturb":
        extract_vturb_hoje()
    else:
        print(f"❌ Comando inválido: {cmd}")
