#!/usr/bin/env python3
"""
Auto Extractor - Automação com Playwright
Faz login, captura token JWT e executa extrações automaticamente
"""

import os
import subprocess
import time
import json
from datetime import datetime
from playwright.sync_api import sync_playwright, Page
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =====================================================
# CONFIGURAÇÕES
# =====================================================

# Utmify
UTMIFY_URL = "https://app.utmify.com.br/login/?t=1770734115134"
UTMIFY_EMAIL = "grupogritt@gmail.com"
UTMIFY_PASSWORD = "Projeto8d@"

# VTurb
VTURB_URL = "https://app.vturb.com/folders"
VTURB_LOGIN_URL = "https://app.vturb.com/login"
VTURB_EMAIL = "anaclarabichuete@gmail.com"
VTURB_PASSWORD = "Projeto8d@"

# Caminhos dos scripts de extração
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
UTMIFY_SCRIPT = os.path.join(SCRIPT_DIR, "utmify_extract.py")
VTURB_SCRIPT = os.path.join(SCRIPT_DIR, "vturb_extract.py")


# =====================================================
# UTMIFY
# =====================================================

def get_utmify_token(playwright) -> str:
    """Faz login no Utmify e captura o token JWT"""
    
    logger.info("🚀 Iniciando captura do token Utmify...")
    
    # Usa Firefox (mais estável no Mac) com headless=False para debug
    browser = playwright.firefox.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    
    captured_token = None
    
    def handle_request(request):
        nonlocal captured_token
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer ") and len(auth_header) > 50:
            captured_token = auth_header.replace("Bearer ", "")
            logger.info("✅ Token Utmify capturado!")
    
    page.on("request", handle_request)
    
    try:
        # Acessa a página (vai redirecionar para login)
        logger.info("📍 Acessando Utmify...")
        page.goto(UTMIFY_URL, wait_until="networkidle", timeout=60000)
        
        # Aguarda carregar
        time.sleep(3)
        
        # Verifica se precisa fazer login
        current_url = page.url.lower()
        logger.info(f"📍 URL atual: {page.url}")
        
        if "login" in current_url or "signin" in current_url or "auth" in current_url:
            logger.info("🔐 Página de login detectada, fazendo login...")
            
            # Preenche email
            email_input = page.wait_for_selector('input[type="email"], input[name="email"], input[placeholder*="mail"]', timeout=10000)
            email_input.fill(UTMIFY_EMAIL)
            
            # Preenche senha
            password_input = page.wait_for_selector('input[type="password"]', timeout=10000)
            password_input.fill(UTMIFY_PASSWORD)
            
            # Clica no botão de login
            login_button = page.query_selector('button[type="submit"], button:has-text("Entrar"), button:has-text("Login")')
            if login_button:
                login_button.click()
            else:
                password_input.press("Enter")
            
            # Aguarda redirecionamento
            logger.info("⏳ Aguardando autenticação...")
            time.sleep(10)
        
        # Se ainda não capturou, navega para forçar requisições
        if not captured_token:
            logger.info("🔄 Navegando para campanhas...")
            page.goto(UTMIFY_URL, wait_until="networkidle", timeout=60000)
            time.sleep(5)
        
        # Se ainda não capturou, recarrega
        if not captured_token:
            logger.info("🔄 Forçando requisição...")
            page.reload()
            time.sleep(5)
        
    except Exception as e:
        logger.error(f"❌ Erro no Utmify: {e}")
    finally:
        browser.close()
    
    return captured_token


# =====================================================
# VTURB
# =====================================================

def get_vturb_token(playwright) -> str:
    """Faz login no VTurb e captura o token JWT"""
    
    logger.info("🚀 Iniciando captura do token VTurb...")
    
    # Usa Firefox (mais estável no Mac) com headless=False para debug
    browser = playwright.firefox.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    
    captured_token = None
    
    def handle_request(request):
        nonlocal captured_token
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer ") and len(auth_header) > 100:
            captured_token = auth_header.replace("Bearer ", "")
            logger.info("✅ Token VTurb capturado!")
    
    page.on("request", handle_request)
    
    try:
        # Acessa página de login
        logger.info("📍 Acessando VTurb...")
        page.goto(VTURB_LOGIN_URL, wait_until="networkidle", timeout=60000)
        
        time.sleep(2)
        
        # Preenche email
        logger.info("🔐 Fazendo login...")
        email_input = page.wait_for_selector('input[type="email"], input[name="email"], input[id="email"]', timeout=10000)
        email_input.fill(VTURB_EMAIL)
        
        # Preenche senha
        password_input = page.wait_for_selector('input[type="password"]', timeout=10000)
        password_input.fill(VTURB_PASSWORD)
        
        # Clica no botão de login
        login_button = page.query_selector('button[type="submit"]')
        if login_button:
            login_button.click()
        else:
            password_input.press("Enter")
        
        # Aguarda redirecionamento
        logger.info("⏳ Aguardando autenticação...")
        time.sleep(10)
        
        # Navega para a página de folders para forçar requisições
        if "folders" not in page.url:
            page.goto(VTURB_URL, wait_until="networkidle", timeout=30000)
            time.sleep(5)
        
        # Se não capturou, tenta recarregar
        if not captured_token:
            logger.info("🔄 Forçando requisição...")
            page.reload()
            time.sleep(5)
        
    except Exception as e:
        logger.error(f"❌ Erro no VTurb: {e}")
    finally:
        browser.close()
    
    return captured_token


# =====================================================
# EXTRAÇÃO
# =====================================================

def run_extraction(script_path: str, token_env: str, token: str, command: str = "hoje"):
    """Executa script de extração com o token"""
    
    if not token:
        logger.error(f"❌ Token não disponível para {script_path}")
        return False
    
    if not os.path.exists(script_path):
        logger.error(f"❌ Script não encontrado: {script_path}")
        return False
    
    logger.info(f"🔄 Executando: {os.path.basename(script_path)} {command}")
    
    # Define o token como variável de ambiente
    env = os.environ.copy()
    env[token_env] = token
    
    try:
        # Tenta python3 primeiro (Mac/Linux), depois python (Windows)
        python_cmd = "python3"
        result = subprocess.run(
            [python_cmd, script_path, command],
            env=env,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info(f"✅ Extração concluída!")
            print(result.stdout)
            return True
        else:
            logger.error(f"❌ Erro na extração:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Timeout na extração")
        return False
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        return False


# =====================================================
# MAIN
# =====================================================

def extract_hoje():
    """Executa extração completa de HOJE"""
    
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - DADOS DE HOJE")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)
    
    with sync_playwright() as playwright:
        
        # === UTMIFY ===
        print("\n" + "=" * 60)
        print("📊 UTMIFY")
        print("=" * 60)
        
        utmify_token = get_utmify_token(playwright)
        
        if utmify_token:
            run_extraction(UTMIFY_SCRIPT, "UTMIFY_TOKEN", utmify_token, "hoje")
        else:
            logger.error("❌ Falha ao capturar token Utmify")
        
        # === VTURB ===
        print("\n" + "=" * 60)
        print("📊 VTURB")
        print("=" * 60)
        
        vturb_token = get_vturb_token(playwright)
        
        if vturb_token:
            run_extraction(VTURB_SCRIPT, "VTURB_TOKEN", vturb_token, "hoje")
        else:
            logger.error("❌ Falha ao capturar token VTurb")
    
    print("\n" + "=" * 60)
    print("✅ EXTRAÇÃO AUTOMÁTICA CONCLUÍDA!")
    print("=" * 60)


def extract_utmify_hoje():
    """Extrai apenas Utmify hoje"""
    
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - UTMIFY HOJE")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)
    
    with sync_playwright() as playwright:
        utmify_token = get_utmify_token(playwright)
        
        if utmify_token:
            run_extraction(UTMIFY_SCRIPT, "UTMIFY_TOKEN", utmify_token, "hoje")
        else:
            logger.error("❌ Falha ao capturar token Utmify")


def extract_vturb_hoje():
    """Extrai apenas VTurb hoje"""
    
    print("=" * 60)
    print("🤖 AUTO EXTRACTOR - VTURB HOJE")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)
    
    with sync_playwright() as playwright:
        vturb_token = get_vturb_token(playwright)
        
        if vturb_token:
            run_extraction(VTURB_SCRIPT, "VTURB_TOKEN", vturb_token, "hoje")
        else:
            logger.error("❌ Falha ao capturar token VTurb")


def show_help():
    print("""
Auto Extractor - Automação com Playwright

Uso: python auto_extract.py [comando]

Comandos:
  hoje      - Extrai Utmify + VTurb (dados de hoje)
  utmify    - Extrai apenas Utmify (dados de hoje)
  vturb     - Extrai apenas VTurb (dados de hoje)
  
Exemplos:
  python auto_extract.py hoje
  python auto_extract.py utmify
  python auto_extract.py vturb

Requisitos:
  pip install playwright
  playwright install chromium
""")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)
    
    comando = sys.argv[1].lower()
    
    if comando == "hoje":
        extract_hoje()
    elif comando == "utmify":
        extract_utmify_hoje()
    elif comando == "vturb":
        extract_vturb_hoje()
    elif comando in ["-h", "--help", "help"]:
        show_help()
    else:
        print(f"❌ Comando inválido: {comando}")
        show_help()
        sys.exit(1)