#!/usr/bin/env python3
"""
Scheduler - Agenda execuções automáticas ao longo do dia
Roda o auto_extract.py em horários específicos
"""

import schedule
import time
import subprocess
import os
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminho do script de automação
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTO_EXTRACT = os.path.join(SCRIPT_DIR, "auto_extract.py")


def run_extraction():
    """Executa a extração automática"""
    
    logger.info("=" * 60)
    logger.info(f"🕐 EXECUÇÃO AGENDADA - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        result = subprocess.run(
            ["python", AUTO_EXTRACT, "hoje"],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos de timeout
        )
        
        print(result.stdout)
        
        if result.returncode == 0:
            logger.info("✅ Extração concluída com sucesso!")
        else:
            logger.error(f"❌ Erro na extração: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Timeout na extração (10 min)")
    except Exception as e:
        logger.error(f"❌ Erro: {e}")


def main():
    """Configura e inicia o scheduler"""
    
    print("=" * 60)
    print("🤖 SCHEDULER - EXTRAÇÃO AUTOMÁTICA")
    print("=" * 60)
    print("")
    print("Horários programados:")
    print("  • 10:00 - Extração manhã")
    print("  • 14:00 - Extração tarde")
    print("  • 18:00 - Extração fim do dia")
    print("  • 22:00 - Extração noite")
    print("")
    print("Pressione Ctrl+C para parar")
    print("=" * 60)
    
    # Agenda os horários
    schedule.every().day.at("10:00").do(run_extraction)
    schedule.every().day.at("14:00").do(run_extraction)
    schedule.every().day.at("18:00").do(run_extraction)
    schedule.every().day.at("22:00").do(run_extraction)
    
    # Executa uma vez ao iniciar (opcional - comente se não quiser)
    # run_extraction()
    
    # Loop principal
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verifica a cada 1 minuto


if __name__ == "__main__":
    main()
