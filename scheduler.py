#!/usr/bin/env python3
"""
Scheduler - Execução contínua ao longo do dia.
Fluxo recomendado:
1) Ao iniciar: valida tokens e roda extração de hoje.
2) Ao iniciar: roda carga completa de ontem (Utmify + VTurb).
3) Durante o dia: roda extração de hoje a cada hora.
"""

import schedule
import time
import subprocess
import os
import sys
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminhos dos scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTO_EXTRACT = os.path.join(SCRIPT_DIR, "auto_extract.py")
UTMIFY_EXTRACT = os.path.join(SCRIPT_DIR, "utmify_extract.py")
VTURB_EXTRACT = os.path.join(SCRIPT_DIR, "vturb_extract.py")

# Configuração de execução
PYTHON_BIN = os.getenv("PYTHON_BIN", "python3")
RUN_STARTUP_TODAY = os.getenv("SCHEDULER_RUN_STARTUP_TODAY", "true").lower() in ("1", "true", "yes", "on")
RUN_STARTUP_YESTERDAY = os.getenv("SCHEDULER_RUN_STARTUP_YESTERDAY", "true").lower() in ("1", "true", "yes", "on")
ACTIVE_START_HOUR = int(os.getenv("SCHEDULER_ACTIVE_START_HOUR", "8"))
ACTIVE_END_HOUR = int(os.getenv("SCHEDULER_ACTIVE_END_HOUR", "22"))
TEST_INCLUDE_YESTERDAY = os.getenv("SCHEDULER_TEST_INCLUDE_YESTERDAY", "false").lower() in ("1", "true", "yes", "on")


def extract_summary_blocks(output: str) -> list:
    """Extrai blocos de resumo da saída dos scripts."""
    if not output:
        return []

    lines = output.splitlines()
    blocks = []
    i = 0

    while i < len(lines):
        if "📈 RESUMO" in lines[i] or lines[i].strip() == "RESUMO":
            start = i
            end = None
            for j in range(i, len(lines)):
                if "✅ Extração concluída!" in lines[j]:
                    end = j
                    break

            if end is None:
                end = min(i + 14, len(lines) - 1)

            block = "\n".join(lines[start:end + 1]).strip()
            if block:
                blocks.append(block)

            i = end + 1
            continue

        i += 1

    return blocks


def classify_summary(block: str) -> str:
    text = block.lower()
    if "campanhas:" in text or "spend:" in text or "roas:" in text:
        return "UTMIFY"
    if "players:" in text or "views:" in text or "conversões:" in text:
        return "VTURB"
    return "GERAL"


def run_command(label: str, args: list, timeout: int = 1200) -> tuple:
    """Executa comando e retorna (ok, stdout, stderr, summaries)."""
    logger.info("=" * 60)
    logger.info(f"🚀 {label} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info(f"🧪 Comando: {PYTHON_BIN} {' '.join(args)}")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        proc = subprocess.Popen(
            [PYTHON_BIN] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout_lines = []
        stderr_lines = []
        deadline = time.time() + timeout

        while True:
            if proc.poll() is not None:
                break

            if time.time() > deadline:
                proc.kill()
                logger.error(f"❌ Timeout em {label}")
                return False, "", "", []

            out_line = proc.stdout.readline() if proc.stdout else ""
            if out_line:
                stdout_lines.append(out_line)
                print(out_line, end="")

            err_line = proc.stderr.readline() if proc.stderr else ""
            if err_line:
                stderr_lines.append(err_line)
                print(err_line, end="")

            time.sleep(0.05)

        remaining_out, remaining_err = proc.communicate(timeout=5)
        if remaining_out:
            stdout_lines.append(remaining_out)
            print(remaining_out, end="")
        if remaining_err:
            stderr_lines.append(remaining_err)
            print(remaining_err, end="")

        stdout = "".join(stdout_lines)
        stderr = "".join(stderr_lines)
        summaries = extract_summary_blocks(stdout)
        elapsed = datetime.now() - start_time

        if proc.returncode == 0:
            logger.info(f"✅ {label} concluído com sucesso em {str(elapsed).split('.')[0]}")
        else:
            logger.error(f"❌ {label} falhou (exit={proc.returncode}) em {str(elapsed).split('.')[0]}")

        if summaries:
            logger.info("📋 Resumo(s) coletado(s):")
            for block in summaries:
                kind = classify_summary(block)
                logger.info(f"[{kind}]")
                for line in block.splitlines():
                    logger.info(line)

        return proc.returncode == 0, stdout, stderr, summaries

    except subprocess.TimeoutExpired:
        logger.error(f"❌ Timeout em {label}")
        return False, "", "", []
    except Exception as e:
        logger.error(f"❌ Erro em {label}: {e}")
        return False, "", "", []


def log_cycle_summary(cycle_name: str, runs: list):
    total = len(runs)
    ok_count = sum(1 for r in runs if r["ok"])

    logger.info("=" * 60)
    logger.info(f"🧾 RESUMO DO CICLO: {cycle_name}")
    logger.info(f"Etapas com sucesso: {ok_count}/{total}")

    for r in runs:
        status = "✅" if r["ok"] else "❌"
        logger.info(f"{status} {r['label']}")
        if r["summaries"]:
            kinds = sorted({classify_summary(s) for s in r["summaries"]})
            logger.info(f"   Resumos: {', '.join(kinds)}")

    logger.info("=" * 60)


def run_today_cycle(reason: str = "Execução agendada") -> bool:
    """Roda ciclo de hoje (Utmify + VTurb) usando auto_extract."""
    label = f"{reason} | AUTO_EXTRACT HOJE"
    ok, _, _, summaries = run_command(label, [AUTO_EXTRACT, "hoje"], timeout=1800)
    log_cycle_summary(reason, [{"label": label, "ok": ok, "summaries": summaries}])
    return ok


def run_yesterday_backfill() -> bool:
    """Roda carga completa de ontem (Utmify + VTurb)."""
    runs = []

    label_u = "Carga de ontem | UTMIFY"
    ok_utmify, _, _, summaries_u = run_command(label_u, [UTMIFY_EXTRACT, "ontem"], timeout=1800)
    runs.append({"label": label_u, "ok": ok_utmify, "summaries": summaries_u})

    label_v = "Carga de ontem | VTURB"
    ok_vturb, _, _, summaries_v = run_command(label_v, [VTURB_EXTRACT, "ontem"], timeout=1800)
    runs.append({"label": label_v, "ok": ok_vturb, "summaries": summaries_v})

    log_cycle_summary("BACKFILL ONTEM", runs)
    return ok_utmify and ok_vturb


def within_active_window(now: datetime) -> bool:
    return ACTIVE_START_HOUR <= now.hour <= ACTIVE_END_HOUR


def hourly_job():
    now = datetime.now()
    if not within_active_window(now):
        logger.info(
            f"⏭️ Fora da janela ativa ({ACTIVE_START_HOUR:02d}:00-{ACTIVE_END_HOUR:02d}:59). Pulando execução."
        )
        return
    run_today_cycle(reason="Execução horária")


def main():
    """Configura e inicia o scheduler contínuo."""

    print("=" * 60)
    print("🤖 SCHEDULER - EXTRAÇÃO CONTÍNUA")
    print("=" * 60)
    print(f"Janela ativa: {ACTIVE_START_HOUR:02d}:00 até {ACTIVE_END_HOUR:02d}:59")
    print("Frequência: de hora em hora (minuto 00)")
    print(f"Startup hoje: {'SIM' if RUN_STARTUP_TODAY else 'NAO'}")
    print(f"Startup ontem: {'SIM' if RUN_STARTUP_YESTERDAY else 'NAO'}")
    print("Pressione Ctrl+C para parar")
    print("=" * 60)

    if RUN_STARTUP_TODAY:
        run_today_cycle(reason="Startup")
    if RUN_STARTUP_YESTERDAY:
        run_yesterday_backfill()

    # Agenda execução horária.
    schedule.every().hour.at(":00").do(hourly_job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(20)
    except KeyboardInterrupt:
        print("\n⏹️ Scheduler finalizado.")


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "run"

    if mode == "test":
        print("=" * 60)
        print("🧪 SCHEDULER TEST MODE")
        print("=" * 60)
        ok_today = run_today_cycle(reason="Teste único")
        ok_yesterday = True
        if TEST_INCLUDE_YESTERDAY:
            ok_yesterday = run_yesterday_backfill()
        sys.exit(0 if (ok_today and ok_yesterday) else 1)
    elif mode == "run":
        main()
    else:
        print("Uso: python3 scheduler.py [run|test]")
        sys.exit(1)
