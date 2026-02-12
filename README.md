# 🤖 Extração Automática - Utmify & VTurb

Sistema de extração automática de dados do Utmify e VTurb para PostgreSQL.

## 📁 Arquivos

| Arquivo | Função |
|---------|--------|
| `auto_extract.py` | 🤖 Automação com Playwright (login + extração) |
| `scheduler.py` | ⏰ Agendador (roda várias vezes ao dia) |
| `utmify_extract.py` | Extração Utmify (hoje/ontem) |
| `utmify_extract_data.py` | Extração Utmify (data específica) |
| `vturb_extract.py` | Extração VTurb (hoje/ontem) |
| `vturb_extract_data.py` | Extração VTurb (data específica) |

## 🚀 Instalação

### 1. Instalar dependências Python

```bash
pip install -r requirements.txt
```

### 2. Instalar navegador do Playwright

```bash
playwright install chromium
```

## 📖 Uso

### Extração Manual (com token)

```bash
# Utmify
python utmify_extract.py hoje
python utmify_extract.py ontem
python utmify_extract_data.py 14/01/2026

# VTurb  
python vturb_extract.py hoje
python vturb_extract.py ontem
python vturb_extract_data.py 14/01/2026
```

### Extração Automática (sem precisar de token)

```bash
# Extrai Utmify + VTurb (dados de hoje)
python auto_extract.py hoje

# Extrai apenas Utmify
python auto_extract.py utmify

# Extrai apenas VTurb
python auto_extract.py vturb
```

### Agendamento (roda o dia todo)

```bash
python scheduler.py
```

Horários programados: 10h, 14h, 18h, 22h

## 🔄 Fluxo Recomendado

### De manhã (manual)
```bash
python utmify_extract.py ontem
python vturb_extract.py ontem
```

### Durante o dia (automático)
```bash
python scheduler.py
# ou
python auto_extract.py hoje
```

## 🗄️ Banco de Dados

### Views para conectar no Looker Studio

| View | Dados |
|------|-------|
| `vw_campaigns` | Campanhas Utmify |
| `vw_ads` | Anúncios/Criativos Utmify |
| `vw_ads_with_campaign` | Anúncios com nome da campanha |
| `vw_vturb` | Players VTurb |

## ⚠️ Troubleshooting

### Erro de login

Se o login automático falhar, pode ser que a página mudou. Rode com `headless=False` para ver o navegador:

```python
# Em auto_extract.py, mude:
browser = playwright.chromium.launch(headless=False)
```

### Token expirado

Os tokens JWT expiram. O `auto_extract.py` resolve isso fazendo login automaticamente.

### Timeout

Se a extração demorar muito, aumente o timeout nos scripts.

## 🔐 Segurança

As credenciais estão no arquivo `auto_extract.py`. Para maior segurança, use variáveis de ambiente:

```bash
export UTMIFY_EMAIL="seu@email.com"
export UTMIFY_PASSWORD="sua_senha"
export VTURB_EMAIL="seu@email.com"
export VTURB_PASSWORD="sua_senha"
```
