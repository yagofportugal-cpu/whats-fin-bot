import os
import re
import uuid
import unicodedata
import datetime as dt
import requests
from collections import defaultdict
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()
app = FastAPI()

GRAPH_VER = "v22.0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ============================
# TIMEZONE (Brasil)
# ============================
TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Sao_Paulo"))

def now_local():
    return dt.datetime.now(TZ)

def now_iso():
    # timestamp em UTC p/ auditoria
    return now_local().astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def today_iso():
    return now_local().date().isoformat()

def yesterday_iso():
    return (now_local().date() - dt.timedelta(days=1)).isoformat()

# ----------------------------
# Estado (memória)
# ----------------------------
PENDING = {}  # {from: {"tx": {...}, "await": "...", "stage": "...", "ctx": {...}}}

# Dedup inbound (evita retry duplicar lançamentos)
SEEN_MSG = {}  # msg_id -> datetime_utc
SEEN_TTL_SECONDS = int(os.environ.get("SEEN_TTL_SECONDS", "3600"))  # 1h

MSG_SALVO = "Show, já registrei aqui no nosso BD, quando tiver mais alguma movimentação me sinalize aqui!"
TXT_INICIAL = "Olá, bora conferir saldos hoje ou você quer registrar algo?"

CANON_KEYS = [
    "id", "timestamp", "tipo", "valor", "moeda", "categoria", "descricao",
    "pagamento", "data", "confianca", "confirmado", "mensagem_original"
]

# ============================
# WhatsApp send
# ============================
def wa_url():
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    return f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"

def wa_headers():
    token = os.environ["WA_ACCESS_TOKEN"]
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def _post_wa(payload: dict):
    r = requests.post(wa_url(), headers=wa_headers(), json=payload, timeout=25)
    if r.status_code >= 400:
        print("WHATSAPP API ERROR:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def send_whatsapp_text(to: str, text: str):
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": (text or "")[:3800]},
    })

def send_whatsapp_buttons(to: str, body_text: str, buttons: list):
    # limite: 3 botões
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": (body_text or "")[:1024]},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": b["id"], "title": b["title"][:20]}}
                    for b in (buttons or [])[:3]
                ]
            },
        },
    })

def send_whatsapp_list(to: str, body_text: str, button_label: str, rows: list, section_title: str = "Opções"):
    # limite: 10 rows
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": (body_text or "")[:1024]},
            "action": {
                "button": (button_label or "Abrir")[:20],
                "sections": [{
                    "title": (section_title or "Opções")[:24],
                    "rows": [{
                        "id": (r["id"] or "")[:200],
                        "title": (r["title"] or "")[:24],
                        "description": (r.get("description") or "")[:72],
                    } for r in (rows or [])[:10]],
                }],
            },
        },
    })

# ============================
# Google Sheets
# ============================
def _sheets_service():
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def append_row(values: list):
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    rng = os.environ.get("GOOGLE_SHEETS_RANGE", "lancamentos!A1")
    svc = _sheets_service()
    body = {"values": [values]}
    return (
        svc.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )

# ============================
# MENUS DINÂMICOS (aba Menus)
# ============================
MENU_SHEET_NAME = os.environ.get("GOOGLE_SHEETS_MENU_SHEET", "Menus")
MENU_CACHE_TTL_SECONDS = int(os.environ.get("MENU_CACHE_TTL_SECONDS", "300"))  # 5 min
_MENU_CACHE = {"ts": None, "data": None}

def _read_column_values(range_a1: str) -> list[str]:
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    svc = _sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    values = res.get("values") or []
    out = []
    for row in values:
        if not row:
            continue
        v = str(row[0]).strip()
        if v:
            out.append(v)
    return out

def get_menus(force: bool = False) -> dict:
    now = dt.datetime.utcnow()
    ts = _MENU_CACHE.get("ts")
    if not force and ts and (now - ts).total_seconds() < MENU_CACHE_TTL_SECONDS and _MENU_CACHE.get("data"):
        return _MENU_CACHE["data"]

    rng_origem = f"{MENU_SHEET_NAME}!A2:A"
    rng_receb  = f"{MENU_SHEET_NAME}!B2:B"  # recebimento em B
    rng_catdes = f"{MENU_SHEET_NAME}!D2:D"
    rng_pagdes = f"{MENU_SHEET_NAME}!E2:E"

    origens = _read_column_values(rng_origem)
    receb   = _read_column_values(rng_receb)
    cats    = _read_column_values(rng_catdes)
    pays    = _read_column_values(rng_pagdes)

    data = {
        "origens_receita": origens,
        "recebimentos_receita": receb,
        "categorias_despesa": cats,
        "pagamentos_despesa": pays,
    }

    _MENU_CACHE["ts"] = now
    _MENU_CACHE["data"] = data
    return data

def _send_menu_in_chunks(to: str, body: str, button_label: str, items: list[str], id_prefix: str, section_title: str):
    if not items:
        send_whatsapp_text(to, "Não encontrei opções no menu. Preencha a aba *Menus* e tente novamente.")
        return

    chunk_size = 10
    total_pages = (len(items) + chunk_size - 1) // chunk_size

    for idx in range(0, len(items), chunk_size):
        chunk = items[idx: idx + chunk_size]
        rows = [{"id": f"{id_prefix}_{idx+i}", "title": v} for i, v in enumerate(chunk)]
        page = idx // chunk_size + 1
        suffix = "" if total_pages == 1 else f" ({page}/{total_pages})"
        send_whatsapp_list(to, body + suffix, button_label, rows, section_title=section_title)

# ============================
# Normalização de headers (aba lançamentos)
# ============================
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def normalize_header(h: str) -> str:
    if h is None:
        return ""
    s = str(h).strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"\(.*?\)", "", s).strip()
    s = re.sub(r"[\s\-\/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)

    mapping = {
        "descricao": "descricao",
        "data": "data",
        "timestamp": "timestamp",
        "tipo": "tipo",
        "valor": "valor",
        "moeda": "moeda",
        "categoria": "categoria",
        "pagamento": "pagamento",
        "confianca": "confianca",
        "confirmado": "confirmado",
        "mensagem_original": "mensagem_original",
        "id": "id",
    }

    if s in mapping:
        return mapping[s]
    if s.startswith("mensagem"):
        return "mensagem_original"
    if s.startswith("confirm"):
        return "confirmado"
    if s.startswith("confi"):
        return "confianca"
    if s.startswith("descr"):
        return "descricao"
    return s

def read_all_rows():
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    rng = os.environ.get("GOOGLE_SHEETS_READ_RANGE") or os.environ.get("GOOGLE_SHEETS_RANGE", "lancamentos!A1")

    svc = _sheets_service()
    res = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        )
        .execute()
    )

    values = res.get("values") or []
    if not values or len(values) < 2:
        return []

    raw_headers = values[0]
    headers = [normalize_header(h) for h in raw_headers]

    rows = []
    for line in values[1:]:
        row = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            row[h] = line[i] if i < len(line) else ""
        canon = {k: row.get(k, "") for k in CANON_KEYS}
        rows.append(canon)

    return rows

# ============================
# Helpers gerais
# ============================
def parse_valor(text: str):
    t = (text or "").lower()
    m = re.search(r"(-?\d{1,9}(?:[.,]\d{2})?)", t)
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def parse_data_text(text: str):
    t = (text or "").lower().strip()
    if t == "hoje":
        return today_iso()
    if t == "ontem":
        return yesterday_iso()

    m = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b", t)
    if not m:
        return None

    d = int(m.group(1))
    mo = int(m.group(2))
    y = m.group(3)
    if y is None:
        y = now_local().date().year
    else:
        y = int(y)
        if y < 100:
            y += 2000
    try:
        return dt.date(y, mo, d).isoformat()
    except:
        return None

def normalize_sign(tx: dict):
    v = tx.get("valor")
    if v is None or v == "":
        return
    try:
        vv = float(v)
    except:
        return
    if tx.get("tipo") == "despesa":
        tx["valor"] = -abs(vv)
    elif tx.get("tipo") == "receita":
        tx["valor"] = abs(vv)

def ensure_receita_descricao(tx: dict):
    if tx.get("tipo") != "receita":
        return
    if tx.get("descricao") and str(tx["descricao"]).strip():
        return
    origem = (tx.get("categoria") or "").strip()
    original = (tx.get("mensagem_original") or "").strip()
    if origem:
        tx["descricao"] = f"Receita - {origem}"
    elif original:
        tx["descricao"] = original[:180]
    else:
        tx["descricao"] = "Receita"

def tx_to_row(tx: dict):
    return [
        tx.get("id", ""),
        tx.get("timestamp", ""),
        tx.get("tipo", ""),
        tx.get("valor", ""),
        tx.get("moeda", "BRL"),
        tx.get("categoria", ""),
        tx.get("descricao", ""),
        tx.get("pagamento", ""),
        tx.get("data", ""),
        tx.get("confianca", ""),
        tx.get("confirmado", ""),
        tx.get("mensagem_original", ""),
    ]

def required_fields(tx: dict):
    base = ["tipo", "valor", "categoria", "pagamento", "data"]
    if tx.get("tipo") == "despesa":
        base.insert(3, "descricao")
    return base

def next_missing(tx: dict):
    for f in required_fields(tx):
        v = tx.get(f)
        if v is None:
            return f
        if isinstance(v, str) and not v.strip():
            return f
    return None

def fmt_money_br(x: float):
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

def iso_to_br(d: dt.date) -> str:
    return d.strftime("%d/%m/%Y")

# ============================
# Wizard UI
# ============================
def ask_inicio(to: str):
    send_whatsapp_buttons(
        to,
        TXT_INICIAL,
        [
            {"id": "inicio_receita", "title": "Receita"},
            {"id": "inicio_despesa", "title": "Despesa"},
            {"id": "inicio_mais", "title": "Mais"},
        ],
    )

def ask_inicio_mais(to: str):
    rows = [
        {"id": "inicio_resumo", "title": "Resumo", "description": "Diário / Semanal / Mensal / 3m / 6m / 12m"},
        {"id": "inicio_analise", "title": "Análise", "description": "Pergunta livre ou atalhos"},
    ]
    send_whatsapp_list(to, "Escolha uma opção:", "Abrir", rows, section_title="Mais")

def ask_categoria_ou_origem(to: str, tx: dict):
    menus = get_menus()

    if tx.get("tipo") == "receita":
        origens = menus.get("origens_receita") or []
        _send_menu_in_chunks(
            to=to,
            body="Qual a *ORIGEM* dessa receita?",
            button_label="Escolher",
            items=origens,
            id_prefix="origem",
            section_title="Origem",
        )
    else:
        cats = menus.get("categorias_despesa") or []
        _send_menu_in_chunks(
            to=to,
            body="Qual a *CATEGORIA* dessa despesa?",
            button_label="Escolher",
            items=cats,
            id_prefix="cat",
            section_title="Categoria",
        )

def ask_pagamento_despesa(to: str):
    menus = get_menus()
    pays = menus.get("pagamentos_despesa") or []
    _send_menu_in_chunks(
        to=to,
        body="Como foi o pagamento?",
        button_label="Escolher",
        items=pays,
        id_prefix="pay",
        section_title="Pagamento",
    )

def ask_recebimento_receita(to: str):
    menus = get_menus()
    recs = menus.get("recebimentos_receita") or []
    _send_menu_in_chunks(
        to=to,
        body="Como foi o recebimento?",
        button_label="Escolher",
        items=recs,
        id_prefix="rec",
        section_title="Recebimento",
    )

def ask_data(to: str):
    send_whatsapp_buttons(
        to,
        "Qual a data de competência?",
        [
            {"id": "data_hoje", "title": "Hoje"},
            {"id": "data_ontem", "title": "Ontem"},
            {"id": "data_outra", "title": "Outra"},
        ],
    )

def format_confirm(tx: dict):
    v_abs = abs(float(tx["valor"])) if tx.get("valor") is not None and tx.get("valor") != "" else 0.0
    sinal = "+" if tx.get("tipo") == "receita" else "-"
    label_cat = "origem" if tx.get("tipo") == "receita" else "categoria"
    label_pay = "recebimento" if tx.get("tipo") == "receita" else "pagamento"
    return (
        "Confirma o lançamento?\n"
        f"- tipo: {tx.get('tipo')}\n"
        f"- valor: {sinal}R$ {fmt_money_br(v_abs)}\n"
        f"- {label_cat}: {tx.get('categoria')}\n"
        f"- {label_pay}: {tx.get('pagamento')}\n"
        f"- data: {tx.get('data')}\n"
    )

def ask_confirm(to: str, tx: dict):
    msg = format_confirm(tx) + "\nSelecione:"
    send_whatsapp_buttons(
        to,
        msg,
        [
            {"id": "confirm_sim", "title": "SIM"},
            {"id": "confirm_cancelar", "title": "CANCELAR"},
        ],
    )

def ask_resumo_periodo(to: str):
    send_whatsapp_buttons(
        to,
        "Qual resumo você quer ver?",
        [
            {"id": "res_diario", "title": "Diário"},
            {"id": "res_semanal", "title": "Semanal"},
            {"id": "res_mensal", "title": "Mensal"},
        ],
    )
    rows = [
        {"id": "res_3m", "title": "3 meses", "description": "Últimos 3 meses"},
        {"id": "res_6m", "title": "6 meses", "description": "Últimos 6 meses"},
        {"id": "res_12m", "title": "12 meses", "description": "Últimos 12 meses"},
    ]
    send_whatsapp_list(to, "Ou escolha em *Outros*:", "Abrir", rows, section_title="Outros")

def ask_analise_inicio(to: str):
    send_whatsapp_buttons(
        to,
        "Como você quer fazer a análise?",
        [
            {"id": "ana_livre", "title": "Perguntar"},
            {"id": "ana_atalhos", "title": "Atalhos"},
            {"id": "ana_cancelar", "title": "Cancelar"},
        ],
    )

def ask_analise_atalhos(to: str):
    rows = [
        {"id": "ana_top_despesas_mes", "title": "Top despesas (mês)", "description": "Maiores categorias do mês"},
        {"id": "ana_top_receitas_mes", "title": "Top receitas (mês)", "description": "Maiores origens do mês"},
        {"id": "ana_compare_mes", "title": "Comparar mês vs anterior", "description": "Receitas/Despesas/Saldos"},
        {"id": "ana_alertas", "title": "Alertas de aumento", "description": "Categorias que subiram"},
        {"id": "ana_cortes_10", "title": "Cortar 10% de gastos", "description": "Onde dá para reduzir"},
        {"id": "ana_forma_pagto", "title": "Gastos por pagamento", "description": "PIX/Débito/Crédito/etc"},
    ]
    send_whatsapp_list(to, "Escolha um atalho:", "Abrir", rows, section_title="Atalhos")

def ask_text_field(to: str, field: str, tx: dict):
    if field == "valor":
        send_whatsapp_text(to, "Qual o *VALOR*? Ex: 35,90")
    elif field == "descricao":
        send_whatsapp_text(to, "Qual a *DESCRIÇÃO* (curta)? Ex: pão e leite")
    elif field == "data":
        send_whatsapp_text(to, "Digite a data (dd/mm) ou 'hoje' / 'ontem'.")
    elif field == "categoria":
        if tx.get("tipo") == "receita":
            send_whatsapp_text(to, "Digite a *ORIGEM* (texto). Ex: Salário, PLR, etc.")
        else:
            send_whatsapp_text(to, "Digite a *CATEGORIA* (texto). Ex: Pet, Viagem, etc.")
    elif field == "pagamento":
        if tx.get("tipo") == "receita":
            send_whatsapp_text(to, "Digite a forma de *RECEBIMENTO* (texto). Ex: PIX, Dinheiro.")
        else:
            send_whatsapp_text(to, "Digite a forma de *PAGAMENTO* (texto). Ex: PIX, Débito.")
    else:
        send_whatsapp_text(to, "Preciso de uma informação (texto).")

def continue_wizard(to: str, tx: dict):
    nxt = next_missing(tx)
    if nxt is None:
        ensure_receita_descricao(tx)
        normalize_sign(tx)
        ask_confirm(to, tx)
        return "confirm"

    if nxt == "categoria":
        ask_categoria_ou_origem(to, tx)
        return "categoria"

    if nxt == "pagamento":
        if tx.get("tipo") == "receita":
            ask_recebimento_receita(to)
            return "recebimento"
        ask_pagamento_despesa(to)
        return "pagamento"

    if nxt == "data":
        ask_data(to)
        return "data"

    ask_text_field(to, nxt, tx)
    return nxt

# ============================
# Resumo
# ============================
def _to_float(v):
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def _parse_date_any(v):
    if v is None or v == "":
        return None

    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v

    if isinstance(v, (int, float)):
        try:
            base = dt.date(1899, 12, 30)
            return base + dt.timedelta(days=float(v))
        except:
            return None

    s = str(v).strip()

    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            base = dt.date(1899, 12, 30)
            return base + dt.timedelta(days=float(s))
        except:
            pass

    try:
        return dt.date.fromisoformat(s[:10])
    except:
        pass

    m = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        yy = m.group(3)
        if yy is None:
            y = now_local().date().year
        else:
            y = int(yy)
            if y < 100:
                y += 2000
        try:
            return dt.date(y, mo, d)
        except:
            return None

    return None

def get_period_range(kind: str):
    today = now_local().date()
    if kind == "diario":
        start = today
    elif kind == "semanal":
        start = today - dt.timedelta(days=6)
    elif kind == "mensal":
        start = today.replace(day=1)
    elif kind == "3m":
        start = today - dt.timedelta(days=89)
    elif kind == "6m":
        start = today - dt.timedelta(days=179)
    elif kind == "12m":
        start = today - dt.timedelta(days=364)
    else:
        start = today
    return start, today

def build_resumo_text(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    start, end = get_period_range(kind)

    total_rec = 0.0
    total_des = 0.0
    rec_by_cat = defaultdict(float)
    des_by_cat = defaultdict(float)

    for r in rows:
        d = _parse_date_any(r.get("data"))
        if not d:
            continue
        if d < start or d > end:
            continue

        tipo = (r.get("tipo") or "").strip().lower()
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        val = _to_float(r.get("valor"))

        if tipo == "receita":
            total_rec += abs(val)
            rec_by_cat[cat] += abs(val)
        elif tipo == "despesa":
            total_des += abs(val)
            des_by_cat[cat] += abs(val)

    label = {
        "diario": "Resumo Diário",
        "semanal": "Resumo Semanal",
        "mensal": "Resumo Mensal",
        "3m": "Resumo 3 meses",
        "6m": "Resumo 6 meses",
        "12m": "Resumo 12 meses",
    }.get(kind, "Resumo")

    rec_top = sorted(rec_by_cat.items(), key=lambda x: x[1], reverse=True)[:12]
    des_top = sorted(des_by_cat.items(), key=lambda x: x[1], reverse=True)[:12]

    perc = 0.0
    if total_rec > 0:
        perc = (total_des / total_rec) * 100.0

    lines = []
    lines.append(f"*{label}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append("*Receitas por origem*")
    if rec_top:
        for c, v in rec_top:
            lines.append(f"- {c}: R$ {fmt_money_br(v)}")
    else:
        lines.append("- (sem receitas no período)")
    lines.append("")
    lines.append("*Despesas por categoria*")
    if des_top:
        for c, v in des_top:
            lines.append(f"- {c}: R$ {fmt_money_br(v)}")
    else:
        lines.append("- (sem despesas no período)")
    lines.append("")
    lines.append(f"Receitas: +R$ {fmt_money_br(total_rec)}")
    lines.append(f"Despesas: -R$ {fmt_money_br(total_des)}")
    lines.append(f"Saldo:   R$ {fmt_money_br(total_rec - total_des)}")
    lines.append("")
    lines.append(f"Neste período suas despesas equivaleram a *{perc:.1f}%* sobre suas receitas.")

    return "\n".join(lines)

# ============================
# Análise (IA + atalhos determinísticos)
# ============================
ANALISE_MAX_ROWS = int(os.environ.get("ANALISE_MAX_ROWS", "400"))  # controla custo/latência

def _rows_in_period(rows, start: dt.date, end: dt.date):
    out = []
    for r in rows:
        d = _parse_date_any(r.get("data"))
        if not d:
            continue
        if start <= d <= end:
            out.append(r)
    return out

def _month_range(ref: dt.date):
    start = ref.replace(day=1)
    end = ref
    return start, end

def _prev_month_range(ref: dt.date):
    first = ref.replace(day=1)
    prev_end = first - dt.timedelta(days=1)
    prev_start = prev_end.replace(day=1)
    return prev_start, prev_end

def _sum_by(rows, field: str, tipo: str | None = None):
    acc = defaultdict(float)
    total = 0.0
    for r in rows:
        t = (r.get("tipo") or "").strip().lower()
        if tipo and t != tipo:
            continue
        k = (r.get(field) or "Sem").strip() or "Sem"
        v = abs(_to_float(r.get("valor")))
        if t not in ["receita", "despesa"]:
            continue
        total += v
        acc[k] += v
    top = sorted(acc.items(), key=lambda x: x[1], reverse=True)
    return total, top

def analise_atalho(code: str) -> str:
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    today = now_local().date()
    m_start, m_end = _month_range(today)
    pm_start, pm_end = _prev_month_range(today)

    rows_m = _rows_in_period(rows, m_start, m_end)
    rows_pm = _rows_in_period(rows, pm_start, pm_end)

    def fmt_list(title: str, pairs, limit=8):
        lines = [f"*{title}*"]
        if not pairs:
            lines.append("- (sem dados)")
            return "\n".join(lines)
        for k, v in pairs[:limit]:
            lines.append(f"- {k}: R$ {fmt_money_br(v)}")
        return "\n".join(lines)

    if code == "ana_top_despesas_mes":
        total, top = _sum_by(rows_m, "categoria", tipo="despesa")
        return "\n".join([
            "*Top despesas (mês)*",
            f"Período: {iso_to_br(m_start)} a {iso_to_br(m_end)}",
            "",
            fmt_list("Despesas por categoria", top, 10),
            "",
            f"Total despesas: R$ {fmt_money_br(total)}",
        ])

    if code == "ana_top_receitas_mes":
        total, top = _sum_by(rows_m, "categoria", tipo="receita")
        return "\n".join([
            "*Top receitas (mês)*",
            f"Período: {iso_to_br(m_start)} a {iso_to_br(m_end)}",
            "",
            fmt_list("Receitas por origem", top, 10),
            "",
            f"Total receitas: R$ {fmt_money_br(total)}",
        ])

    if code == "ana_forma_pagto":
        total, top = _sum_by(rows_m, "pagamento", tipo="despesa")
        return "\n".join([
            "*Gastos por forma de pagamento (mês)*",
            f"Período: {iso_to_br(m_start)} a {iso_to_br(m_end)}",
            "",
            fmt_list("Despesas por pagamento", top, 10),
            "",
            f"Total despesas: R$ {fmt_money_br(total)}",
        ])

    if code == "ana_compare_mes":
        rec_m, _ = _sum_by(rows_m, "categoria", tipo="receita")
        des_m, _ = _sum_by(rows_m, "categoria", tipo="despesa")
        rec_pm, _ = _sum_by(rows_pm, "categoria", tipo="receita")
        des_pm, _ = _sum_by(rows_pm, "categoria", tipo="despesa")

        def delta(a, b):
            if b == 0:
                return "N/A"
            return f"{((a-b)/b)*100:.1f}%".replace(".", ",")

        return "\n".join([
            "*Comparação mês vs anterior*",
            f"Mês atual: {iso_to_br(m_start)} a {iso_to_br(m_end)}",
            f"Mês anterior: {iso_to_br(pm_start)} a {iso_to_br(pm_end)}",
            "",
            f"Receitas: R$ {fmt_money_br(rec_m)} (var: {delta(rec_m, rec_pm)})",
            f"Despesas: R$ {fmt_money_br(des_m)} (var: {delta(des_m, des_pm)})",
            f"Saldo:    R$ {fmt_money_br(rec_m - des_m)} (antes: R$ {fmt_money_br(rec_pm - des_pm)})",
        ])

    if code == "ana_alertas":
        # alerta simples: categorias de despesa que mais cresceram (mês vs mês anterior)
        def cat_map(rows_):
            m = defaultdict(float)
            for r in rows_:
                if (r.get("tipo") or "").strip().lower() != "despesa":
                    continue
                cat = (r.get("categoria") or "Sem").strip() or "Sem"
                m[cat] += abs(_to_float(r.get("valor")))
            return m

        m1 = cat_map(rows_m)
        m0 = cat_map(rows_pm)
        growth = []
        for cat, v1 in m1.items():
            v0 = m0.get(cat, 0.0)
            if v0 <= 0:
                continue
            growth.append((cat, v1 - v0, v1, v0))
        growth.sort(key=lambda x: x[1], reverse=True)

        lines = ["*Alertas de aumento (mês vs anterior)*"]
        lines.append(f"Mês atual: {iso_to_br(m_start)} a {iso_to_br(m_end)}")
        lines.append(f"Mês anterior: {iso_to_br(pm_start)} a {iso_to_br(pm_end)}")
        lines.append("")
        if not growth:
            lines.append("Nenhuma categoria com aumento claro (ou mês anterior sem base).")
            return "\n".join(lines)

        for cat, dv, v1, v0 in growth[:8]:
            pct = (dv / v0) * 100.0 if v0 > 0 else 0.0
            lines.append(f"- {cat}: +R$ {fmt_money_br(dv)} ({pct:.1f}% )".replace(".", ","))
        return "\n".join(lines)

    if code == "ana_cortes_10":
        # sugestão simples: mostra top despesas e simula corte 10% nelas
        total, top = _sum_by(rows_m, "categoria", tipo="despesa")
        if total <= 0:
            return "Sem despesas no período para sugerir cortes."
        alvo = total * 0.10
        lines = [
            "*Cortar 10% de gastos (mês)*",
            f"Período: {iso_to_br(m_start)} a {iso_to_br(m_end)}",
            f"Meta de redução: R$ {fmt_money_br(alvo)}",
            "",
            "Priorize cortar nas maiores categorias:",
        ]
        for cat, v in top[:8]:
            lines.append(f"- {cat}: R$ {fmt_money_br(v)} (10% = R$ {fmt_money_br(v*0.10)})")
        return "\n".join(lines)

    return "Atalho não reconhecido."

def _available_fields_summary(rows: list[dict]) -> str:
    # ajuda o guardrail
    return "Campos disponíveis: tipo, valor, categoria, descricao, pagamento, data."

def _guardrail_question(question: str) -> str | None:
    q = (question or "").lower()
    # sinais claros de pedido impossível
    forbidden = ["cartão", "banco", "conta", "agência", "cpf", "cnpj", "loja", "merchant", "estabelecimento", "parcela", "parcelado"]
    if any(w in q for w in forbidden):
        return (
            "Eu não tenho esses dados na planilha (ex: banco/cartão/loja). "
            "Se você quiser esse tipo de análise, precisa criar uma coluna específica e começar a registrar isso.\n"
            + _available_fields_summary([])
        )
    return None

def call_openai_for_analysis(question: str, rows: list[dict]) -> str:
    """
    Chama OpenAI Responses API via HTTPS.
    Necessário: OPENAI_API_KEY
    Opcional: OPENAI_MODEL (default gpt-4.1-mini)
    """
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return "IA não está configurada (OPENAI_API_KEY ausente). Use os atalhos por enquanto."

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    # reduz payload
    slim = []
    for r in rows[:ANALISE_MAX_ROWS]:
        slim.append({
            "tipo": r.get("tipo"),
            "valor": r.get("valor"),
            "categoria": r.get("categoria"),
            "descricao": (r.get("descricao") or "")[:120],
            "pagamento": r.get("pagamento"),
            "data": r.get("data"),
        })

    sys = (
        "Você é um analista financeiro pessoal. "
        "Responda apenas usando os dados fornecidos. "
        "Se a pergunta pedir algo fora dos dados, diga explicitamente que não dá e sugira qual coluna faltaria. "
        "Se fizer cálculos, mostre números e período. "
        "Se houver ambiguidade, faça 1 pergunta objetiva."
    )

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": sys},
            {"role": "user", "content": f"Pergunta: {question}\n\nDados (amostra): {slim}"},
        ],
    }

    r = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=35,
    )
    if r.status_code >= 400:
        return f"Falha ao chamar IA: {r.status_code} {r.text[:300]}"

    data = r.json()
    # tenta extrair texto
    out = ""
    try:
        for item in data.get("output", []):
            if item.get("type") == "message":
                for c in item.get("content", []):
                    if c.get("type") == "output_text":
                        out += c.get("text", "")
    except:
        pass
    return out.strip() or "A IA não retornou texto."

def analise_livre(question: str) -> str:
    g = _guardrail_question(question)
    if g:
        return g

    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    # recorte padrão: últimos 12 meses
    start, end = get_period_range("12m")
    recorte = _rows_in_period(rows, start, end)

    if not recorte:
        return "Não encontrei lançamentos no período padrão (últimos 12 meses)."

    # chama IA
    return call_openai_for_analysis(question, recorte)

# ============================
# Inbound parse + dedup cleanup
# ============================
def extract_inbound(msg: dict):
    inter = msg.get("interactive") or {}
    if msg.get("type") == "interactive" or inter:
        itype = inter.get("type")
        if itype == "button_reply":
            rep = inter.get("button_reply") or {}
            return ("choice", rep.get("id"), rep.get("title"))
        if itype == "list_reply":
            rep = inter.get("list_reply") or {}
            return ("choice", rep.get("id"), rep.get("title"))
        return ("text", "", "")

    text = (msg.get("text") or {}).get("body", "")
    return ("text", (text or "").strip(), "")

def cleanup_seen():
    now = dt.datetime.utcnow()
    for k, t in list(SEEN_MSG.items()):
        if (now - t).total_seconds() > SEEN_TTL_SECONDS:
            SEEN_MSG.pop(k, None)

# ============================
# Webhook
# ============================
@app.get("/")
def home():
    return {"status": "ok"}

@app.head("/")
def head_home():
    return Response(status_code=200)

@app.get("/webhook")
def verify(request: Request):
    qp = dict(request.query_params)
    verify_token = qp.get("hub.verify_token")
    challenge = qp.get("hub.challenge", "")
    if verify_token == os.environ.get("WA_VERIFY_TOKEN"):
        return Response(content=challenge, media_type="text/plain")
    return Response(status_code=403)

@app.post("/webhook")
async def receive(req: Request):
    body = await req.json()

    entry = (body.get("entry") or [{}])[0]
    changes = (entry.get("changes") or [{}])[0]
    value = changes.get("value") or {}

    # ignora eventos de status (entrega, lido, etc)
    if value.get("statuses"):
        return {"ok": True}

    messages = value.get("messages") or []
    if not messages:
        return {"ok": True}

    for msg in messages:
        from_number = msg.get("from")
        if not from_number:
            continue

        allowed = os.environ.get("ALLOWED_WA_NUMBER", "").strip()
        if allowed and from_number != allowed:
            continue

        cleanup_seen()
        msg_id = msg.get("id")
        if msg_id:
            if msg_id in SEEN_MSG:
                continue
            SEEN_MSG[msg_id] = dt.datetime.utcnow()

        msg_type = msg.get("type")
        if msg_type not in ["text", "interactive"]:
            continue

        kind, val, title = extract_inbound(msg)

        # cancelar (global)
        if kind == "text" and val.lower().strip() in ["cancelar", "cancela"]:
            PENDING.pop(from_number, None)
            send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
            continue

        pending = PENDING.get(from_number)

        # inicia menu
        if not pending:
            PENDING[from_number] = {"tx": None, "await": "inicio", "stage": "menu", "ctx": {}}
            ask_inicio(from_number)
            continue

        await_field = pending.get("await")

        # ======================
        # MENU INICIAL
        # ======================
        if await_field == "inicio":
            if kind != "choice":
                ask_inicio(from_number)
                continue

            if val == "inicio_receita":
                tx = {
                    "id": str(uuid.uuid4()),
                    "timestamp": now_iso(),
                    "tipo": "receita",
                    "valor": None,
                    "moeda": "BRL",
                    "categoria": None,  # origem
                    "descricao": None,  # auto
                    "pagamento": None,  # recebimento
                    "data": None,
                    "confianca": 0.60,
                    "confirmado": "não",
                    "mensagem_original": "",
                }
                pending["tx"] = tx
                pending["ctx"] = {}
                pending["await"] = continue_wizard(from_number, tx)
                continue

            if val == "inicio_despesa":
                tx = {
                    "id": str(uuid.uuid4()),
                    "timestamp": now_iso(),
                    "tipo": "despesa",
                    "valor": None,
                    "moeda": "BRL",
                    "categoria": None,
                    "descricao": None,
                    "pagamento": None,
                    "data": None,
                    "confianca": 0.60,
                    "confirmado": "não",
                    "mensagem_original": "",
                }
                pending["tx"] = tx
                pending["ctx"] = {}
                pending["await"] = continue_wizard(from_number, tx)
                continue

            if val == "inicio_mais":
                pending["tx"] = None
                pending["ctx"] = {}
                pending["await"] = "inicio_mais"
                ask_inicio_mais(from_number)
                continue

            ask_inicio(from_number)
            continue

        # ======================
        # MENU "MAIS"
        # ======================
        if await_field == "inicio_mais":
            if kind != "choice":
                ask_inicio_mais(from_number)
                continue

            if val == "inicio_resumo":
                pending["tx"] = None
                pending["ctx"] = {}
                pending["await"] = "resumo_periodo"
                ask_resumo_periodo(from_number)
                continue

            if val == "inicio_analise":
                pending["tx"] = None
                pending["ctx"] = {}
                pending["await"] = "analise_inicio"
                ask_analise_inicio(from_number)
                continue

            ask_inicio_mais(from_number)
            continue

        # ======================
        # RESUMO
        # ======================
        if await_field == "resumo_periodo":
            if kind != "choice":
                ask_resumo_periodo(from_number)
                continue

            mapping = {
                "res_diario": "diario",
                "res_semanal": "semanal",
                "res_mensal": "mensal",
                "res_3m": "3m",
                "res_6m": "6m",
                "res_12m": "12m",
            }
            if val in mapping:
                send_whatsapp_text(from_number, build_resumo_text(mapping[val]))
                PENDING.pop(from_number, None)
                continue

            ask_resumo_periodo(from_number)
            continue

        # ======================
        # ANÁLISE: menu inicial
        # ======================
        if await_field == "analise_inicio":
            if kind != "choice":
                ask_analise_inicio(from_number)
                continue

            if val == "ana_cancelar":
                PENDING.pop(from_number, None)
                send_whatsapp_text(from_number, "Ok. Mande qualquer mensagem para começar de novo.")
                continue

            if val == "ana_livre":
                pending["await"] = "analise_livre_pergunta"
                send_whatsapp_text(from_number, "Manda sua pergunta de análise em texto livre.")
                continue

            if val == "ana_atalhos":
                pending["await"] = "analise_atalhos"
                ask_analise_atalhos(from_number)
                continue

            ask_analise_inicio(from_number)
            continue

        # ======================
        # ANÁLISE: atalhos
        # ======================
        if await_field == "analise_atalhos":
            if kind != "choice":
                ask_analise_atalhos(from_number)
                continue

            txt = analise_atalho(val)
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            continue

        # ======================
        # ANÁLISE: pergunta livre
        # ======================
        if await_field == "analise_livre_pergunta":
            if kind != "text" or not val.strip():
                send_whatsapp_text(from_number, "Me manda a pergunta em texto (ex: 'Qual categoria mais cresceu este mês?').")
                continue

            txt = analise_livre(val.strip())
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            continue

        # daqui pra frente: fluxo lançamento
        tx = pending.get("tx") or {}

        # ======================
        # CONFIRM
        # ======================
        if await_field == "confirm":
            if (kind == "choice" and val == "confirm_sim") or (kind == "text" and val.lower().strip() in ["sim", "ok", "confirmar"]):
                tx["confirmado"] = "sim"
                ensure_receita_descricao(tx)
                normalize_sign(tx)
                append_row(tx_to_row(tx))
                PENDING.pop(from_number, None)
                send_whatsapp_text(from_number, MSG_SALVO)
                continue

            if (kind == "choice" and val == "confirm_cancelar") or (kind == "text" and val.lower().strip() in ["nao", "não", "cancelar", "cancela"]):
                PENDING.pop(from_number, None)
                send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
                continue

            send_whatsapp_text(from_number, "Selecione SIM para gravar ou CANCELAR para descartar.")
            continue

        # ======================
        # CATEGORIA / ORIGEM
        # ======================
        if await_field == "categoria":
            if kind == "choice" and val:
                if tx.get("tipo") == "receita" and val.startswith("origem_"):
                    tx["categoria"] = title or ""
                elif tx.get("tipo") == "despesa" and val.startswith("cat_"):
                    tx["categoria"] = title or ""

                if not (tx.get("categoria") or "").strip():
                    pending["tx"] = tx
                    pending["await"] = "categoria_texto"
                    ask_text_field(from_number, "categoria", tx)
                    continue

                pending["tx"] = tx
                pending["await"] = continue_wizard(from_number, tx)
                continue

            send_whatsapp_text(from_number, "Escolha uma opção na lista.")
            ask_categoria_ou_origem(from_number, tx)
            continue

        if await_field == "categoria_texto":
            if kind != "text" or not val.strip():
                ask_text_field(from_number, "categoria", tx)
                continue
            tx["categoria"] = val.strip()
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            continue

        # ======================
        # VALOR
        # ======================
        if await_field == "valor":
            if kind != "text":
                ask_text_field(from_number, "valor", tx)
                continue
            v = parse_valor(val)
            if v is None:
                send_whatsapp_text(from_number, "Valor inválido. Ex: 35,90")
                ask_text_field(from_number, "valor", tx)
                continue
            tx["valor"] = v
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            continue

        # ======================
        # DESCRIÇÃO (despesa)
        # ======================
        if await_field == "descricao":
            if kind != "text" or not val.strip():
                ask_text_field(from_number, "descricao", tx)
                continue
            tx["descricao"] = val.strip()
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            continue

        # ======================
        # PAGAMENTO (despesa)
        # ======================
        if await_field == "pagamento":
            if kind == "choice" and val and val.startswith("pay_"):
                tx["pagamento"] = (title or "").strip().lower() or "desconhecido"
                pending["tx"] = tx
                pending["await"] = continue_wizard(from_number, tx)
                continue
            send_whatsapp_text(from_number, "Escolha uma opção na lista de pagamento.")
            ask_pagamento_despesa(from_number)
            continue

        # ======================
        # RECEBIMENTO (receita)
        # ======================
        if await_field == "recebimento":
            if kind == "choice" and val and val.startswith("rec_"):
                tx["pagamento"] = (title or "").strip().lower() or "pix"
                pending["tx"] = tx
                pending["await"] = continue_wizard(from_number, tx)
                continue
            send_whatsapp_text(from_number, "Escolha uma opção na lista de recebimento.")
            ask_recebimento_receita(from_number)
            continue

        # ======================
        # DATA
        # ======================
        if await_field == "data":
            if kind == "choice" and val in ["data_hoje", "data_ontem", "data_outra"]:
                if val == "data_hoje":
                    tx["data"] = today_iso()
                    pending["tx"] = tx
                    pending["await"] = continue_wizard(from_number, tx)
                    continue
                if val == "data_ontem":
                    tx["data"] = yesterday_iso()
                    pending["tx"] = tx
                    pending["await"] = continue_wizard(from_number, tx)
                    continue
                pending["tx"] = tx
                pending["await"] = "data_texto"
                ask_text_field(from_number, "data", tx)
                continue

            send_whatsapp_text(from_number, "Use os botões: Hoje / Ontem / Outra.")
            ask_data(from_number)
            continue

        if await_field == "data_texto":
            if kind != "text" or not val.strip():
                ask_text_field(from_number, "data", tx)
                continue
            d = parse_data_text(val.strip())
            if not d:
                send_whatsapp_text(from_number, "Data inválida. Use hoje/ontem ou dd/mm (ex: 29/12).")
                ask_text_field(from_number, "data", tx)
                continue
            tx["data"] = d
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            continue

        # fallback
        pending["tx"] = tx
        pending["await"] = continue_wizard(from_number, tx)

    return {"ok": True}
