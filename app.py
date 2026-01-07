import os
import re
import uuid
import unicodedata
import datetime as dt
from zoneinfo import ZoneInfo
from collections import defaultdict

import requests
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ============================================================
# BOOT
# ============================================================
load_dotenv()
app = FastAPI()

GRAPH_VER = "v22.0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ============================================================
# TIMEZONE (Brasil)
# ============================================================
TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Sao_Paulo"))

def now_local():
    return dt.datetime.now(TZ)

def now_iso_utc():
    return now_local().astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def today_iso_local():
    return now_local().date().isoformat()

def yesterday_iso_local():
    return (now_local().date() - dt.timedelta(days=1)).isoformat()

# ============================================================
# STATE
# ============================================================
PENDING = {}      # {from: {"tx": {...} or None, "await": "...", "stage": "...", "ctx": {...}}}
SEEN_MSG = {}     # {msg_id: datetime_utc}
SEEN_TTL_SECONDS = int(os.environ.get("SEEN_TTL_SECONDS", "3600"))  # 1h

# paging para listas (1 lista por vez; "Mais opções" pagina)
MENU_PAGING = {}  # {from: {"items":[...], "offset": int, "id_prefix": str, "field": str, ...}}

# ============================================================
# TEXTS
# ============================================================
MSG_SALVO = "Show, já registrei aqui no nosso BD, quando tiver mais alguma movimentação me sinalize aqui!"
TXT_CATALOGO = "Escolha um módulo:"
TXT_FIN_HOME = "Financeiro: quer registrar algo ou ver análises?"

# ============================================================
# WHATSAPP SEND
# ============================================================
def wa_url():
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    return f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"

def wa_headers():
    token = os.environ["WA_ACCESS_TOKEN"]
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

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
                        "id": (r.get("id") or "")[:200],
                        "title": (r.get("title") or "")[:24],
                        "description": (r.get("description") or "")[:72],
                    } for r in (rows or [])[:10]],
                }],
            },
        },
    })

# ============================================================
# GOOGLE SHEETS
# ============================================================
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

# ============================================================
# MENUS (aba Menus) + CACHE
# ============================================================
MENU_SHEET_NAME = os.environ.get("GOOGLE_SHEETS_MENU_SHEET", "Menus")
MENU_CACHE_TTL_SECONDS = int(os.environ.get("MENU_CACHE_TTL_SECONDS", "300"))  # 5 min
_MENU_CACHE = {"ts": None, "data": None}

def _read_range_values(range_a1: str):
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    svc = _sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    return res.get("values") or []

def _read_column_values(range_a1: str) -> list:
    values = _read_range_values(range_a1)
    out = []
    for row in values:
        if not row:
            continue
        v = row[0]
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    return out

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

def get_menus(force: bool = False) -> dict:
    now = dt.datetime.utcnow()
    ts = _MENU_CACHE.get("ts")
    if not force and ts and (now - ts).total_seconds() < MENU_CACHE_TTL_SECONDS and _MENU_CACHE.get("data"):
        return _MENU_CACHE["data"]

    # colunas conforme seu mapeamento
    origens = _read_column_values(f"{MENU_SHEET_NAME}!A2:A")
    receb   = _read_column_values(f"{MENU_SHEET_NAME}!B2:B")
    cats    = _read_column_values(f"{MENU_SHEET_NAME}!D2:D")
    pays    = _read_column_values(f"{MENU_SHEET_NAME}!E2:E")

    # envelopes (opcional): D=Categoria, F=Teto Mensal
    budget_rows = _read_range_values(f"{MENU_SHEET_NAME}!D2:F")
    budgets = {}
    for r in budget_rows:
        if not r or len(r) < 1:
            continue
        cat = str(r[0]).strip() if r[0] is not None else ""
        teto = r[2] if len(r) >= 3 else None
        if cat:
            tv = _to_float(teto)
            if tv > 0:
                budgets[cat] = tv

    data = {
        "origens_receita": origens,
        "recebimentos_receita": receb,
        "categorias_despesa": cats,
        "pagamentos_despesa": pays,
        "budgets_mensais": budgets,
    }
    _MENU_CACHE["ts"] = now
    _MENU_CACHE["data"] = data
    return data

# ============================================================
# READ ROWS
# ============================================================
CANON_KEYS = [
    "id", "timestamp", "tipo", "valor", "moeda", "categoria", "descricao",
    "pagamento", "data", "confianca", "confirmado", "mensagem_original"
]

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
        "id": "id",
        "timestamp": "timestamp",
        "tipo": "tipo",
        "valor": "valor",
        "moeda": "moeda",
        "categoria": "categoria",
        "descricao": "descricao",
        "pagamento": "pagamento",
        "data": "data",
        "confianca": "confianca",
        "confirmado": "confirmado",
        "mensagem_original": "mensagem_original",
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

# ============================================================
# HELPERS / PARSERS
# ============================================================
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
        return today_iso_local()
    if t == "ontem":
        return yesterday_iso_local()

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

def period_label(kind: str) -> str:
    return {
        "diario": "Diário",
        "semanal": "Semanal",
        "mensal": "Mensal",
        "3m": "3 meses",
        "6m": "6 meses",
        "12m": "12 meses",
    }.get(kind, kind)

# ============================================================
# DATES / PERIODS
# ============================================================
def _parse_date_any(v):
    if v is None or v == "":
        return None

    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v

    # serial do Sheets
    if isinstance(v, (int, float)):
        try:
            base = dt.date(1899, 12, 30)
            return base + dt.timedelta(days=float(v))
        except:
            return None

    s = str(v).strip()

    # serial como string
    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            base = dt.date(1899, 12, 30)
            return base + dt.timedelta(days=float(s))
        except:
            pass

    # iso YYYY-MM-DD
    try:
        return dt.date.fromisoformat(s[:10])
    except:
        pass

    # dd/mm
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

def get_prev_period(start: dt.date, end: dt.date):
    delta = (end - start).days
    prev_end = start - dt.timedelta(days=1)
    prev_start = prev_end - dt.timedelta(days=delta)
    return prev_start, prev_end

def _filter_period(rows: list, start: dt.date, end: dt.date):
    out = []
    for r in rows:
        d = _parse_date_any(r.get("data"))
        if not d:
            continue
        if start <= d <= end:
            out.append(r)
    return out

# ============================================================
# MENU PAGING (1 lista por vez)
# ============================================================
def send_paged_menu(to: str, body: str, items: list, id_prefix: str, field: str, section_title: str):
    items = [str(x).strip() for x in (items or []) if str(x).strip()]
    if not items:
        send_whatsapp_text(to, "Não encontrei opções na aba Menus. Preencha e tente novamente.")
        return

    offset = 0
    MENU_PAGING[to] = {
        "items": items,
        "offset": offset,
        "id_prefix": id_prefix,
        "field": field,
        "section_title": section_title,
        "body": body
    }
    return _send_paged_menu_page(to)

def _send_paged_menu_page(to: str):
    ctx = MENU_PAGING.get(to)
    if not ctx:
        return

    items = ctx["items"]
    offset = ctx["offset"]
    id_prefix = ctx["id_prefix"]
    section_title = ctx["section_title"]
    body = ctx["body"]

    page_items = items[offset: offset + 9]  # 9 + 1 "Mais opções" = 10
    rows = []
    for i, v in enumerate(page_items):
        global_idx = offset + i
        rows.append({"id": f"{id_prefix}_{global_idx}", "title": v})

    if offset + 9 < len(items):
        rows.append({"id": f"{id_prefix}__more", "title": "Mais opções", "description": "Abrir próxima página"})

    total_pages = (len(items) + 8) // 9
    page = (offset // 9) + 1
    suffix = f" ({page}/{total_pages})" if total_pages > 1 else ""

    send_whatsapp_list(to, body + suffix, "Escolher", rows, section_title=section_title)

# ============================================================
# CATALOGO (NOVO)
# ============================================================
def ask_catalogo(to: str):
    rows = [
        {"id": "cat_fin", "title": "Financeiro", "description": "Receita, despesa, resumos, análises"},
        {"id": "cat_prof", "title": "Profissional", "description": "Em construção"},
        {"id": "cat_pess", "title": "Pessoal", "description": "Em construção"},
        {"id": "cat_treino", "title": "Treinos", "description": "Em construção"},
        {"id": "cat_alim", "title": "Alimentação", "description": "Em construção"},
    ]
    send_whatsapp_list(to, TXT_CATALOGO, "Abrir", rows, section_title="Catálogo")

# ============================================================
# FINANCEIRO - WIZARD UI (RENOMEADO)
# ============================================================
def ask_fin_inicio(to: str):
    send_whatsapp_buttons(
        to,
        TXT_FIN_HOME,
        [
            {"id": "fin_inicio_receita", "title": "Receita"},
            {"id": "fin_inicio_despesa", "title": "Despesa"},
            {"id": "fin_inicio_mais", "title": "Mais"},
        ],
    )

def ask_fin_mais(to: str):
    rows = [
        {"id": "fin_mais_resumo", "title": "Resumo", "description": "Diário, semanal, mensal, 3m, 6m, 12m"},
        {"id": "fin_mais_analises", "title": "Análises", "description": "Financeiro (determinístico)"},
        {"id": "fin_mais_catalogo", "title": "Voltar ao Catálogo", "description": "Trocar de módulo"},
    ]
    send_whatsapp_list(to, "Escolha:", "Abrir", rows, section_title="Financeiro")

def ask_categoria_ou_origem(to: str, tx: dict):
    menus = get_menus()
    if tx.get("tipo") == "receita":
        items = menus.get("origens_receita") or []
        send_paged_menu(to, "Qual a *ORIGEM* dessa receita?", items, id_prefix="origem", field="categoria", section_title="Origem")
    else:
        items = menus.get("categorias_despesa") or []
        send_paged_menu(to, "Qual a *CATEGORIA* dessa despesa?", items, id_prefix="cat", field="categoria", section_title="Categoria")

def ask_pagamento_despesa(to: str):
    menus = get_menus()
    items = menus.get("pagamentos_despesa") or []
    send_paged_menu(to, "Como foi o pagamento?", items, id_prefix="pay", field="pagamento", section_title="Pagamento")

def ask_recebimento_receita(to: str):
    menus = get_menus()
    items = menus.get("recebimentos_receita") or []
    send_paged_menu(to, "Como foi o recebimento?", items, id_prefix="rec", field="pagamento", section_title="Recebimento")

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

def format_confirm(tx: dict):
    v_abs = abs(float(tx["valor"])) if tx.get("valor") not in [None, ""] else 0.0
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

# ============================================================
# RESUMO (FORMATO PEDIDO)
# ============================================================
def build_resumo_text(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    total_rec = 0.0
    total_des = 0.0
    rec_by_cat = defaultdict(float)
    des_by_cat = defaultdict(float)

    for r in filtered:
        tipo = (r.get("tipo") or "").strip().lower()
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        val = _to_float(r.get("valor"))

        if tipo == "receita":
            total_rec += abs(val)
            rec_by_cat[cat] += abs(val)
        elif tipo == "despesa":
            total_des += abs(val)
            des_by_cat[cat] += abs(val)

    rec_top = sorted(rec_by_cat.items(), key=lambda x: x[1], reverse=True)[:12]
    des_top = sorted(des_by_cat.items(), key=lambda x: x[1], reverse=True)[:12]

    perc = (total_des / total_rec * 100.0) if total_rec > 0 else 0.0
    label = f"Resumo {period_label(kind)}"

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

def ask_periodo(to: str, prefix: str):
    send_whatsapp_buttons(
        to,
        "Qual período?",
        [
            {"id": f"{prefix}_diario", "title": "Diário"},
            {"id": f"{prefix}_semanal", "title": "Semanal"},
            {"id": f"{prefix}_mensal", "title": "Mensal"},
        ],
    )
    rows = [
        {"id": f"{prefix}_3m", "title": "3 meses", "description": "Últimos 3 meses"},
        {"id": f"{prefix}_6m", "title": "6 meses", "description": "Últimos 6 meses"},
        {"id": f"{prefix}_12m", "title": "12 meses", "description": "Últimos 12 meses"},
    ]
    send_whatsapp_list(to, "Ou escolha em *Outros*:", "Abrir", rows, section_title="Outros")

# ============================================================
# ANÁLISES (DETERMINÍSTICAS)
# ============================================================
def analysis_kpis(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    rec = 0.0
    des = 0.0
    for r in filtered:
        tipo = (r.get("tipo") or "").strip().lower()
        val = abs(_to_float(r.get("valor")))
        if tipo == "receita":
            rec += val
        elif tipo == "despesa":
            des += val

    saldo = rec - des
    perc = (des / rec * 100.0) if rec > 0 else 0.0

    lines = []
    lines.append(f"*KPIs - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append(f"Receitas: +R$ {fmt_money_br(rec)}")
    lines.append(f"Despesas: -R$ {fmt_money_br(des)}")
    lines.append(f"Saldo:   R$ {fmt_money_br(saldo)}")
    lines.append(f"% Despesas/Receitas: *{perc:.1f}%*")
    return "\n".join(lines)

def analysis_top(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    rec_by = defaultdict(float)
    des_by = defaultdict(float)
    for r in filtered:
        tipo = (r.get("tipo") or "").strip().lower()
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        v = abs(_to_float(r.get("valor")))
        if tipo == "receita":
            rec_by[cat] += v
        elif tipo == "despesa":
            des_by[cat] += v

    rec_top = sorted(rec_by.items(), key=lambda x: x[1], reverse=True)[:10]
    des_top = sorted(des_by.items(), key=lambda x: x[1], reverse=True)[:10]

    lines = []
    lines.append(f"*Top - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append("*Receitas por origem (Top 10)*")
    if not rec_top:
        lines.append("- (sem receitas)")
    else:
        for c, v in rec_top:
            lines.append(f"- {c}: R$ {fmt_money_br(v)}")
    lines.append("")
    lines.append("*Despesas por categoria (Top 10)*")
    if not des_top:
        lines.append("- (sem despesas)")
    else:
        for c, v in des_top:
            lines.append(f"- {c}: R$ {fmt_money_br(v)}")
    return "\n".join(lines)

def analysis_variacao(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    prev_start, prev_end = get_prev_period(start, end)

    now_rows = _filter_period(rows, start, end)
    prev_rows = _filter_period(rows, prev_start, prev_end)

    def agg(rr):
        rec = 0.0
        des = 0.0
        des_by = defaultdict(float)
        for r in rr:
            tipo = (r.get("tipo") or "").strip().lower()
            cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
            v = abs(_to_float(r.get("valor")))
            if tipo == "receita":
                rec += v
            elif tipo == "despesa":
                des += v
                des_by[cat] += v
        return rec, des, des_by

    now_rec, now_des, now_by = agg(now_rows)
    prev_rec, prev_des, prev_by = agg(prev_rows)

    def pct(a, b):
        if b <= 0:
            return None
        return (a - b) / b * 100.0

    lines = []
    lines.append(f"*Variação vs período anterior - {period_label(kind)}*")
    lines.append(f"Atual: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append(f"Anterior: {iso_to_br(prev_start)}  a {iso_to_br(prev_end)}")
    lines.append("")

    pr = pct(now_rec, prev_rec)
    pd = pct(now_des, prev_des)
    lines.append(f"Receitas: R$ {fmt_money_br(prev_rec)} → R$ {fmt_money_br(now_rec)}" + (f" (*{pr:+.1f}%*)" if pr is not None else ""))
    lines.append(f"Despesas: R$ {fmt_money_br(prev_des)} → R$ {fmt_money_br(now_des)}" + (f" (*{pd:+.1f}%*)" if pd is not None else ""))

    deltas = []
    for cat, nowv in now_by.items():
        prevv = prev_by.get(cat, 0.0)
        delta = nowv - prevv
        if abs(delta) > 0.01:
            p = pct(nowv, prevv)
            deltas.append((cat, nowv, prevv, delta, p))
    deltas.sort(key=lambda x: abs(x[3]), reverse=True)

    lines.append("")
    lines.append("*Maiores variações (despesas) - Top 8*")
    if not deltas:
        lines.append("- (sem variação relevante)")
    else:
        for cat, nowv, prevv, delta, p in deltas[:8]:
            pcttxt = f"{p:+.1f}%" if p is not None else "novo"
            lines.append(f"- {cat}: Δ R$ {fmt_money_br(delta)} ({pcttxt})")

    return "\n".join(lines)

def analysis_aumentos(kind: str, threshold_pct: float = 5.0):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    start, end = get_period_range(kind)
    prev_start, prev_end = get_prev_period(start, end)

    now_rows = _filter_period(rows, start, end)
    prev_rows = _filter_period(rows, prev_start, prev_end)

    now_by = defaultdict(float)
    prev_by = defaultdict(float)

    for r in now_rows:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        now_by[cat] += abs(_to_float(r.get("valor")))

    for r in prev_rows:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        prev_by[cat] += abs(_to_float(r.get("valor")))

    aumentos = []
    novos = []

    for cat, now_val in now_by.items():
        prev_val = prev_by.get(cat, 0.0)
        if prev_val <= 0 and now_val > 0:
            novos.append((cat, now_val))
            continue
        pct = ((now_val - prev_val) / prev_val * 100.0) if prev_val > 0 else None
        if pct is not None and pct > threshold_pct:
            delta = now_val - prev_val
            aumentos.append((cat, now_val, prev_val, delta, pct))

    aumentos.sort(key=lambda x: (x[4], x[3]), reverse=True)
    novos.sort(key=lambda x: x[1], reverse=True)

    lines = []
    lines.append(f"*Alertas de aumento (> {threshold_pct:.0f}%) - {period_label(kind)}*")
    lines.append(f"Atual: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append(f"Anterior: {iso_to_br(prev_start)}  a {iso_to_br(prev_end)}")
    lines.append("")

    if not aumentos:
        lines.append("Nenhuma categoria de despesa aumentou acima do limite no comparativo.")
    else:
        lines.append("*Categorias que aumentaram:*")
        for cat, now_val, prev_val, delta, pct in aumentos[:10]:
            lines.append(f"- {cat}: R$ {fmt_money_br(now_val)} (antes R$ {fmt_money_br(prev_val)} | Δ R$ {fmt_money_br(delta)} | +{pct:.1f}%)")

    if novos:
        lines.append("")
        lines.append("*Novos no período (sem histórico no anterior):*")
        for cat, now_val in novos[:8]:
            lines.append(f"- {cat}: R$ {fmt_money_br(now_val)}")

    return "\n".join(lines)

def analysis_corte10(kind: str, cut_pct: float = 0.10):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."

    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    des_by = defaultdict(float)
    total_des = 0.0
    for r in filtered:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        v = abs(_to_float(r.get("valor")))
        total_des += v
        des_by[cat] += v

    lines = []
    lines.append(f"*Cortar {int(cut_pct*100)}% de gastos - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")

    if total_des <= 0:
        lines.append("Sem despesas no período. Não há o que cortar.")
        return "\n".join(lines)

    target_cut = total_des * cut_pct
    cats = sorted(des_by.items(), key=lambda x: x[1], reverse=True)

    top = cats[:8]
    rest_total = sum(v for _, v in cats[8:])
    shown = top[:] + ([("Outros", rest_total)] if rest_total > 0 else [])

    lines.append(f"Despesas totais: -R$ {fmt_money_br(total_des)}")
    lines.append(f"Meta de corte ({int(cut_pct*100)}%): *R$ {fmt_money_br(target_cut)}*")
    lines.append("")
    lines.append("*Sugestão de corte por categoria (proporcional):*")

    running = 0.0
    for cat, v in shown:
        cut = v * cut_pct
        running += cut
        teto = max(0.0, v - cut)
        lines.append(f"- {cat}: atual R$ {fmt_money_br(v)} | cortar R$ {fmt_money_br(cut)} | novo teto R$ {fmt_money_br(teto)}")

    diff = target_cut - running
    if abs(diff) >= 0.01:
        lines.append("")
        lines.append(f"Ajuste de arredondamento: R$ {fmt_money_br(diff)}")

    lines.append("")
    lines.append("Se você não corta dos TOP gastos, você só está brincando de economia.")

    return "\n".join(lines)

def analysis_pagamentos(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    by_tipo_metodo = defaultdict(float)
    for r in filtered:
        tipo = (r.get("tipo") or "").strip().lower()
        metodo = (r.get("pagamento") or "desconhecido").strip().lower() or "desconhecido"
        v = abs(_to_float(r.get("valor")))
        if tipo in ["receita", "despesa"]:
            by_tipo_metodo[(tipo, metodo)] += v

    lines = []
    lines.append(f"*Split por pagamento/recebimento - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append("*Receitas por recebimento:*")
    recs = [(m, v) for (t, m), v in by_tipo_metodo.items() if t == "receita"]
    recs.sort(key=lambda x: x[1], reverse=True)
    if not recs:
        lines.append("- (sem receitas)")
    else:
        for m, v in recs[:10]:
            lines.append(f"- {m}: R$ {fmt_money_br(v)}")

    lines.append("")
    lines.append("*Despesas por pagamento:*")
    dess = [(m, v) for (t, m), v in by_tipo_metodo.items() if t == "despesa"]
    dess.sort(key=lambda x: x[1], reverse=True)
    if not dess:
        lines.append("- (sem despesas)")
    else:
        for m, v in dess[:10]:
            lines.append(f"- {m}: R$ {fmt_money_br(v)}")

    return "\n".join(lines)

def analysis_tendencia(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    daily = defaultdict(float)
    for r in filtered:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        d = _parse_date_any(r.get("data"))
        if not d:
            continue
        daily[d] += abs(_to_float(r.get("valor")))

    days = sorted(daily.items(), key=lambda x: x[0])
    if not days:
        return "Sem despesas no período."

    total = sum(v for _, v in days)
    avg = total / max(1, len(days))
    worst = sorted(days, key=lambda x: x[1], reverse=True)[:5]

    lines = []
    lines.append(f"*Tendência diária (despesas) - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append(f"Média diária (dias com gasto): R$ {fmt_money_br(avg)}")
    lines.append("")
    lines.append("*Dias mais caros (Top 5):*")
    for d, v in worst:
        lines.append(f"- {iso_to_br(d)}: R$ {fmt_money_br(v)}")

    return "\n".join(lines)

def analysis_alertas_picos(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    daily = defaultdict(float)
    for r in filtered:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        d = _parse_date_any(r.get("data"))
        if not d:
            continue
        daily[d] += abs(_to_float(r.get("valor")))

    vals = list(daily.values())
    if not vals:
        return "Sem despesas no período."

    mean = sum(vals) / len(vals)
    spikes = [(d, v) for d, v in daily.items() if v > 2.0 * mean]
    spikes.sort(key=lambda x: x[1], reverse=True)

    lines = []
    lines.append(f"*Alertas (picos de gasto) - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    lines.append(f"Média diária (dias com gasto): R$ {fmt_money_br(mean)}")
    lines.append("")
    if not spikes:
        lines.append("Nenhum pico acima de 2x a média.")
    else:
        lines.append("*Dias fora da curva:*")
        for d, v in spikes[:8]:
            lines.append(f"- {iso_to_br(d)}: R$ {fmt_money_br(v)}")

    return "\n".join(lines)

def analysis_recorrencias(kind: str):
    rows = read_all_rows()
    if not rows:
        return "Não encontrei lançamentos na planilha ainda."
    start, end = get_period_range(kind)
    filtered = _filter_period(rows, start, end)

    cnt = defaultdict(int)
    val = defaultdict(float)
    for r in filtered:
        if (r.get("tipo") or "").strip().lower() != "despesa":
            continue
        desc = (r.get("descricao") or "").strip().lower()
        if not desc:
            continue
        cnt[desc] += 1
        val[desc] += abs(_to_float(r.get("valor")))

    items = [(d, c, val[d]) for d, c in cnt.items() if c >= 2]
    items.sort(key=lambda x: (x[1], x[2]), reverse=True)

    lines = []
    lines.append(f"*Recorrências (despesas) - {period_label(kind)}*")
    lines.append(f"Período: {iso_to_br(start)}  a {iso_to_br(end)}")
    lines.append("")
    if not items:
        lines.append("Nenhuma descrição repetiu 2x ou mais no período.")
    else:
        lines.append("*Itens recorrentes (>=2x):*")
        for d, c, v in items[:10]:
            lines.append(f"- {d[:24]}: {c}x | total R$ {fmt_money_br(v)}")

    return "\n".join(lines)

def ask_analise_tipo(to: str):
    rows = [
        {"id": "fin_an_kpis", "title": "KPIs do período", "description": "Receita, despesa, saldo, %"},
        {"id": "fin_an_top", "title": "Top categorias/origens", "description": "Ranking e concentração"},
        {"id": "fin_an_variacao", "title": "Variação vs anterior", "description": "Δ e %"},
        {"id": "fin_an_aumento", "title": "Alertas aumento (>5%)", "description": "Por categoria (despesa)"},
        {"id": "fin_an_corte10", "title": "Cortar 10% de gastos", "description": "Plano por categoria"},
        {"id": "fin_an_pagamentos", "title": "Split pagamento/recebimento", "description": "Por método"},
        {"id": "fin_an_tendencia", "title": "Tendência diária", "description": "Dias mais caros e média"},
        {"id": "fin_an_alertas", "title": "Alertas (picos)", "description": "Dias fora da curva"},
        {"id": "fin_an_recorrencias", "title": "Recorrências", "description": "Descrições repetidas"},
        {"id": "fin_an_voltar", "title": "Voltar", "description": "Escolher outro período"},
    ]
    send_whatsapp_list(to, "Qual análise você quer?", "Abrir", rows, section_title="Análises")

# ============================================================
# DEDUP
# ============================================================
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

# ============================================================
# WEBHOOKS
# ============================================================
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

        # cancelar
        if kind == "text" and val.lower().strip() in ["cancelar", "cancela"]:
            PENDING.pop(from_number, None)
            MENU_PAGING.pop(from_number, None)
            send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
            continue

        pending = PENDING.get(from_number)

        # inicia catálogo no primeiro contato
        if not pending:
            PENDING[from_number] = {"tx": None, "await": "catalogo", "stage": "catalogo", "ctx": {}}
            ask_catalogo(from_number)
            continue

        await_field = pending.get("await")
        ctx = pending.get("ctx") or {}

        # =====================================================
        # CATALOGO (NOVO)
        # =====================================================
        if await_field == "catalogo":
            if kind != "choice":
                ask_catalogo(from_number)
                continue

            if val == "cat_fin":
                ctx["module"] = "financeiro"
                pending["ctx"] = ctx
                pending["await"] = "fin_inicio"
                ask_fin_inicio(from_number)
                continue

            # placeholders (por enquanto)
            if val in ["cat_prof", "cat_pess", "cat_treino", "cat_alim"]:
                nome = {
                    "cat_prof": "Profissional",
                    "cat_pess": "Pessoal",
                    "cat_treino": "Treinos",
                    "cat_alim": "Alimentação",
                }.get(val, "Módulo")
                send_whatsapp_text(from_number, f"{nome} ainda não está implementado. Por enquanto, só *Financeiro*.")
                ask_catalogo(from_number)
                continue

            ask_catalogo(from_number)
            continue

        # =====================================================
        # FINANCEIRO - HOME (botões)
        # =====================================================
        if await_field == "fin_inicio":
            if kind != "choice":
                ask_fin_inicio(from_number)
                continue

            if val == "fin_inicio_receita":
                tx = {
                    "id": str(uuid.uuid4()),
                    "timestamp": now_iso_utc(),
                    "tipo": "receita",
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
                pending["await"] = continue_wizard(from_number, tx)
                continue

            if val == "fin_inicio_despesa":
                tx = {
                    "id": str(uuid.uuid4()),
                    "timestamp": now_iso_utc(),
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
                pending["await"] = continue_wizard(from_number, tx)
                continue

            if val == "fin_inicio_mais":
                pending["tx"] = None
                pending["await"] = "fin_mais_menu"
                ask_fin_mais(from_number)
                continue

            ask_fin_inicio(from_number)
            continue

        # =====================================================
        # FINANCEIRO - MAIS (Resumo / Análises / Catálogo)
        # =====================================================
        if await_field == "fin_mais_menu":
            if kind != "choice":
                ask_fin_mais(from_number)
                continue

            if val == "fin_mais_resumo":
                pending["await"] = "fin_resumo_periodo"
                ask_periodo(from_number, prefix="fin_res")
                continue

            if val == "fin_mais_analises":
                pending["await"] = "fin_analise_periodo"
                ask_periodo(from_number, prefix="fin_anp")
                continue

            if val == "fin_mais_catalogo":
                pending["tx"] = None
                pending["await"] = "catalogo"
                ask_catalogo(from_number)
                continue

            ask_fin_mais(from_number)
            continue

        # =====================================================
        # FINANCEIRO - RESUMO: período
        # =====================================================
        if await_field == "fin_resumo_periodo":
            if kind != "choice":
                ask_periodo(from_number, prefix="fin_res")
                continue

            period_map = {
                "fin_res_diario": "diario",
                "fin_res_semanal": "semanal",
                "fin_res_mensal": "mensal",
                "fin_res_3m": "3m",
                "fin_res_6m": "6m",
                "fin_res_12m": "12m",
            }
            k = period_map.get(val)
            if not k:
                ask_periodo(from_number, prefix="fin_res")
                continue

            send_whatsapp_text(from_number, build_resumo_text(k))
            PENDING.pop(from_number, None)
            MENU_PAGING.pop(from_number, None)
            continue

        # =====================================================
        # FINANCEIRO - ANÁLISE: período
        # =====================================================
        if await_field == "fin_analise_periodo":
            if kind != "choice":
                ask_periodo(from_number, prefix="fin_anp")
                continue

            period_map = {
                "fin_anp_diario": "diario",
                "fin_anp_semanal": "semanal",
                "fin_anp_mensal": "mensal",
                "fin_anp_3m": "3m",
                "fin_anp_6m": "6m",
                "fin_anp_12m": "12m",
            }
            k = period_map.get(val)
            if not k:
                ask_periodo(from_number, prefix="fin_anp")
                continue

            ctx["analysis_kind"] = k
            pending["ctx"] = ctx
            pending["await"] = "fin_analise_tipo"
            ask_analise_tipo(from_number)
            continue

        # =====================================================
        # FINANCEIRO - ANÁLISE: tipo
        # =====================================================
        if await_field == "fin_analise_tipo":
            if kind != "choice":
                ask_analise_tipo(from_number)
                continue

            kind_sel = ctx.get("analysis_kind") or "mensal"

            if val == "fin_an_voltar":
                pending["await"] = "fin_analise_periodo"
                ask_periodo(from_number, prefix="fin_anp")
                continue

            if val == "fin_an_kpis":
                send_whatsapp_text(from_number, analysis_kpis(kind_sel))
            elif val == "fin_an_top":
                send_whatsapp_text(from_number, analysis_top(kind_sel))
            elif val == "fin_an_variacao":
                send_whatsapp_text(from_number, analysis_variacao(kind_sel))
            elif val == "fin_an_aumento":
                send_whatsapp_text(from_number, analysis_aumentos(kind_sel, threshold_pct=5.0))
            elif val == "fin_an_corte10":
                send_whatsapp_text(from_number, analysis_corte10(kind_sel, cut_pct=0.10))
            elif val == "fin_an_pagamentos":
                send_whatsapp_text(from_number, analysis_pagamentos(kind_sel))
            elif val == "fin_an_tendencia":
                send_whatsapp_text(from_number, analysis_tendencia(kind_sel))
            elif val == "fin_an_alertas":
                send_whatsapp_text(from_number, analysis_alertas_picos(kind_sel))
            elif val == "fin_an_recorrencias":
                send_whatsapp_text(from_number, analysis_recorrencias(kind_sel))
            else:
                ask_analise_tipo(from_number)
                continue

            PENDING.pop(from_number, None)
            MENU_PAGING.pop(from_number, None)
            continue

        # =====================================================
        # A partir daqui: wizard de lançamento (financeiro)
        # =====================================================
        tx = pending.get("tx") or {}

        # CONFIRM
        if await_field == "confirm":
            if (kind == "choice" and val == "confirm_sim") or (kind == "text" and val.lower().strip() in ["sim", "ok", "confirmar"]):
                tx["confirmado"] = "sim"
                ensure_receita_descricao(tx)
                normalize_sign(tx)
                append_row(tx_to_row(tx))
                PENDING.pop(from_number, None)
                MENU_PAGING.pop(from_number, None)
                send_whatsapp_text(from_number, MSG_SALVO)
                continue

            if (kind == "choice" and val == "confirm_cancelar") or (kind == "text" and val.lower().strip() in ["nao", "não", "cancelar", "cancela"]):
                PENDING.pop(from_number, None)
                MENU_PAGING.pop(from_number, None)
                send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
                continue

            send_whatsapp_text(from_number, "Selecione SIM para gravar ou CANCELAR para descartar.")
            continue

        # MENUS PAGINADOS (origem/cat/pay/rec)
        if kind == "choice":
            mp = MENU_PAGING.get(from_number)
            if mp and val:
                if val == f"{mp['id_prefix']}__more":
                    mp["offset"] = mp["offset"] + 9
                    MENU_PAGING[from_number] = mp
                    _send_paged_menu_page(from_number)
                    continue

                if val.startswith(mp["id_prefix"] + "_"):
                    try:
                        idx = int(val.split("_")[-1])
                    except:
                        idx = None

                    if idx is not None and 0 <= idx < len(mp["items"]):
                        chosen = mp["items"][idx]
                        field = mp["field"]

                        if field == "categoria":
                            tx["categoria"] = chosen
                        elif field == "pagamento":
                            tx["pagamento"] = str(chosen).strip().lower() or "desconhecido"

                        MENU_PAGING.pop(from_number, None)
                        pending["tx"] = tx
                        pending["await"] = continue_wizard(from_number, tx)
                        continue

        # VALOR
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

        # DESCRIÇÃO (despesa)
        if await_field == "descricao":
            if kind != "text" or not val.strip():
                ask_text_field(from_number, "descricao", tx)
                continue
            tx["descricao"] = val.strip()
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            continue

        # DATA
        if await_field == "data":
            if kind == "choice" and val in ["data_hoje", "data_ontem", "data_outra"]:
                if val == "data_hoje":
                    tx["data"] = today_iso_local()
                    pending["tx"] = tx
                    pending["await"] = continue_wizard(from_number, tx)
                    continue
                if val == "data_ontem":
                    tx["data"] = yesterday_iso_local()
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
