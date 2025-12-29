import os
import re
import uuid
import datetime as dt
import requests
from collections import defaultdict

from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()
app = FastAPI()

GRAPH_VER = "v22.0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

PENDING = {}  # {from: {"tx": {...}, "await": "...", "stage": "..."}}

ORIGENS_RECEITA = [
    "Salário", "Férias", "13º", "Bônus", "Comissão", "PLR",
    "Reembolso", "Rendimentos", "Freela", "Outros"
]
CATEGORIAS_DESPESA = [
    "Mercado", "Transporte", "Moradia", "Alimentação", "Assinaturas",
    "Saúde", "Lazer", "Educação", "Impostos", "Outros"
]

PAGAMENTOS_DESPESA = ["pix", "débito", "crédito", "dinheiro", "desconhecido"]

MSG_SALVO = "Show, já registrei aqui no nosso BD, quando tiver mais alguma movimentação me sinalize aqui!"

TXT_INICIAL = "Olá, bora conferir saldos hoje ou você quer registrar algo?"

# ----------------------------
# WhatsApp send
# ----------------------------
def wa_url():
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    return f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"

def wa_headers():
    token = os.environ["WA_ACCESS_TOKEN"]
    return {"Authorization": f"Bearer {token}"}

def _post_wa(payload: dict):
    r = requests.post(wa_url(), headers=wa_headers(), json=payload, timeout=20)
    if r.status_code >= 400:
        print("WHATSAPP API ERROR:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def send_whatsapp_text(to: str, text: str):
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text[:3800]},
    })

def send_whatsapp_buttons(to: str, body_text: str, buttons: list):
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text[:1024]},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": b["id"], "title": b["title"][:20]}}
                    for b in buttons[:3]
                ]
            },
        },
    })

def send_whatsapp_list(to: str, body_text: str, button_label: str, rows: list, section_title: str = "Opções"):
    return _post_wa({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text[:1024]},
            "action": {
                "button": button_label[:20],
                "sections": [{
                    "title": section_title[:24],
                    "rows": [{
                        "id": r["id"][:200],
                        "title": r["title"][:24],
                        "description": (r.get("description") or "")[:72],
                    } for r in rows[:10]],
                }],
            },
        },
    })

# ----------------------------
# Google Sheets
# ----------------------------
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

def read_all_rows():
    """
    Lê a planilha inteira e retorna lista de dicts.
    Pressupõe header na primeira linha com nomes:
    id,timestamp,tipo,valor,moeda,categoria,descricao,pagamento,data_competencia,confianca,confirmado,mensagem_original
    """
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    rng = os.environ.get("GOOGLE_SHEETS_READ_RANGE") or os.environ.get("GOOGLE_SHEETS_RANGE", "lancamentos!A1")
    svc = _sheets_service()
    res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get("values") or []
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    rows = []
    for line in values[1:]:
        row = {}
        for i, h in enumerate(headers):
            row[h] = line[i] if i < len(line) else ""
        rows.append(row)
    return rows

# ----------------------------
# Helpers
# ----------------------------
def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def today_iso():
    return dt.date.today().isoformat()

def parse_valor(text: str):
    t = text.lower()
    m = re.search(r"(\d{1,9}(?:[.,]\d{2})?)", t)
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def parse_data(text: str):
    t = text.lower().strip()
    if t == "hoje":
        return dt.date.today().isoformat()
    if t == "ontem":
        return (dt.date.today() - dt.timedelta(days=1)).isoformat()

    m = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b", t)
    if not m:
        return None
    d = int(m.group(1))
    mo = int(m.group(2))
    y = m.group(3)
    if y is None:
        y = dt.date.today().year
    else:
        y = int(y)
        if y < 100:
            y += 2000
    try:
        return dt.date(y, mo, d).isoformat()
    except:
        return None

def normalize_sign(tx: dict):
    if tx.get("valor") is None:
        return
    v = float(tx["valor"])
    if tx.get("tipo") == "despesa":
        tx["valor"] = -abs(v)
    elif tx.get("tipo") == "receita":
        tx["valor"] = abs(v)

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
        tx["id"],
        tx["timestamp"],
        tx["tipo"],
        tx["valor"],
        tx["moeda"],
        tx["categoria"],
        tx["descricao"],
        tx["pagamento"],
        tx["data_competencia"],
        tx["confianca"],
        tx["confirmado"],
        tx["mensagem_original"],
    ]

def format_confirm(tx: dict):
    v = f"{abs(float(tx['valor'])):.2f}".replace(".", ",") if tx.get("valor") is not None else "N/A"
    label_cat = "origem" if tx.get("tipo") == "receita" else "categoria"
    label_pay = "recebimento" if tx.get("tipo") == "receita" else "pagamento"
    sinal = "+" if tx.get("tipo") == "receita" else "-"
    return (
        "Confirma o lançamento?\n"
        f"- tipo: {tx.get('tipo')}\n"
        f"- valor: {sinal}{v} {tx.get('moeda','BRL')}\n"
        f"- {label_cat}: {tx.get('categoria')}\n"
        f"- {label_pay}: {tx.get('pagamento')}\n"
        f"- data_competencia: {tx.get('data_competencia')}\n"
    )

def required_fields(tx: dict):
    base = ["tipo", "valor", "categoria", "pagamento", "data_competencia"]
    if tx.get("tipo") == "despesa":
        base.insert(3, "descricao")
    return base

def next_missing(tx: dict):
    for f in required_fields(tx):
        v = tx.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            return f
    return None

# ----------------------------
# Wizard: telas
# ----------------------------
def ask_inicio(to: str):
    send_whatsapp_buttons(
        to,
        TXT_INICIAL,
        [
            {"id": "inicio_receita", "title": "Receita"},
            {"id": "inicio_despesa", "title": "Despesa"},
            {"id": "inicio_resumo", "title": "Resumo"},
        ],
    )

def ask_categoria_ou_origem(to: str, tx: dict):
    if tx.get("tipo") == "receita":
        rows = [{"id": f"origem_{o.lower().replace('º','o').replace(' ', '_')}", "title": o} for o in ORIGENS_RECEITA]
        send_whatsapp_list(to, "Qual a ORIGEM dessa receita?", "Escolher", rows, section_title="Origem")
    else:
        rows = [{"id": f"cat_{c.lower().replace(' ', '_')}", "title": c} for c in CATEGORIAS_DESPESA]
        send_whatsapp_list(to, "Qual a CATEGORIA dessa despesa?", "Escolher", rows, section_title="Categoria")

def ask_pagamento_despesa(to: str):
    rows = [{"id": f"pay_{p.replace('é','e').replace('í','i')}", "title": p} for p in PAGAMENTOS_DESPESA]
    send_whatsapp_list(to, "Como foi o pagamento?", "Escolher", rows, section_title="Pagamento")

def ask_recebimento_receita(to: str):
    send_whatsapp_buttons(
        to,
        "Como foi o recebimento?",
        [
            {"id": "rec_dinheiro", "title": "Dinheiro"},
            {"id": "rec_pix", "title": "PIX"},
        ],
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

def ask_confirm(to: str, tx: dict):
    msg = format_confirm(tx) + "\n\nSelecione:"
    send_whatsapp_buttons(
        to,
        msg,
        [
            {"id": "confirm_sim", "title": "SIM"},
            {"id": "confirm_cancelar", "title": "CANCELAR"},
        ],
    )

def ask_resumo_periodo(to: str):
    # 3 botões + lista para 12 meses (limitação de 3 botões)
    send_whatsapp_buttons(
        to,
        "Qual resumo você quer ver?",
        [
            {"id": "res_diario", "title": "Diário"},
            {"id": "res_semanal", "title": "Semanal"},
            {"id": "res_mensal", "title": "Mensal"},
        ],
    )
    # e em seguida, manda lista com 12 meses (ou você pode mandar apenas se pedir)
    rows = [{"id": "res_12m", "title": "12 meses", "description": "Últimos 12 meses"}]
    send_whatsapp_list(to, "Ou escolha:", "Abrir", rows, section_title="Outros")

def ask_text_field(to: str, field: str, tx: dict):
    if field == "valor":
        send_whatsapp_text(to, "Qual o VALOR? Ex: 35,90")
    elif field == "descricao":
        send_whatsapp_text(to, "Qual a DESCRIÇÃO (curta)? Ex: pão e leite")
    elif field == "data_competencia":
        send_whatsapp_text(to, "Digite a data (dd/mm) ou 'hoje' / 'ontem'.")
    elif field == "categoria":
        if tx.get("tipo") == "receita":
            send_whatsapp_text(to, "Digite a ORIGEM (texto). Ex: Salário, PLR, etc.")
        else:
            send_whatsapp_text(to, "Digite a CATEGORIA (texto). Ex: Pet, Viagem, etc.")
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

    if nxt == "data_competencia":
        ask_data(to)
        return "data"

    ask_text_field(to, nxt, tx)
    return nxt

# ----------------------------
# Resumo: cálculo
# ----------------------------
def _to_float(v):
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def _parse_iso_date(v):
    # data_competencia esperado YYYY-MM-DD
    try:
        return dt.date.fromisoformat(str(v)[:10])
    except:
        return None

def get_period_range(kind: str):
    today = dt.date.today()
    if kind == "diario":
        start = today
    elif kind == "semanal":
        start = today - dt.timedelta(days=6)
    elif kind == "mensal":
        start = today.replace(day=1)
    elif kind == "12m":
        # últimos 365 dias
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
        d = _parse_iso_date(r.get("data_competencia"))
        if not d:
            continue
        if d < start or d > end:
            continue

        tipo = (r.get("tipo") or "").strip().lower()
        cat = (r.get("categoria") or "Sem categoria").strip() or "Sem categoria"
        val = _to_float(r.get("valor"))

        # seu modelo: receita positivo, despesa negativo
        if tipo == "receita":
            total_rec += abs(val)
            rec_by_cat[cat] += abs(val)
        elif tipo == "despesa":
            total_des += abs(val)  # abs para mostrar total de gasto
            des_by_cat[cat] += abs(val)

    saldo = total_rec - total_des

    # ordenar top categorias
    rec_top = sorted(rec_by_cat.items(), key=lambda x: x[1], reverse=True)[:8]
    des_top = sorted(des_by_cat.items(), key=lambda x: x[1], reverse=True)[:8]

    def fmt_money(x):
        return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    label = {"diario": "Diário", "semanal": "Semanal", "mensal": "Mensal", "12m": "Últimos 12 meses"}.get(kind, kind)

    lines = []
    lines.append(f"Resumo {label} ({start.isoformat()} a {end.isoformat()})")
    lines.append("")
    lines.append(f"Receitas: +R$ {fmt_money(total_rec)}")
    lines.append(f"Despesas: -R$ {fmt_money(total_des)}")
    lines.append(f"Saldo:   R$ {fmt_money(saldo)}")
    lines.append("")
    lines.append("Receitas por origem (top):")
    if rec_top:
        for c, v in rec_top:
            lines.append(f"- {c}: R$ {fmt_money(v)}")
    else:
        lines.append("- (sem receitas no período)")
    lines.append("")
    lines.append("Despesas por categoria (top):")
    if des_top:
        for c, v in des_top:
            lines.append(f"- {c}: R$ {fmt_money(v)}")
    else:
        lines.append("- (sem despesas no período)")

    return "\n".join(lines)

# ----------------------------
# Inbound parse
# ----------------------------
def extract_inbound(msg: dict):
    if msg.get("type") == "interactive" or msg.get("interactive"):
        inter = msg.get("interactive") or {}
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

# ----------------------------
# Webhook
# ----------------------------
@app.get("/")
def home():
    return {"status": "ok"}

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
    messages = value.get("messages") or []

    if not messages:
        return {"ok": True}

    msg = messages[0]
    from_number = msg.get("from")

    allowed = os.environ.get("ALLOWED_WA_NUMBER", "").strip()
    if allowed and from_number != allowed:
        return {"ok": True}

    kind, val, title = extract_inbound(msg)

    # cancelar
    if kind == "text" and val.lower().strip() in ["cancelar", "cancela"]:
        PENDING.pop(from_number, None)
        send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
        return {"ok": True}

    pending = PENDING.get(from_number)

    # Se NÃO há wizard ativo: inicia menu inicial
    if not pending:
        PENDING[from_number] = {"tx": None, "await": "inicio", "stage": "menu"}
        ask_inicio(from_number)
        return {"ok": True}

    await_field = pending.get("await")

    # -------------------------
    # MENU INICIAL
    # -------------------------
    if await_field == "inicio":
        if kind != "choice":
            ask_inicio(from_number)
            return {"ok": True}

        if val == "inicio_receita":
            tx = {
                "id": str(uuid.uuid4()),
                "timestamp": now_iso(),
                "tipo": "receita",
                "valor": None,
                "moeda": "BRL",
                "categoria": None,     # origem
                "descricao": None,     # auto
                "pagamento": None,     # recebimento (pix/dinheiro)
                "data_competencia": None,
                "confianca": 0.60,
                "confirmado": "não",
                "mensagem_original": "",
            }
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}

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
                "data_competencia": None,
                "confianca": 0.60,
                "confirmado": "não",
                "mensagem_original": "",
            }
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}

        if val == "inicio_resumo":
            pending["tx"] = None
            pending["await"] = "resumo_periodo"
            ask_resumo_periodo(from_number)
            return {"ok": True}

        ask_inicio(from_number)
        return {"ok": True}

    # -------------------------
    # RESUMO: escolher período
    # -------------------------
    if await_field == "resumo_periodo":
        if kind != "choice":
            ask_resumo_periodo(from_number)
            return {"ok": True}

        if val == "res_diario":
            txt = build_resumo_text("diario")
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            return {"ok": True}

        if val == "res_semanal":
            txt = build_resumo_text("semanal")
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            return {"ok": True}

        if val == "res_mensal":
            txt = build_resumo_text("mensal")
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            return {"ok": True}

        if val == "res_12m":
            txt = build_resumo_text("12m")
            send_whatsapp_text(from_number, txt)
            PENDING.pop(from_number, None)
            return {"ok": True}

        ask_resumo_periodo(from_number)
        return {"ok": True}

    # daqui pra frente: fluxo de lançamento (wizard)
    tx = pending.get("tx") or {}
    # -------------------------
    # CONFIRMAÇÃO
    # -------------------------
    if await_field == "confirm":
        if (kind == "choice" and val == "confirm_sim") or (kind == "text" and val.lower().strip() in ["sim", "ok", "confirmar"]):
            tx["confirmado"] = "sim"
            ensure_receita_descricao(tx)
            normalize_sign(tx)
            append_row(tx_to_row(tx))
            PENDING.pop(from_number, None)
            send_whatsapp_text(from_number, MSG_SALVO)
            return {"ok": True}

        if (kind == "choice" and val == "confirm_cancelar") or (kind == "text" and val.lower().strip() in ["nao", "não", "cancelar", "cancela"]):
            PENDING.pop(from_number, None)
            send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
            return {"ok": True}

        send_whatsapp_text(from_number, "Selecione SIM para gravar ou CANCELAR para descartar.")
        return {"ok": True}

    # -------------------------
    # CATEGORIA / ORIGEM
    # -------------------------
    if await_field == "categoria":
        if kind == "choice" and val:
            if tx.get("tipo") == "receita":
                if val.startswith("origem_"):
                    tx["categoria"] = title or "Outros"
            else:
                if val.startswith("cat_"):
                    tx["categoria"] = title or "Outros"

            if (tx.get("categoria") or "").lower() == "outros":
                pending["tx"] = tx
                pending["await"] = "categoria_texto"
                ask_text_field(from_number, "categoria", tx)
                return {"ok": True}

            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}

        send_whatsapp_text(from_number, "Escolha uma opção na lista.")
        ask_categoria_ou_origem(from_number, tx)
        return {"ok": True}

    if await_field == "categoria_texto":
        if kind != "text" or not val.strip():
            ask_text_field(from_number, "categoria", tx)
            return {"ok": True}
        tx["categoria"] = val.strip()
        pending["tx"] = tx
        pending["await"] = continue_wizard(from_number, tx)
        return {"ok": True}

    # -------------------------
    # VALOR (texto)
    # -------------------------
    if await_field == "valor":
        if kind != "text":
            ask_text_field(from_number, "valor", tx)
            return {"ok": True}
        v = parse_valor(val)
        if v is None:
            send_whatsapp_text(from_number, "Valor inválido. Ex: 35,90")
            ask_text_field(from_number, "valor", tx)
            return {"ok": True}
        tx["valor"] = v
        pending["tx"] = tx
        pending["await"] = continue_wizard(from_number, tx)
        return {"ok": True}

    # -------------------------
    # DESCRIÇÃO (apenas despesa)
    # -------------------------
    if await_field == "descricao":
        if kind != "text" or not val.strip():
            ask_text_field(from_number, "descricao", tx)
            return {"ok": True}
        tx["descricao"] = val.strip()
        pending["tx"] = tx
        pending["await"] = continue_wizard(from_number, tx)
        return {"ok": True}

    # -------------------------
    # PAGAMENTO (despesa)
    # -------------------------
    if await_field == "pagamento":
        if kind == "choice" and val and val.startswith("pay_"):
            tx["pagamento"] = (title or "desconhecido").lower().strip()
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}
        send_whatsapp_text(from_number, "Escolha uma opção na lista de pagamento.")
        ask_pagamento_despesa(from_number)
        return {"ok": True}

    # -------------------------
    # RECEBIMENTO (receita)
    # -------------------------
    if await_field == "recebimento":
        if kind == "choice" and val in ["rec_dinheiro", "rec_pix"]:
            tx["pagamento"] = "dinheiro" if val == "rec_dinheiro" else "pix"
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}
        send_whatsapp_text(from_number, "Use os botões: Dinheiro ou PIX.")
        ask_recebimento_receita(from_number)
        return {"ok": True}

    # -------------------------
    # DATA
    # -------------------------
    if await_field == "data":
        if kind == "choice" and val in ["data_hoje", "data_ontem", "data_outra"]:
            if val == "data_hoje":
                tx["data_competencia"] = today_iso()
                pending["tx"] = tx
                pending["await"] = continue_wizard(from_number, tx)
                return {"ok": True}
            if val == "data_ontem":
                tx["data_competencia"] = (dt.date.today() - dt.timedelta(days=1)).isoformat()
                pending["tx"] = tx
                pending["await"] = continue_wizard(from_number, tx)
                return {"ok": True}
            pending["tx"] = tx
            pending["await"] = "data_texto"
            ask_text_field(from_number, "data_competencia", tx)
            return {"ok": True}
        send_whatsapp_text(from_number, "Use os botões: Hoje / Ontem / Outra.")
        ask_data(from_number)
        return {"ok": True}

    if await_field == "data_texto":
        if kind != "text" or not val.strip():
            ask_text_field(from_number, "data_competencia", tx)
            return {"ok": True}
        d = parse_data(val.strip())
        if not d:
            send_whatsapp_text(from_number, "Data inválida. Use hoje/ontem ou dd/mm (ex: 29/12).")
            ask_text_field(from_number, "data_competencia", tx)
            return {"ok": True}
        tx["data_competencia"] = d
        pending["tx"] = tx
        pending["await"] = continue_wizard(from_number, tx)
        return {"ok": True}

    # fallback
    pending["tx"] = tx
    pending["await"] = continue_wizard(from_number, tx)
    return {"ok": True}
