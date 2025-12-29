import os
import re
import uuid
import datetime as dt
import requests

from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()
app = FastAPI()

GRAPH_VER = "v22.0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Estado por número: {"tx": {...}, "await": "tipo|valor|categoria|descricao|pagamento|data|confirm", "stage":"wizard"}
PENDING = {}

# ORIGEM (receita) -> gravar na coluna categoria
ORIGENS_RECEITA = [
    "Salário", "Férias", "13º", "Bônus", "Comissão", "PLR",
    "Reembolso", "Rendimentos", "Freela", "Outros"
]

# Categorias padrão para despesa (lista)
CATEGORIAS_DESPESA = [
    "Mercado", "Transporte", "Moradia", "Alimentação", "Assinaturas",
    "Saúde", "Lazer", "Educação", "Impostos", "Outros"
]

PAGAMENTOS = ["pix", "débito", "crédito", "dinheiro", "desconhecido"]

# ----------------------------
# WhatsApp: envio
# ----------------------------
def wa_url():
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    return f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"

def wa_headers():
    token = os.environ["WA_ACCESS_TOKEN"]
    return {"Authorization": f"Bearer {token}"}

def send_whatsapp_text(to: str, text: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text[:3800]},
    }
    r = requests.post(wa_url(), headers=wa_headers(), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def send_whatsapp_buttons(to: str, body_text: str, buttons: list):
    """
    buttons: [{"id":"x","title":"X"}, ...]  (máx 3)
    """
    payload = {
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
    }
    r = requests.post(wa_url(), headers=wa_headers(), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def send_whatsapp_list(to: str, body_text: str, button_label: str, rows: list, section_title: str = "Opções"):
    """
    rows: [{"id":"x","title":"X","description":"..."}]
    """
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text[:1024]},
            "action": {
                "button": button_label[:20],
                "sections": [
                    {
                        "title": section_title[:24],
                        "rows": [
                            {
                                "id": r["id"][:200],
                                "title": r["title"][:24],
                                "description": (r.get("description") or "")[:72],
                            }
                            for r in rows[:10]
                        ],
                    }
                ],
            },
        },
    }
    r = requests.post(wa_url(), headers=wa_headers(), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ----------------------------
# Sheets: append
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

# ----------------------------
# Parsing / Normalização
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

def tx_to_row(tx: dict):
    # id, timestamp, tipo, valor, moeda, categoria, descricao, pagamento, data_competencia, confianca, confirmado, mensagem_original
    return [
        tx["id"],
        tx["timestamp"],
        tx["tipo"],                 # "receita" ou "despesa"
        tx["valor"],                # sinal já normalizado
        tx["moeda"],
        tx["categoria"],            # despesa=categoria, receita=origem
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
    sinal = "+" if tx.get("tipo") == "receita" else "-"
    return (
        "Confirma o lançamento?\n"
        f"- tipo: {tx.get('tipo')}\n"
        f"- valor: {sinal}{v} {tx.get('moeda','BRL')}\n"
        f"- {label_cat}: {tx.get('categoria')}\n"
        f"- descricao: {tx.get('descricao')}\n"
        f"- pagamento: {tx.get('pagamento')}\n"
        f"- data_competencia: {tx.get('data_competencia')}\n"
    )

def required_fields(tx: dict):
    # Wizard sempre pede: tipo, valor, categoria/origem, descricao, pagamento, data
    return ["tipo", "valor", "categoria", "descricao", "pagamento", "data_competencia"]

def next_missing(tx: dict):
    for f in required_fields(tx):
        v = tx.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            return f
    return None

# ----------------------------
# Wizard: perguntas por seleção quando possível
# ----------------------------
def ask_tipo(to: str):
    send_whatsapp_buttons(
        to,
        "Opa. O que vamos registrar hoje?\nEscolha uma opção:",
        [
            {"id": "tipo_receita", "title": "Receita"},
            {"id": "tipo_despesa", "title": "Despesa"},
        ],
    )

def ask_categoria_ou_origem(to: str, tx: dict):
    if tx.get("tipo") == "receita":
        rows = [{"id": f"origem_{o.lower().replace('º','o').replace(' ', '_')}", "title": o} for o in ORIGENS_RECEITA]
        send_whatsapp_list(
            to,
            "Qual a ORIGEM dessa receita?",
            "Escolher",
            rows,
            section_title="Origem",
        )
    else:
        rows = [{"id": f"cat_{c.lower().replace(' ', '_')}", "title": c} for c in CATEGORIAS_DESPESA]
        send_whatsapp_list(
            to,
            "Qual a CATEGORIA dessa despesa?",
            "Escolher",
            rows,
            section_title="Categoria",
        )

def ask_pagamento(to: str):
    rows = [{"id": f"pay_{p.replace('é','e').replace('í','i')}", "title": p} for p in PAGAMENTOS]
    send_whatsapp_list(
        to,
        "Como foi o pagamento?",
        "Escolher",
        rows,
        section_title="Pagamento",
    )

def ask_data(to: str):
    # botões (máx 3) -> Hoje / Ontem / Outra
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
    msg = format_confirm(tx) + "\n\nResponda selecionando:"
    send_whatsapp_buttons(
        to,
        msg,
        [
            {"id": "confirm_sim", "title": "SIM"},
            {"id": "confirm_cancelar", "title": "CANCELAR"},
        ],
    )

def ask_text_field(to: str, field: str, tx: dict):
    if field == "valor":
        send_whatsapp_text(to, "Qual o VALOR? Ex: 35,90")
    elif field == "descricao":
        send_whatsapp_text(to, "Qual a DESCRIÇÃO (curta)? Ex: pão e leite")
    elif field == "data_competencia":
        send_whatsapp_text(to, "Digite a data (dd/mm) ou 'hoje' / 'ontem'.")
    elif field == "categoria":
        # só cai aqui se usuário escolheu "Outros" e precisa digitar
        if tx.get("tipo") == "receita":
            send_whatsapp_text(to, "Digite a ORIGEM (texto). Ex: Salário, PLR, etc.")
        else:
            send_whatsapp_text(to, "Digite a CATEGORIA (texto). Ex: Pet, Viagem, etc.")
    else:
        send_whatsapp_text(to, "Preciso de uma informação (texto).")

def continue_wizard(to: str, tx: dict):
    nxt = next_missing(tx)
    if nxt is None:
        # normaliza sinal antes de confirmar
        normalize_sign(tx)
        ask_confirm(to, tx)
        return "confirm"

    if nxt == "tipo":
        ask_tipo(to)
        return "tipo"

    if nxt == "categoria":
        ask_categoria_ou_origem(to, tx)
        return "categoria"

    if nxt == "pagamento":
        ask_pagamento(to)
        return "pagamento"

    if nxt == "data_competencia":
        ask_data(to)
        return "data"

    # valor / descricao são por texto
    ask_text_field(to, nxt, tx)
    return nxt

# ----------------------------
# Inbound: extrair texto ou seleção
# ----------------------------
def extract_inbound(msg: dict):
    # retorna (kind, value, title)
    # kind: "text" ou "choice"
    if msg.get("type") == "interactive" or msg.get("interactive"):
        inter = msg.get("interactive") or {}
        itype = inter.get("type")
        if itype == "button_reply":
            rep = inter.get("button_reply") or {}
            return ("choice", rep.get("id"), rep.get("title"))
        if itype == "list_reply":
            rep = inter.get("list_reply") or {}
            return ("choice", rep.get("id"), rep.get("title"))
        # fallback
        return ("text", "", "")
    # texto normal
    text = (msg.get("text") or {}).get("body", "")
    return ("text", (text or "").strip(), "")

# ----------------------------
# Webhook Meta
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

    # trava opcional: só você
    allowed = os.environ.get("ALLOWED_WA_NUMBER", "").strip()
    if allowed and from_number != allowed:
        return {"ok": True}

    kind, val, title = extract_inbound(msg)

    # comando global cancelar (texto)
    if kind == "text" and val.lower().strip() in ["cancelar", "cancela"]:
        PENDING.pop(from_number, None)
        send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
        return {"ok": True}

    pending = PENDING.get(from_number)

    # Se NÃO há wizard ativo: sempre inicia
    if not pending:
        tx = {
            "id": str(uuid.uuid4()),
            "timestamp": now_iso(),
            "tipo": None,
            "valor": None,
            "moeda": "BRL",
            "categoria": None,
            "descricao": None,
            "pagamento": None,
            "data_competencia": None,
            "confianca": 0.50,
            "confirmado": "não",
            "mensagem_original": (val if kind == "text" else "").strip(),
        }

        # Se a mensagem inicial tiver valor, aproveita (mas ainda pergunta tipo)
        if kind == "text":
            pv = parse_valor(val)
            if pv is not None:
                tx["valor"] = pv

        PENDING[from_number] = {"tx": tx, "await": "tipo", "stage": "wizard"}
        ask_tipo(from_number)
        return {"ok": True}

    tx = pending["tx"]
    await_field = pending.get("await")

    # -------------------------
    # CONFIRMAÇÃO
    # -------------------------
    if await_field == "confirm":
        if (kind == "choice" and val == "confirm_sim") or (kind == "text" and val.lower().strip() in ["sim", "ok", "confirmar"]):
            tx["confirmado"] = "sim"
            normalize_sign(tx)
            append_row(tx_to_row(tx))
            PENDING.pop(from_number, None)
            send_whatsapp_text(from_number, "Gravado na planilha. Mande qualquer mensagem para registrar outro.")
            return {"ok": True}

        if (kind == "choice" and val == "confirm_cancelar") or (kind == "text" and val.lower().strip() in ["nao", "não", "cancelar", "cancela"]):
            PENDING.pop(from_number, None)
            send_whatsapp_text(from_number, "Cancelado. Mande qualquer mensagem para começar de novo.")
            return {"ok": True}

        send_whatsapp_text(from_number, "Selecione SIM para gravar ou CANCELAR para descartar.")
        return {"ok": True}

    # -------------------------
    # TIPO (botões)
    # -------------------------
    if await_field == "tipo":
        if kind == "choice" and val in ["tipo_receita", "tipo_despesa"]:
            tx["tipo"] = "receita" if val == "tipo_receita" else "despesa"
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}

        send_whatsapp_text(from_number, "Use os botões para escolher: Receita ou Despesa.")
        ask_tipo(from_number)
        return {"ok": True}

    # -------------------------
    # CATEGORIA/ORIGEM (lista)
    # -------------------------
    if await_field == "categoria":
        if kind == "choice" and val:
            if tx.get("tipo") == "receita":
                # origem_...
                if val.startswith("origem_"):
                    tx["categoria"] = title or "Outros"
            else:
                # cat_...
                if val.startswith("cat_"):
                    tx["categoria"] = title or "Outros"

            # Se escolheu "Outros", peça texto específico
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

    # Categoria/Origem digitada quando escolheu "Outros"
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
    # DESCRIÇÃO (texto)
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
    # PAGAMENTO (lista)
    # -------------------------
    if await_field == "pagamento":
        if kind == "choice" and val and val.startswith("pay_"):
            tx["pagamento"] = title.lower().strip() if title else "desconhecido"
            pending["tx"] = tx
            pending["await"] = continue_wizard(from_number, tx)
            return {"ok": True}

        send_whatsapp_text(from_number, "Escolha uma opção na lista de pagamento.")
        ask_pagamento(from_number)
        return {"ok": True}

    # -------------------------
    # DATA (botões Hoje/Ontem/Outra)
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

            # outra -> pede texto
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

    # fallback: se algo ficou fora de sincronia, continua wizard
    pending["tx"] = tx
    pending["await"] = continue_wizard(from_number, tx)
    return {"ok": True}
