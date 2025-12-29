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

# pendência por número: { from_number: {"tx": {...}, "stage": "collect"|"confirm", "await": "campo"} }
PENDING = {}

REQUIRED_FIELDS = ["tipo", "valor", "categoria", "descricao", "pagamento", "data_competencia"]

# ----------------------------
# WhatsApp send
# ----------------------------
def send_whatsapp_text(to: str, text: str):
    token = os.environ["WA_ACCESS_TOKEN"]
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    url = f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text[:3800]},
    }

    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ----------------------------
# Sheets append
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
# Helpers: parsing/validation
# ----------------------------
def _now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _today_iso_date():
    return dt.date.today().isoformat()

def parse_valor(text: str):
    t = text.lower()
    m = re.search(r"(\d{1,6}(?:[.,]\d{2})?)", t)
    if not m:
        return None
    # 1.234,56 / 1234,56 / 35,90 / 35.90 (MVP)
    raw = m.group(1)
    # remove separador de milhar simples
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def parse_tipo(text: str):
    t = text.lower()
    if any(k in t for k in ["receita", "recebi", "entrada", "salário", "salario", "ganhei", "caiu", "pix recebido"]):
        return "receita"
    if any(k in t for k in ["gasto", "paguei", "comprei", "debitei", "gastei", "pago"]):
        return "gasto"
    return None

def parse_pagamento(text: str):
    t = text.lower()
    if "pix" in t:
        return "pix"
    if any(k in t for k in ["debito", "débito"]):
        return "débito"
    if any(k in t for k in ["credito", "crédito", "cartao", "cartão"]):
        return "crédito"
    if any(k in t for k in ["dinheiro", "cash"]):
        return "dinheiro"
    return None

def parse_categoria(text: str):
    t = text.lower()
    if any(k in t for k in ["mercado", "supermerc", "padaria", "hortifruti"]):
        return "Mercado"
    if any(k in t for k in ["uber", "99", "ônibus", "onibus", "metro", "gasolina", "combust"]):
        return "Transporte"
    if any(k in t for k in ["aluguel", "condominio", "condomínio", "luz", "energia", "agua", "água", "internet"]):
        return "Moradia"
    if any(k in t for k in ["ifood", "restaurante", "lanche", "pizza", "bar"]):
        return "Alimentação"
    if any(k in t for k in ["netflix", "spotify", "assinatura", "prime"]):
        return "Assinaturas"
    return None

def parse_data_competencia(text: str):
    t = text.lower()

    if "hoje" in t:
        return dt.date.today().isoformat()
    if "ontem" in t:
        return (dt.date.today() - dt.timedelta(days=1)).isoformat()

    # dd/mm, dd-mm, dd/mm/yyyy, dd-mm-yy
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

def normalize_tipo(answer: str):
    a = answer.lower().strip()
    if a in ["gasto", "despesa", "saída", "saida"]:
        return "gasto"
    if a in ["receita", "entrada"]:
        return "receita"
    return None

def normalize_pagamento(answer: str):
    a = answer.lower().strip()
    if a in ["pix"]:
        return "pix"
    if a in ["debito", "débito"]:
        return "débito"
    if a in ["credito", "crédito", "cartao", "cartão"]:
        return "crédito"
    if a in ["dinheiro", "cash"]:
        return "dinheiro"
    return None

def normalize_data(answer: str):
    a = answer.lower().strip()
    if a == "hoje":
        return dt.date.today().isoformat()
    if a == "ontem":
        return (dt.date.today() - dt.timedelta(days=1)).isoformat()
    return parse_data_competencia(a)

def missing_fields(tx: dict):
    missing = []
    for f in REQUIRED_FIELDS:
        v = tx.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            missing.append(f)
    return missing

def next_question(field: str):
    if field == "tipo":
        return "Qual o **TIPO**? Responda: gasto ou receita."
    if field == "valor":
        return "Qual o **VALOR**? Ex: 35,90"
    if field == "categoria":
        return "Qual a **CATEGORIA**? Ex: Mercado / Transporte / Moradia / etc."
    if field == "descricao":
        return "Qual a **DESCRIÇÃO** (curta)? Ex: pão e leite"
    if field == "pagamento":
        return "Qual o **PAGAMENTO**? Responda: pix / débito / crédito / dinheiro"
    if field == "data_competencia":
        return "Qual a **DATA**? Responda: hoje / ontem / ou dd/mm (ex: 29/12)"
    return "Preciso de mais informação."

def format_confirm(tx: dict):
    v = f"{tx['valor']:.2f}".replace(".", ",") if isinstance(tx.get("valor"), (int, float)) else "N/A"
    return (
        "Confirma o lançamento?\n"
        f"- tipo: {tx.get('tipo')}\n"
        f"- valor: {v} {tx.get('moeda','BRL')}\n"
        f"- categoria: {tx.get('categoria')}\n"
        f"- descricao: {tx.get('descricao')}\n"
        f"- pagamento: {tx.get('pagamento')}\n"
        f"- data_competencia: {tx.get('data_competencia')}\n\n"
        "Responda: SIM / CANCELAR"
    )

def tx_to_row(tx: dict):
    # colunas: id, timestamp, tipo, valor, moeda, categoria, descricao, pagamento, data_competencia, confianca, confirmado, mensagem_original
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

def init_tx(original_text: str):
    # tenta inferir tudo, mas NÃO assume obrigatórios ausentes
    tipo = parse_tipo(original_text)
    valor = parse_valor(original_text)
    categoria = parse_categoria(original_text)
    pagamento = parse_pagamento(original_text)
    data_comp = parse_data_competencia(original_text)

    # descricao: se a mensagem for muito pobre (só número), força perguntar depois
    cleaned = original_text.strip()
    descr = cleaned
    if re.fullmatch(r"\s*\d{1,6}(?:[.,]\d{2})?\s*", cleaned):
        descr = None

    # confiança simples (MVP): quanto mais inferiu, maior
    inferred = sum(1 for x in [tipo, valor, categoria, pagamento, data_comp, descr] if x is not None)
    confianca = min(0.35 + inferred * 0.10, 0.95)

    return {
        "id": str(uuid.uuid4()),
        "timestamp": _now_iso(),
        "tipo": tipo,
        "valor": valor,
        "moeda": "BRL",
        "categoria": categoria,
        "descricao": descr,
        "pagamento": pagamento,
        "data_competencia": data_comp,
        "confianca": float(confianca),
        "confirmado": "não",
        "mensagem_original": original_text.strip(),
    }

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
    text = (msg.get("text") or {}).get("body", "").strip()
    t = text.lower().strip()

    # trava opcional: só você
    allowed = os.environ.get("ALLOWED_WA_NUMBER", "").strip()
    if allowed and from_number != allowed:
        return {"ok": True}

    # comandos globais
    if t in ["cancelar", "cancela"]:
        PENDING.pop(from_number, None)
        send_whatsapp_text(from_number, "Cancelado.")
        return {"ok": True}

    pending = PENDING.get(from_number)

    # ---------
    # Se está em confirmação
    # ---------
    if pending and pending.get("stage") == "confirm":
        if t in ["sim", "confirmar", "ok"]:
            tx = pending["tx"]
            tx["confirmado"] = "sim"
            PENDING.pop(from_number, None)

            append_row(tx_to_row(tx))
            send_whatsapp_text(from_number, "Gravado na planilha.")
            return {"ok": True}

        # qualquer coisa diferente de SIM/CANCELAR
        send_whatsapp_text(from_number, "Responda: SIM para gravar ou CANCELAR para descartar.")
        return {"ok": True}

    # ---------
    # Se está coletando um campo faltante
    # ---------
    if pending and pending.get("stage") == "collect":
        field = pending.get("await")
        tx = pending["tx"]

        # aplica resposta no campo esperado, com validação
        if field == "tipo":
            v = normalize_tipo(text)
            if not v:
                send_whatsapp_text(from_number, "Tipo inválido. Responda: gasto ou receita.")
                return {"ok": True}
            tx["tipo"] = v

        elif field == "valor":
            v = parse_valor(text)
            if v is None:
                send_whatsapp_text(from_number, "Valor inválido. Ex: 35,90")
                return {"ok": True}
            tx["valor"] = v

        elif field == "categoria":
            # aceita texto livre
            tx["categoria"] = text.strip()

        elif field == "descricao":
            tx["descricao"] = text.strip()

        elif field == "pagamento":
            v = normalize_pagamento(text)
            if not v:
                send_whatsapp_text(from_number, "Pagamento inválido. Responda: pix / débito / crédito / dinheiro")
                return {"ok": True}
            tx["pagamento"] = v

        elif field == "data_competencia":
            v = normalize_data(text)
            if not v:
                send_whatsapp_text(from_number, "Data inválida. Use: hoje / ontem / ou dd/mm (ex: 29/12)")
                return {"ok": True}
            tx["data_competencia"] = v

        # verifica próximos faltantes
        miss = missing_fields(tx)
        if miss:
            next_f = miss[0]
            pending["tx"] = tx
            pending["await"] = next_f
            send_whatsapp_text(from_number, next_question(next_f))
            return {"ok": True}

        # tudo preenchido -> pedir confirmação
        pending["tx"] = tx
        pending["stage"] = "confirm"
        pending["await"] = None
        send_whatsapp_text(from_number, format_confirm(tx))
        return {"ok": True}

    # ---------
    # Novo lançamento
    # ---------
    tx = init_tx(text)

    miss = missing_fields(tx)
    if miss:
        # inicia coleta do primeiro faltante
        PENDING[from_number] = {"tx": tx, "stage": "collect", "await": miss[0]}
        send_whatsapp_text(from_number, next_question(miss[0]))
        return {"ok": True}

    # se já veio completo, pede confirmação direto
    PENDING[from_number] = {"tx": tx, "stage": "confirm", "await": None}
    send_whatsapp_text(from_number, format_confirm(tx))
    return {"ok": True}
