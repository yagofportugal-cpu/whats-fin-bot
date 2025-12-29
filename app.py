import os
import requests
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

GRAPH_VER = "v22.0"

def send_whatsapp_text(to: str, text: str):
    token = os.environ["WA_ACCESS_TOKEN"]
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]

    url = f"https://graph.facebook.com/{GRAPH_VER}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text}
    }

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=20
    )
    r.raise_for_status()
    return r.json()

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

    # quando não é mensagem (ex: status), ignora
    if not messages:
        return {"ok": True}

    msg = messages[0]
    from_number = msg.get("from")
    text = (msg.get("text") or {}).get("body", "")

    # Resposta automática
    send_whatsapp_text(from_number, f"Recebi: {text}")

    return {"ok": True}
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import datetime as dt
import uuid

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _sheets_service():
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def append_row(values):
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

@app.get("/test-sheets")
def test_sheets():
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    row = [
        str(uuid.uuid4()),
        now,
        "gasto",
        1.23,
        "BRL",
        "Teste",
        "linha teste",
        "desconhecido",
        dt.date.today().isoformat(),
        0.99,
        "sim",
        "teste-sheets"
    ]
    append_row(row)
    return {"ok": True}

