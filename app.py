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
