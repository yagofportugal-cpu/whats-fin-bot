import os
from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

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
    print("INCOMING:", body)
    return {"ok": True}
