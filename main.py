from fastapi import FastAPI, Request
import os
import urllib.request
import urllib.parse
import json

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")

@app.get("/")
async def root():
    return {"message": "Bot is running"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    print("Received update:", data)

    message = data.get("message")
    if message:
        chat_id = message["chat"]["id"]
        text = message.get("text", "")

        if text and not text.startswith("/"):
            send_text_message(chat_id, "I received your message")

    return {"ok": True}

def send_text_message(chat_id, text):
    if not BOT_TOKEN:
        print("BOT_TOKEN is missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(req) as response:
        print("Telegram sendMessage response:", response.read().decode("utf-8"))
