from fastapi import FastAPI, Request
import os
import urllib.request
import json
import traceback

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

@app.get("/")
async def root():
    return {"message": "Bot is running"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        print("Received update:", data)

        message = data.get("message")
        if not message:
            print("No message found in update")
            return {"ok": True}

        chat_id = message["chat"]["id"]
        text = message.get("text", "")

        print("chat_id =", chat_id)
        print("text =", text)

        if text and not text.startswith("/"):
            send_text_message(chat_id, "I received your message")
        else:
            print("Skipping reply")

        return {"ok": True}

    except Exception as e:
        print("ERROR INSIDE /webhook:", str(e))
        print(traceback.format_exc())
        return {"ok": False, "error": str(e)}

def send_text_message(chat_id, text):
    try:
        if not BOT_TOKEN:
            print("BOT_TOKEN is missing")
            return

        print("BOT_TOKEN exists, sending message now")

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

    except Exception as e:
        print("ERROR INSIDE send_text_message:", str(e))
        print(traceback.format_exc())

@app.get("/test-ftd")
async def test_ftd(
    affiliate_name: str = "AlphaMedia",
    total_ftds: int = 12,
    total_leads: int = 184
):
    message_text = format_ftd_message(affiliate_name, total_ftds, total_leads)

    test_group_id = -1003752696322
    send_text_message(test_group_id, message_text)

    return {"ok": True, "message_sent": message_text}

def format_ftd_message(affiliate_name, total_ftds, total_leads):
    conversion_rate = 0
    if total_leads > 0:
        conversion_rate = (total_ftds / total_leads) * 100

    return (
        f"🔥 You have a new FTD\n\n"
        f"Affiliate: {affiliate_name}\n"
        f"Overall results until now:\n"
        f"FTDs: {total_ftds}\n"
        f"Leads: {total_leads}\n"
        f"Conversion Rate: {conversion_rate:.2f}%"
    )
