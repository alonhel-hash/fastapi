from fastapi import FastAPI, Request
import os
import urllib.request
import json
import traceback
import psycopg

def fix_chat_id(chat_id):
    chat_id = int(chat_id)
    if chat_id > 0:
        chat_id = -chat_id
    return chat_id

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

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
            return {"ok": True}

        text = (message.get("text") or "").strip()
        chat = message.get("chat", {})
        user = message.get("from", {})

        chat_id = chat.get("id")
        chat_id = fix_chat_id(chat_id)   # ✅ FIX HERE

        chat_title = chat.get("title") or chat.get("first_name") or "Unknown Chat"
        chat_type = chat.get("type")

        telegram_user_id = user.get("id")
        username = (user.get("username") or "").strip()
        first_name = (user.get("first_name") or "").strip()

        print("chat_id =", chat_id)
        print("chat_type =", chat_type)
        print("username =", username)
        print("text =", text)

        if text.startswith("/register_affiliate"):

            if chat_type not in ["group", "supergroup"]:
                send_text_message(chat_id, "❌ Use this command inside a Telegram group")
                return {"ok": True}

            admin_ok, admin_msg = verify_or_bind_admin(username, telegram_user_id, first_name)

            if not admin_ok:
                send_text_message(chat_id, admin_msg)
                return {"ok": True}

            parsed = parse_register_affiliate_command(text)

            if not parsed["ok"]:
                send_text_message(chat_id, parsed["error"])
                return {"ok": True}

            save_affiliate_group_mapping(
                affiliate_name=parsed["affiliate_name"],
                affiliate_email=parsed["affiliate_email"],
                affiliate_hash=parsed["affiliate_hash"],
                telegram_group_id=chat_id,   # now always negative ✅
                telegram_group_title=chat_title,
                created_by_telegram_user_id=telegram_user_id,
            )

            send_text_message(
                chat_id,
                "✅ Affiliate registered successfully\n\n"
                f"Affiliate: {parsed['affiliate_name']}\n"
                f"Email: {parsed['affiliate_email']}\n"
                f"Hash: {parsed['affiliate_hash']}\n"
                f"Group: {chat_title}\n"
                f"Chat ID: {chat_id}"
            )

        return {"ok": True}

    except Exception as e:
        print("ERROR:", str(e))
        print(traceback.format_exc())
        return {"ok": False}


def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL is missing")
    return psycopg.connect(DATABASE_URL)


def verify_or_bind_admin(username, telegram_user_id, first_name):
    if not username:
        return False, "❌ You need a Telegram username"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, telegram_user_id, is_active FROM telegram_admin_users WHERE allowed_username = %s",
                (username,)
            )
            row = cur.fetchone()

            if not row:
                return False, "❌ Not allowed admin"

            admin_id, saved_id, is_active = row

            if not is_active:
                return False, "❌ Admin not active"

            if saved_id is None:
                cur.execute(
                    "UPDATE telegram_admin_users SET telegram_user_id=%s WHERE id=%s",
                    (telegram_user_id, admin_id)
                )
                conn.commit()
                return True, "OK"

            if int(saved_id) != int(telegram_user_id):
                return False, "❌ Wrong Telegram user"

            return True, "OK"


def parse_register_affiliate_command(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    if len(lines) < 4:
        return {"ok": False, "error": "❌ Wrong format"}

    def get_val(prefix):
        for l in lines:
            if l.lower().startswith(prefix):
                return l.split(":", 1)[1].strip()
        return None

    name = get_val("name:")
    email = get_val("email:")
    hash_ = get_val("hash:")

    if not name or not email or not hash_:
        return {"ok": False, "error": "❌ Missing fields"}

    return {
        "ok": True,
        "affiliate_name": name,
        "affiliate_email": email,
        "affiliate_hash": hash_,
    }


def save_affiliate_group_mapping(
    affiliate_name,
    affiliate_email,
    affiliate_hash,
    telegram_group_id,
    telegram_group_title,
    created_by_telegram_user_id,
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO affiliate_group_mappings (
                    affiliate_name,
                    affiliate_email,
                    affiliate_hash,
                    telegram_group_id,
                    telegram_group_title,
                    created_by_telegram_user_id,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,TRUE,NOW(),NOW())
                ON CONFLICT DO NOTHING
                """,
                (
                    affiliate_name,
                    affiliate_email,
                    affiliate_hash,
                    telegram_group_id,
                    telegram_group_title,
                    created_by_telegram_user_id,
                )
            )
            conn.commit()


def send_text_message(chat_id, text):
    try:
        if not BOT_TOKEN:
            return

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

        data = json.dumps({
            "chat_id": chat_id,
            "text": text
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        urllib.request.urlopen(req)

    except Exception as e:
        print("Telegram error:", str(e))
