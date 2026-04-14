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

# 👉 PUT YOUR BOT USERNAME HERE (WITHOUT @)
BOT_USERNAME = "purplmasterbot"

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

        chat_id = fix_chat_id(chat.get("id"))
        chat_type = chat.get("type")

        username = (user.get("username") or "").strip()
        first_name = (user.get("first_name") or "").strip()

        print("chat_id =", chat_id)
        print("text =", text)

        # =============================
        # REGISTER AFFILIATE
        # =============================
        if text.startswith("/register_affiliate"):

            if chat_type not in ["group", "supergroup"]:
                send_text_message(chat_id, "❌ Use this command inside a Telegram group")
                return {"ok": True}

            admin_ok, admin_msg = verify_or_bind_admin(username, user.get("id"), first_name)

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
                telegram_group_id=chat_id,
                telegram_group_title=chat.get("title"),
                created_by_telegram_user_id=user.get("id"),
            )

            send_text_message(chat_id, "✅ Affiliate registered")

            return {"ok": True}

        # =============================
        # 🤖 AI BOT RESPONSE (FIXED)
        # =============================

        entities = message.get("entities", [])
        bot_tagged = False

        for ent in entities:
            if ent.get("type") == "mention":
                offset = ent.get("offset", 0)
                length = ent.get("length", 0)
                mention_text = text[offset:offset+length]

                if BOT_USERNAME.lower() in mention_text.lower():
                    bot_tagged = True

        if bot_tagged:
            print("BOT TAG DETECTED")

            stats = get_today_stats()
            response = generate_smart_reply(text, stats)

            send_text_message(chat_id, response)

        return {"ok": True}

    except Exception as e:
        print("ERROR:", str(e))
        print(traceback.format_exc())
        return {"ok": False}


# =============================
# DB
# =============================

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL is missing")
    return psycopg.connect(DATABASE_URL)


def get_today_stats():
    with get_db_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= CURRENT_DATE
            """)
            leads = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) FROM conversions
                WHERE deposit_date >= CURRENT_DATE
            """)
            ftds = int(cur.fetchone()[0])

            return {"leads": leads, "ftds": ftds}


# =============================
# 🤖 SMART REPLY
# =============================

def generate_smart_reply(text, stats):

    leads = stats["leads"]
    ftds = stats["ftds"]
    cr = (ftds / leads * 100) if leads > 0 else 0

    text = text.lower()

    if "result" in text or "today" in text:
        return (
            f"Today looks active so far.\n"
            f"FTDs: {ftds} | Leads: {leads} | CR: {cr:.2f}%\n\n"
            f"If quality stays strong, there’s definitely room to push more volume."
        )

    if "ftd" in text:
        return f"Current FTD count today is {ftds}. Keep pushing 👍"

    if "push" in text or "scale" in text:
        return "Performance looks stable. If traffic quality holds, today is a good day to scale."

    return "Things are moving steadily today. Let’s keep momentum 🚀"


# =============================
# EXISTING FUNCTIONS
# =============================

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

    def get_val(prefix):
        for l in lines:
            if l.lower().startswith(prefix):
                return l.split(":", 1)[1].strip()
        return None

    return {
        "ok": True,
        "affiliate_name": get_val("name:"),
        "affiliate_email": get_val("email:"),
        "affiliate_hash": get_val("hash:")
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
