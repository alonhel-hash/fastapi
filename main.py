from fastapi import FastAPI, Request
import os
import urllib.request
import json
import traceback
import psycopg

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
            print("No message found in update")
            return {"ok": True}

        text = (message.get("text") or "").strip()
        chat = message.get("chat", {})
        user = message.get("from", {})

        chat_id = chat.get("id")
        chat_title = chat.get("title") or chat.get("first_name") or "Unknown Chat"
        chat_type = chat.get("type")

        telegram_user_id = user.get("id")
        username = (user.get("username") or "").strip()
        first_name = (user.get("first_name") or "").strip()

        print("chat_id =", chat_id)
        print("chat_title =", chat_title)
        print("chat_type =", chat_type)
        print("telegram_user_id =", telegram_user_id)
        print("username =", username)
        print("text =", text)

        if text.startswith("/register_affiliate"):
            print("STEP 1: command matched")
            send_text_message(chat_id, "STEP 1 OK - command received")

            if chat_type not in ["group", "supergroup"]:
                print("STEP 2: wrong chat type")
                send_text_message(chat_id, "This command must be used inside a Telegram group.")
                return {"ok": True}

            print("STEP 2: checking admin")
            admin_ok, admin_msg = verify_or_bind_admin(username, telegram_user_id, first_name)
            print("admin_ok =", admin_ok, "admin_msg =", admin_msg)

            if not admin_ok:
                send_text_message(chat_id, admin_msg)
                return {"ok": True}

            parsed = parse_register_affiliate_command(text)
            print("parsed =", parsed)

            if not parsed["ok"]:
                send_text_message(chat_id, parsed["error"])
                return {"ok": True}

            affiliate_name = parsed["affiliate_name"]
            affiliate_email = parsed["affiliate_email"]
            affiliate_hash = parsed["affiliate_hash"]

            print("STEP 3: saving mapping")
            save_affiliate_group_mapping(
                affiliate_name=affiliate_name,
                affiliate_email=affiliate_email,
                affiliate_hash=affiliate_hash,
                telegram_group_id=chat_id,
                telegram_group_title=chat_title,
                created_by_telegram_user_id=telegram_user_id,
            )

            print("STEP 4: sending success")
            send_text_message(
                chat_id,
                "✅ Affiliate registered successfully\n\n"
                f"Affiliate: {affiliate_name}\n"
                f"Email: {affiliate_email}\n"
                f"Hash: {affiliate_hash}\n"
                f"Group: {chat_title}"
            )

        return {"ok": True}

    except Exception as e:
        print("ERROR INSIDE /webhook:", str(e))
        print(traceback.format_exc())
        return {"ok": False, "error": str(e)}

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL is missing")
    return psycopg.connect(DATABASE_URL)

def verify_or_bind_admin(username, telegram_user_id, first_name):
    if not username:
        return False, "❌ You need a Telegram username to use this command."

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, allowed_username, telegram_user_id, is_active, is_verified
                FROM telegram_admin_users
                WHERE allowed_username = %s
                """,
                (username,)
            )
            row = cur.fetchone()

            if not row:
                return False, f"❌ You are not an allowed admin. username={username}"

            admin_id, allowed_username, saved_telegram_user_id, is_active, is_verified = row

            if not is_active:
                return False, "❌ Your admin access is not active."

            if saved_telegram_user_id is None:
                cur.execute(
                    """
                    UPDATE telegram_admin_users
                    SET telegram_user_id = %s,
                        first_name = %s,
                        is_verified = TRUE,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (telegram_user_id, first_name, admin_id)
                )
                conn.commit()
                return True, "Admin verified and bound successfully."

            if int(saved_telegram_user_id) != int(telegram_user_id):
                return False, "❌ Your Telegram user ID does not match the approved admin account."

            return True, "Admin verified."

def parse_register_affiliate_command(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    if len(lines) < 4:
        return {
            "ok": False,
            "error": (
                "❌ Wrong format.\n\n"
                "Use this exact format:\n"
                "/register_affiliate\n"
                "name: AlphaMedia\n"
                "email: alpha@company.com\n"
                "hash: HASH123"
            )
        }

    name_line = None
    email_line = None
    hash_line = None

    for line in lines[1:]:
        lower = line.lower()
        if lower.startswith("name:"):
            name_line = line
        elif lower.startswith("email:"):
            email_line = line
        elif lower.startswith("hash:"):
            hash_line = line

    if not name_line or not email_line or not hash_line:
        return {
            "ok": False,
            "error": (
                "❌ Missing one of the required fields.\n\n"
                "Required fields:\n"
                "name:\n"
                "email:\n"
                "hash:"
            )
        }

    affiliate_name = name_line.split(":", 1)[1].strip()
    affiliate_email = email_line.split(":", 1)[1].strip()
    affiliate_hash = hash_line.split(":", 1)[1].strip()

    if not affiliate_name or not affiliate_email or not affiliate_hash:
        return {
            "ok": False,
            "error": "❌ Name, email, and hash must all have values."
        }

    return {
        "ok": True,
        "affiliate_name": affiliate_name,
        "affiliate_email": affiliate_email,
        "affiliate_hash": affiliate_hash,
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
                SELECT id
                FROM affiliate_group_mappings
                WHERE affiliate_hash = %s
                  AND telegram_group_id = %s
                  AND is_active = TRUE
                """,
                (affiliate_hash, telegram_group_id)
            )
            existing = cur.fetchone()

            if existing:
                mapping_id = existing[0]
                cur.execute(
                    """
                    UPDATE affiliate_group_mappings
                    SET affiliate_name = %s,
                        affiliate_email = %s,
                        telegram_group_title = %s,
                        created_by_telegram_user_id = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        affiliate_name,
                        affiliate_email,
                        telegram_group_title,
                        created_by_telegram_user_id,
                        mapping_id,
                    )
                )
            else:
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
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
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
            print("BOT_TOKEN is missing")
            return

        print("Sending Telegram message to chat_id =", chat_id)

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
            body = response.read().decode("utf-8")
            print("Telegram sendMessage response:", body)

    except Exception as e:
        print("ERROR INSIDE send_text_message:", str(e))
        print(traceback.format_exc())
