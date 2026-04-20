from fastapi import FastAPI, Request
import os
import urllib.request
import urllib.parse
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
GETLINKED_BASE_URL = os.getenv("GETLINKED_BASE_URL", "").strip()
GETLINKED_API_KEY = os.getenv("GETLINKED_API_KEY", "").strip()

BOT_USERNAME = "purplmasterbot"

# New review group
FTD_REVIEW_GROUP_ID = int(os.getenv("FTD_REVIEW_GROUP_ID", "-1003991625278"))

# IMPORTANT:
# Put here the EXACT sale status text you want to send when ignoring an FTD
# Example: "Ignored"
IGNORED_SALE_STATUS_VALUE = os.getenv("IGNORED_SALE_STATUS_VALUE", "").strip()


@app.on_event("startup")
def startup():
    init_review_actions_table()


@app.get("/")
async def root():
    return {"message": "Bot is running"}


@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()

        # =============================
        # CALLBACK QUERY (INLINE BUTTONS)
        # =============================
        callback_query = data.get("callback_query")
        if callback_query:
            handle_callback_query(callback_query)
            return {"ok": True}

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

        text_lower = text.lower()

        # =============================
        # /update COMMAND
        # =============================
        if text_lower.startswith("/update"):
            stats = get_group_stats(chat_id)
            response = build_full_report(stats)
            send_text_message(chat_id, response)
            return {"ok": True}

        # =============================
        # /review <newDepositID>
        # TEST A REVIEW MESSAGE WITH BUTTONS
        # =============================
        if text_lower.startswith("/review"):
            parts = text.split()
            if len(parts) < 2:
                send_text_message(chat_id, "Usage: /review <newDepositID>")
                return {"ok": True}

            new_deposit_id = parts[1].strip()
            review_text = build_review_message(new_deposit_id)
            send_text_message(
                FTD_REVIEW_GROUP_ID,
                review_text,
                reply_markup=build_inline_keyboard(new_deposit_id)
            )
            return {"ok": True}

        # =============================
        # REGISTER AFFILIATE
        # =============================
        if text.startswith("/register_affiliate"):

            if chat_type not in ["group", "supergroup"]:
                send_text_message(chat_id, "❌ Use inside a group")
                return {"ok": True}

            admin_ok, admin_msg = verify_or_bind_admin(username, user.get("id"), first_name)

            if not admin_ok:
                send_text_message(chat_id, admin_msg)
                return {"ok": True}

            parsed = parse_register_affiliate_command(text)

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
        # BOT TAG DETECTION
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
            stats = get_group_stats(chat_id)

            if any(word in text_lower for word in ["update", "report", "status", "stats"]):
                response = build_full_report(stats)
            else:
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


def init_review_actions_table():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_ftd_review_actions (
                    id SERIAL PRIMARY KEY,
                    new_deposit_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    telegram_chat_id BIGINT,
                    telegram_message_id BIGINT,
                    telegram_user_id BIGINT,
                    telegram_username TEXT,
                    getlinked_response JSONB,
                    status TEXT NOT NULL DEFAULT 'done',
                    error_message TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_ftd_review_actions_new_deposit_id
                ON telegram_ftd_review_actions (new_deposit_id)
            """)
            conn.commit()


def log_review_action(
    new_deposit_id,
    action_type,
    telegram_chat_id=None,
    telegram_message_id=None,
    telegram_user_id=None,
    telegram_username=None,
    getlinked_response=None,
    status="done",
    error_message=None,
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO telegram_ftd_review_actions (
                    new_deposit_id,
                    action_type,
                    telegram_chat_id,
                    telegram_message_id,
                    telegram_user_id,
                    telegram_username,
                    getlinked_response,
                    status,
                    error_message
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                str(new_deposit_id),
                action_type,
                telegram_chat_id,
                telegram_message_id,
                telegram_user_id,
                telegram_username,
                json.dumps(getlinked_response) if getlinked_response is not None else None,
                status,
                error_message,
            ))
            conn.commit()


def get_group_stats(chat_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT affiliate_name
                FROM affiliate_group_mappings
                WHERE telegram_group_id = %s
                LIMIT 1
            """, (chat_id,))
            row = cur.fetchone()

            if not row:
                return {"leads": 0, "ftds": 0, "affiliate": "Unknown"}

            affiliate = row[0]

            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= CURRENT_DATE
                AND LOWER(TRIM(affiliate_name)) = LOWER(TRIM(%s))
            """, (affiliate,))
            leads = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) FROM conversions
                WHERE deposit_date >= CURRENT_DATE
                AND LOWER(TRIM(affiliate_name)) = LOWER(TRIM(%s))
            """, (affiliate,))
            ftds = int(cur.fetchone()[0])

            return {
                "leads": leads,
                "ftds": ftds,
                "affiliate": affiliate
            }


def get_review_item(new_deposit_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    deposit_id,
                    affiliate_name,
                    affiliate_email,
                    campaign_name,
                    signup_date,
                    deposit_date,
                    raw_json
                FROM conversions
                WHERE deposit_id = %s
                LIMIT 1
            """, (str(new_deposit_id),))
            row = cur.fetchone()

            if not row:
                return None

            raw_json = row[6]
            if isinstance(raw_json, str):
                try:
                    raw_json = json.loads(raw_json)
                except Exception:
                    raw_json = {}

            return {
                "deposit_id": row[0],
                "affiliate_name": row[1],
                "affiliate_email": row[2],
                "campaign_name": row[3],
                "signup_date": row[4],
                "deposit_date": row[5],
                "raw_json": raw_json or {},
            }


# =============================
# GETLINKED API
# =============================

def process_under_review_deposit(
    new_deposit_id,
    affiliate_hash=None,
    sale_status=None,
    stop_sale_status_update=None,
):
    if not GETLINKED_BASE_URL:
        raise Exception("GETLINKED_BASE_URL is missing")
    if not GETLINKED_API_KEY:
        raise Exception("GETLINKED_API_KEY is missing")

    url = f"{GETLINKED_BASE_URL.rstrip('/')}/api/v2/new-deposits/process-stuck-deposits/{urllib.parse.quote(str(new_deposit_id))}"

    payload = {
        "newDepositID": str(new_deposit_id),
    }

    if affiliate_hash:
        payload["affiliateHash"] = affiliate_hash

    if sale_status:
        payload["saleStatus"] = sale_status

    if stop_sale_status_update is not None:
        payload["stopSaleStatusUpdate"] = str(stop_sale_status_update)

    encoded = urllib.parse.urlencode(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Api-Key": GETLINKED_API_KEY,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as response:
        body = response.read().decode("utf-8")
        try:
            return json.loads(body)
        except Exception:
            return {"raw": body}


# =============================
# TELEGRAM CALLBACKS
# =============================

def build_inline_keyboard(new_deposit_id):
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Approve", "callback_data": f"approve_ftd:{new_deposit_id}"},
                {"text": "❌ Ignore", "callback_data": f"ignore_ftd:{new_deposit_id}"}
            ],
            [
                {"text": "🔄 Refresh", "callback_data": f"refresh_ftd:{new_deposit_id}"}
            ]
        ]
    }


def answer_callback_query(callback_query_id, text, show_alert=False):
    if not BOT_TOKEN:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"

    data = json.dumps({
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": show_alert
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    urllib.request.urlopen(req)


def edit_message_text(chat_id, message_id, text, reply_markup=None):
    if not BOT_TOKEN:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"

    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    urllib.request.urlopen(req)


def normalize_telegram_user(user):
    username = (user.get("username") or "").strip()
    if username:
        return f"@{username}"

    first_name = (user.get("first_name") or "").strip()
    last_name = (user.get("last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip()

    return full_name or str(user.get("id") or "unknown")


def build_review_message(new_deposit_id):
    item = get_review_item(new_deposit_id)

    if not item:
        return f"FTD {new_deposit_id}\n\nCould not find this item in local DB yet."

    raw = item["raw_json"] or {}

    sale_status = raw.get("saleStatusMapped") or raw.get("saleStatus") or "N/A"
    affiliate = item.get("affiliate_name") or "Unknown"
    email = item.get("affiliate_email") or raw.get("email") or "N/A"
    campaign_name = item.get("campaign_name") or raw.get("campaignName") or "N/A"
    signup_date = item.get("signup_date") or raw.get("signupDate") or "N/A"
    affiliate_hash = raw.get("affiliateHash") or "N/A"

    text = "🧾 FTD Review Item\n\n"
    text += f"New Deposit ID: {new_deposit_id}\n"
    text += f"Affiliate: {affiliate}\n"
    text += f"Email: {email}\n"
    text += f"Sale Status: {sale_status}\n"
    text += f"Signup Date: {signup_date}\n"
    text += f"Offer / Campaign: {campaign_name}\n"
    text += f"Affiliate Hash: {affiliate_hash}\n"

    sources = raw.get("_underReviewSources") or []
    if isinstance(sources, list) and sources:
        text += "\nReview Sources:\n"
        for src in sources:
            statuses = src.get("statuses", [])
            review_rule = src.get("reviewRule")
            text += f"- statuses: {statuses} | reviewRule: {review_rule}\n"

    return text


def handle_callback_query(callback_query):
    callback_data = (callback_query.get("data") or "").strip()
    callback_query_id = callback_query.get("id")
    from_user = callback_query.get("from", {})
    message = callback_query.get("message", {}) or {}

    chat = message.get("chat", {}) or {}
    chat_id = fix_chat_id(chat.get("id"))
    message_id = message.get("message_id")

    if ":" not in callback_data:
        answer_callback_query(callback_query_id, "Invalid action", True)
        return

    action, new_deposit_id = callback_data.split(":", 1)
    actor = normalize_telegram_user(from_user)

    try:
        if action == "refresh_ftd":
            refreshed_text = build_review_message(new_deposit_id)
            edit_message_text(
                chat_id,
                message_id,
                refreshed_text,
                reply_markup=build_inline_keyboard(new_deposit_id)
            )
            answer_callback_query(callback_query_id, "Refreshed")
            return

        item = get_review_item(new_deposit_id)
        raw = item["raw_json"] if item else {}
        affiliate_hash = raw.get("affiliateHash")

        if action == "approve_ftd":
            response = process_under_review_deposit(
                new_deposit_id=new_deposit_id,
                affiliate_hash=affiliate_hash
            )

            updated_text = build_review_message(new_deposit_id)
            updated_text += f"\n✅ ACTION: APPROVED\nBy: {actor}"

            edit_message_text(
                chat_id,
                message_id,
                updated_text,
                reply_markup={
                    "inline_keyboard": [
                        [{"text": "🔄 Refresh", "callback_data": f"refresh_ftd:{new_deposit_id}"}]
                    ]
                }
            )

            log_review_action(
                new_deposit_id=new_deposit_id,
                action_type="approve",
                telegram_chat_id=chat_id,
                telegram_message_id=message_id,
                telegram_user_id=from_user.get("id"),
                telegram_username=actor,
                getlinked_response=response,
                status="done"
            )

            answer_callback_query(callback_query_id, "FTD approved")
            return

        if action == "ignore_ftd":
            if not IGNORED_SALE_STATUS_VALUE:
                answer_callback_query(callback_query_id, "IGNORED_SALE_STATUS_VALUE missing", True)
                return

            response = process_under_review_deposit(
                new_deposit_id=new_deposit_id,
                affiliate_hash=affiliate_hash,
                sale_status=IGNORED_SALE_STATUS_VALUE,
                stop_sale_status_update=1
            )

            updated_text = build_review_message(new_deposit_id)
            updated_text += f"\n❌ ACTION: IGNORED\nBy: {actor}\nSale Status Sent: {IGNORED_SALE_STATUS_VALUE}"

            edit_message_text(
                chat_id,
                message_id,
                updated_text,
                reply_markup={
                    "inline_keyboard": [
                        [{"text": "🔄 Refresh", "callback_data": f"refresh_ftd:{new_deposit_id}"}]
                    ]
                }
            )

            log_review_action(
                new_deposit_id=new_deposit_id,
                action_type="ignore",
                telegram_chat_id=chat_id,
                telegram_message_id=message_id,
                telegram_user_id=from_user.get("id"),
                telegram_username=actor,
                getlinked_response=response,
                status="done"
            )

            answer_callback_query(callback_query_id, "FTD ignored")
            return

        answer_callback_query(callback_query_id, "Unknown action", True)

    except Exception as e:
        log_review_action(
            new_deposit_id=new_deposit_id,
            action_type=action,
            telegram_chat_id=chat_id,
            telegram_message_id=message_id,
            telegram_user_id=from_user.get("id"),
            telegram_username=actor,
            status="failed",
            error_message=str(e)
        )
        answer_callback_query(callback_query_id, str(e), True)


# =============================
# FULL REPORT
# =============================

def latest_reportable_conversions_cte(alias="latest_conversions"):
    return f"""
        WITH normalized_conversions AS (
            SELECT
                c.*,
                COALESCE(
                    NULLIF(c.raw_json->>'depositID', ''),
                    NULLIF(c.raw_json->>'brokerAccountDepositID', ''),
                    NULLIF(c.raw_json->>'brokerAccountDepositId', ''),
                    NULLIF(c.deposit_id, ''),
                    LOWER(TRIM(COALESCE(c.email, c.raw_json->>'email', ''))) || '|' ||
                    COALESCE(
                        ((c.deposit_date AT TIME ZONE 'Asia/Jerusalem')::date)::text,
                        ((c.updated_at AT TIME ZONE 'Asia/Jerusalem')::date)::text,
                        'no_date'
                    )
                ) AS verification_key,
                COALESCE(
                    NULLIF(c.raw_json->>'depositStatus', '')::int,
                    NULLIF(c.raw_json->>'status', '')::int,
                    NULLIF(c.raw_json->>'reviewStatus', '')::int,
                    NULLIF(c.raw_json->>'review_status', '')::int,
                    NULLIF(c.raw_json->>'conversionStatus', '')::int,
                    CASE
                        WHEN COALESCE(c.raw_json->>'_pulledFrom', '') = 'under-review' THEN 0
                        ELSE 1
                    END
                ) AS resolved_status
            FROM conversions c
            WHERE COALESCE(TRIM(c.email), TRIM(c.raw_json->>'email'), '') <> ''
        ),
        ranked_conversions AS (
            SELECT
                nc.*,
                ROW_NUMBER() OVER (
                    PARTITION BY nc.verification_key
                    ORDER BY
                        CASE
                            WHEN nc.resolved_status = 1 THEN 1
                            WHEN nc.resolved_status = 8 THEN 2
                            WHEN nc.resolved_status = 0 THEN 3
                            ELSE 4
                        END,
                        nc.updated_at DESC NULLS LAST,
                        nc.deposit_date DESC NULLS LAST
                ) AS rn
            FROM normalized_conversions nc
        ),
        {alias} AS (
            SELECT *
            FROM ranked_conversions
            WHERE rn = 1
              AND resolved_status IN (1, 8)
        )
    """
    
def build_full_report(stats):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= CURRENT_DATE
            """)
            leads = int(cur.fetchone()[0])

            cur.execute(f"""
                {latest_reportable_conversions_cte("latest_conversions")}
                SELECT COUNT(*) FROM latest_conversions
                WHERE deposit_date >= CURRENT_DATE
            """)
            ftds = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= DATE_TRUNC('day', NOW() - INTERVAL '1 day')
                AND signup_date <= NOW() - INTERVAL '1 day'
            """)
            y_leads = int(cur.fetchone()[0])

            cur.execute(f"""
                {latest_reportable_conversions_cte("latest_conversions")}
                SELECT COUNT(*) FROM latest_conversions
                WHERE deposit_date >= DATE_TRUNC('day', NOW() - INTERVAL '1 day')
                AND deposit_date <= NOW() - INTERVAL '1 day'
            """)
            y_ftds = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= DATE_TRUNC('day', NOW() - INTERVAL '7 day')
                AND signup_date <= NOW() - INTERVAL '7 day'
            """)
            w_leads = int(cur.fetchone()[0])

            cur.execute(f"""
                {latest_reportable_conversions_cte("latest_conversions")}
                SELECT COUNT(*) FROM latest_conversions
                WHERE deposit_date >= DATE_TRUNC('day', NOW() - INTERVAL '7 day')
                AND deposit_date <= NOW() - INTERVAL '7 day'
            """)
            w_ftds = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) FROM leads
                WHERE signup_date >= NOW() - INTERVAL '1 hour'
            """)
            h_leads = int(cur.fetchone()[0])

            cur.execute(f"""
                {latest_reportable_conversions_cte("latest_conversions")}
                SELECT COUNT(*) FROM latest_conversions
                WHERE deposit_date >= NOW() - INTERVAL '1 hour'
            """)
            h_ftds = int(cur.fetchone()[0])

            cur.execute(f"""
                {latest_reportable_conversions_cte("latest_conversions")}
                ,
                today_leads AS (
                    SELECT affiliate_name, COUNT(*) AS leads
                    FROM leads
                    WHERE signup_date >= CURRENT_DATE
                    GROUP BY affiliate_name
                ),
                today_ftds AS (
                    SELECT affiliate_name, COUNT(*) AS ftds
                    FROM latest_conversions
                    WHERE deposit_date >= CURRENT_DATE
                    GROUP BY affiliate_name
                )
                SELECT
                    COALESCE(l.affiliate_name, f.affiliate_name),
                    COALESCE(l.leads, 0),
                    COALESCE(f.ftds, 0)
                FROM today_leads l
                FULL OUTER JOIN today_ftds f
                ON LOWER(TRIM(l.affiliate_name)) = LOWER(TRIM(f.affiliate_name))
                ORDER BY 3 DESC, 2 DESC
                LIMIT 3
            """)
            top = cur.fetchall()

    cr = (ftds / leads * 100) if leads > 0 else 0

    message = f"⏱ Hourly Report\n\n"

    message += f"📊 Today so far:\n"
    message += f"Leads: {leads}\n"
    message += f"FTDs: {ftds}\n"
    message += f"CR: {cr:.2f}%\n\n"

    message += f"⚡ Last hour pace:\n"
    message += f"Leads: {h_leads}\n"
    message += f"FTDs: {h_ftds}\n"
    message += f"CR: {(h_ftds / h_leads * 100) if h_leads else 0:.2f}%\n\n"

    message += f"📊 Yesterday same time:\n"
    message += f"Leads: {y_leads}\n"
    message += f"FTDs: {y_ftds}\n"
    message += f"CR: {(y_ftds / y_leads * 100) if y_leads else 0:.2f}%\n\n"

    message += f"📊 Last week same time:\n"
    message += f"Leads: {w_leads}\n"
    message += f"FTDs: {w_ftds}\n"
    message += f"CR: {(w_ftds / w_leads * 100) if w_leads else 0:.2f}%\n\n"

    message += f"🔥 Top 3 Affiliates Today:\n"

    if not top:
        message += "No data yet\n"
    else:
        for i, row in enumerate(top, 1):
            name = row[0] or "Unknown"
            l = int(row[1])
            f = int(row[2])
            message += f"{i}. {name} — {f} FTD / {l} Leads ({(f / l * 100) if l else 0:.2f}%)\n"

    return message

# =============================
# SMART REPLY
# =============================

def generate_smart_reply(text, stats):
    leads = stats["leads"]
    ftds = stats["ftds"]
    affiliate = stats["affiliate"]

    cr = (ftds / leads * 100) if leads > 0 else 0
    text = text.lower()

    if "result" in text or "today" in text:
        return (
            f"{affiliate} performance today:\n"
            f"FTDs: {ftds} | Leads: {leads} | CR: {cr:.2f}%\n\n"
            f"If quality holds, there’s room to push more volume."
        )

    if "ftd" in text:
        return f"{affiliate} currently has {ftds} FTDs today 👍"

    if "push" in text or "scale" in text:
        return f"{affiliate} looks stable today. If quality is good, scaling could work."

    return f"{affiliate} is moving steadily today. Let’s keep momentum 🚀"


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


def send_text_message(chat_id, text, reply_markup=None):
    try:
        if not BOT_TOKEN:
            return

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

        payload = {
            "chat_id": chat_id,
            "text": text
        }

        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        urllib.request.urlopen(req)

    except Exception as e:
        print("Telegram error:", str(e))
