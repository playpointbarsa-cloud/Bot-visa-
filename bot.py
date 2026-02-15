import os
import re
import io
import psycopg
from psycopg.rows import tuple_row
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)
from telegram.ext import AIORateLimiter

NUM_RE = re.compile(r"\b\d{6,}\b")  # أي رقم 6+ أرقام

MAX_INLINE = 80          # لو النتائج <= كده، نبعتها رسالة
MAX_RESULTS = 200000     # حد أقصى للنتائج المسموح إرسالها (ملف)

def get_conn():
    # Railway Postgres عادة بيضيف DATABASE_URL تلقائيًا
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DATABASE_URL env var")
    return psycopg.connect(dsn, autocommit=True, row_factory=tuple_row)

def db_init():
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS numbers (
                    num TEXT PRIMARY KEY,
                    prefix6 TEXT GENERATED ALWAYS AS (left(num, 6)) STORED
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_numbers_prefix6 ON numbers(prefix6);
            """)

def db_insert_many(nums: list[str]) -> int:
    if not nums:
        return 0
    # إزالة مكررات داخل الملف لتقليل الضغط
    nums = list(dict.fromkeys(nums))

    inserted = 0
    with get_conn() as con:
        with con.cursor() as cur:
            # إدخال Bulk على شكل VALUES متعددة (Chunks)
            # ON CONFLICT DO NOTHING عشان المكرر يتجاهل
            chunk_size = 5000
            for i in range(0, len(nums), chunk_size):
                chunk = nums[i:i+chunk_size]
                # بنبني VALUES بسرعة
                placeholders = ",".join(["(%s)"] * len(chunk))
                query = f"""
                    INSERT INTO numbers(num)
                    VALUES {placeholders}
                    ON CONFLICT (num) DO NOTHING
                """
                cur.execute(query, chunk)
                # rowcount في INSERT مع ON CONFLICT بيدي عدد المُضاف فعلاً
                inserted += cur.rowcount if cur.rowcount is not None else 0
    return inserted

def db_find(prefix6: str, limit: int = MAX_RESULTS) -> list[str]:
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT num FROM numbers WHERE prefix6 = %s LIMIT %s",
                (prefix6, limit)
            )
            return [r[0] for r in cur.fetchall()]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ ابعتلي ملف .txt فيه أرقام، وأنا هحفظها.\n"
        "🔎 للبحث ابعت /find 123456 أو ابعت 123456 لوحدها.\n"
        "📄 لو النتائج كتير هتوصلك كملف."
    )

async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit() or len(context.args[0]) != 6:
        await update.message.reply_text("استخدم: /find 123456")
        return
    await send_search_results(update, context.args[0])

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.isdigit() and len(text) == 6:
        await send_search_results(update, text)

async def send_search_results(update: Update, prefix6: str):
    results = db_find(prefix6)

    if not results:
        await update.message.reply_text("❌ لا توجد نتائج.")
        return

    count = len(results)
    if count <= MAX_INLINE:
        msg = "\n".join(results)
        await update.message.reply_text(f"✅ النتائج ({count}):\n{msg}")
        return

    # لو كتير → ملف
    content = "\n".join(results).encode("utf-8")
    bio = io.BytesIO(content)
    bio.name = f"results_{prefix6}_{count}.txt"
    await update.message.reply_text(f"✅ عدد النتائج: {count} — هبعتهم كملف.")
    await update.message.reply_document(document=bio)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc:
        return

    if doc.file_size and doc.file_size > 25 * 1024 * 1024:
        await update.message.reply_text("⚠️ الملف كبير. خلّيه أقل من 25MB.")
        return

    tg_file = await doc.get_file()
    data = await tg_file.download_as_bytearray()
    text = data.decode("utf-8", errors="ignore")

    nums = NUM_RE.findall(text)
    if not nums:
        await update.message.reply_text("❌ ملقتش أرقام في الملف.")
        return

    await update.message.reply_text(f"⏳ لقيت {len(nums)} رقم… بحفظهم الآن.")
    inserted = db_insert_many(nums)
    await update.message.reply_text(f"✅ تم حفظ {inserted} رقم جديد (وتم تجاهل المكرر).")

def main():
    db_init()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing BOT_TOKEN env var")

    port = int(os.getenv("PORT", "8000"))
    # Railway domain: هتاخدها من Settings > Domains
    railway_domain = os.getenv("RAILWAY_STATIC_URL")
    if not railway_domain:
        raise RuntimeError("Missing RAILWAY_STATIC_URL env var")

    app = Application.builder().token(token).rate_limiter(AIORateLimiter()).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        webhook_url=f"https://{railway_domain}"
    )

if __name__ == "__main__":
    main()
