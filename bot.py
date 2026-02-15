import os
import io
import psycopg
from psycopg.rows import tuple_row
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)

MAX_INLINE = 60          # لو النتائج قليلة يرسلها رسالة
MAX_RESULTS = 50000      # حد أقصى للنتائج
MIN_QUERY_LEN = 3        # أقل طول بحث (عشان ما يجيب كل حاجة)

def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg.connect(dsn, autocommit=True, row_factory=tuple_row)

def db_init():
    with get_conn() as con:
        with con.cursor() as cur:
            # تسريع بحث LIKE %...% عبر trigram
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lines (
                    line TEXT PRIMARY KEY
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_lines_trgm
                ON lines USING GIN (line gin_trgm_ops);
            """)

def db_insert_lines(raw_lines: list[str]) -> int:
    if not raw_lines:
        return 0

    # تنظيف + إزالة تكرار داخل الملف
    cleaned = []
    seen = set()
    for ln in raw_lines:
        ln = ln.strip()
        if not ln:
            continue
        if ln in seen:
            continue
        seen.add(ln)
        cleaned.append(ln)

    if not cleaned:
        return 0

    inserted = 0
    with get_conn() as con:
        with con.cursor() as cur:
            q = """
                INSERT INTO lines(line)
                VALUES (%s)
                ON CONFLICT (line) DO NOTHING
            """
            # إدخال ثابت وآمن (مش الأسرع، لكنه ما ينهارش)
            for ln in cleaned:
                cur.execute(q, (ln,))
                inserted += cur.rowcount or 0

    return inserted

def db_search_any(query: str, limit: int = MAX_RESULTS) -> list[str]:
    # بحث substring
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT line FROM lines WHERE line LIKE %s LIMIT %s",
                (f"%{query}%", limit)
            )
            return [r[0] for r in cur.fetchall()]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ ابعت ملف .txt (كل سطر بيانات)، وأنا هحفظه.\n"
        "🔎 ابعت أي رقم/جزء رقم للبحث (مش شرط 6 أرقام).\n"
        "مثال: اكتب 9721 أو /find 9721\n"
        "📄 لو النتائج كتير هتجيلك كملف."
    )

async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("استخدم: /find 9721")
        return
    q = context.args[0].strip()
    await send_results(update, q)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = (update.message.text or "").strip()
    if q and not q.startswith("/"):
        await send_results(update, q)

async def send_results(update: Update, query: str):
    # نخليها أرقام فقط (لو عايز تسمح بحروف، شيل الشرط ده)
    if not query.isdigit():
        await update.message.reply_text("ابعت أرقام فقط للبحث.")
        return

    if len(query) < MIN_QUERY_LEN:
        await update.message.reply_text(f"اكتب على الأقل {MIN_QUERY_LEN} أرقام للبحث.")
        return

    results = db_search_any(query)
    if not results:
        await update.message.reply_text("❌ لا توجد نتائج.")
        return

    # لو عايز تضيف |555|55 بعد كل سطر فك التعليق:
    # results = [f"{line}|555|55" for line in results]

    count = len(results)
    if count <= MAX_INLINE:
        await update.message.reply_text("\n".join(results))
        return

    bio = io.BytesIO("\n".join(results).encode("utf-8"))
    bio.name = f"results_{query}_{count}.txt"
    await update.message.reply_text(f"✅ عدد النتائج: {count} — هبعتهم كملف.")
    await update.message.reply_document(document=bio)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc:
        return

    if doc.file_size and doc.file_size > 25 * 1024 * 1024:
        await update.message.reply_text("⚠️ الملف كبير. خلّيه أقل من 25MB.")
        return

    f = await doc.get_file()
    data = await f.download_as_bytearray()
    text = data.decode("utf-8", errors="ignore")

    lines = text.splitlines()
    await update.message.reply_text(f"⏳ جاري الحفظ… عدد السطور: {len(lines)}")
    inserted = db_insert_lines(lines)
    await update.message.reply_text(f"✅ تم حفظ {inserted} سطر جديد (المكرر تجاهل).")

def main():
    db_init()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing BOT_TOKEN")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
