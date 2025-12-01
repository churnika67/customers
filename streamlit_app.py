import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from openai import OpenAI
import bcrypt

load_dotenv()

OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

QUERY_DEFAULT_LIMIT = 500
STATEMENT_TIMEOUT_MS = 15_000
LOCK_TIMEOUT_MS = 3_000
CONNECT_TIMEOUT_S = 5

# ---------- PAGE CONFIG & GLOBAL STYLES ----------

st.set_page_config(page_title="Aurora Query Studio", page_icon="🧭", layout="wide")

st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=SF+Pro+Display:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');

        /* ---------- GLOBAL ---------- */
        html, body, [class*="css"] {
            font-family: 'SF Pro Display', 'Inter', system-ui, -apple-system, BlinkMacSystemFont, sans-serif !important;
            background:
                radial-gradient(circle at top, #111827 0, #020617 55%, #020617 100%);
            color: #e5e7eb;
        }

        .block-container {
            padding: 1.4rem 2.4rem 3rem;
            max-width: 1360px;
        }

        header {visibility: hidden; height: 0px;}
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        a { text-decoration: none; }

        /* ---------- TOP BAR ---------- */
        .top-nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 13px;
            color: #9ca3af;
            margin-bottom: 0.8rem;
        }
        .top-nav-left {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 500;
        }
        .logo-orb {
            width: 18px;
            height: 18px;
            border-radius: 999px;
            background: radial-gradient(circle at 30% 0, #38bdf8, #1d4ed8 55%, #020617 100%);
            box-shadow: 0 0 14px rgba(56, 189, 248, 0.7);
        }
        .top-nav-right {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .status-pill {
            padding: 3px 10px;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.4);
            background: rgba(15, 23, 42, 0.85);
            backdrop-filter: blur(18px);
            -webkit-backdrop-filter: blur(18px);
            font-size: 10px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #e5e7eb;
        }
        .status-text {
            font-size: 12px;
            color: #9ca3af;
        }

        /* ---------- HERO ---------- */
        .hero-kicker {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 4px 11px;
            border-radius: 999px;
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.35);
            font-size: 11px;
            color: #cbd5f5;
            margin-bottom: 0.55rem;
        }
        .hero-kicker-dot {
            width: 7px;
            height: 7px;
            border-radius: 999px;
            background: #22c55e;
            box-shadow: 0 0 10px rgba(34, 197, 94, 0.8);
        }

        .brand-title {
            font-size: 34px;
            font-weight: 650;
            letter-spacing: -0.045em;
            background: linear-gradient(120deg, #f9fafb, #cbd5f5, #93c5fd);
            background-size: 200% 200%;
            -webkit-background-clip: text;
            color: transparent;
            animation: auroraTitle 12s ease-in-out infinite;
        }

        .brand-subtitle {
            color: #9ca3af;
            font-size: 14.5px;
            max-width: 640px;
            margin-top: 0.35rem;
        }

        @keyframes auroraTitle {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }

        /* ---------- WORKSPACE CARD ---------- */
        .workspace {
            margin-top: 1.6rem;
            padding: 20px 22px 22px;
            border-radius: 26px;
            background:
                linear-gradient(135deg, rgba(148, 163, 184, 0.16), rgba(15, 23, 42, 0.85)),
                radial-gradient(circle at top left, rgba(148, 163, 184, 0.25), transparent 55%);
            background-blend-mode: soft-light, normal;
            border: 1px solid rgba(148, 163, 184, 0.28);
            box-shadow:
                0 40px 80px rgba(15, 23, 42, 0.9),
                0 0 0 1px rgba(15, 23, 42, 0.9) inset;
            backdrop-filter: blur(28px);
            -webkit-backdrop-filter: blur(28px);
        }

        .section-title {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 0.1rem;
            color: #e5e7eb;
        }

        .section-caption {
            font-size: 12.5px;
            color: #6b7280;
            margin-bottom: 0.7rem;
        }

        .side-card {
            background: rgba(15, 23, 42, 0.92);
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 16px;
            padding: 10px 13px;
            margin-bottom: 0.7rem;
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.75);
            transition: box-shadow 160ms ease-out, transform 160ms ease-out, border-color 160ms ease-out;
        }

        .side-card:hover {
            box-shadow: 0 22px 52px rgba(15, 23, 42, 0.95);
            transform: translateY(-2px);
            border-color: rgba(129, 140, 248, 0.8);
        }

        .metric-label {
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #9ca3af;
        }

        .metric-value {
            font-size: 19px;
            font-weight: 600;
            color: #e5e7eb;
        }

        /* ---------- TEXT INPUTS ---------- */
        .stTextArea textarea {
            border-radius: 16px !important;
            border: 1px solid rgba(31, 41, 55, 0.85) !important;
            background: rgba(15, 23, 42, 0.92) !important;
            color: #e5e7eb !important;
            font-size: 14.5px !important;
            transition: box-shadow 160ms ease-out, border-color 160ms ease-out, background 160ms ease-out, transform 120ms ease-out;
        }

        .stTextArea textarea::placeholder {
            color: #6b7280 !important;
        }

        .stTextInput input {
            border-radius: 15px !important;
            border: 1px solid rgba(31, 41, 55, 0.9) !important;
            background: rgba(15, 23, 42, 0.96) !important;
            color: #e5e7eb !important;
            font-size: 14px !important;
            transition: box-shadow 160ms ease-out, border-color 160ms ease-out, background 160ms ease-out, transform 120ms ease-out;
        }

        .stTextInput input::placeholder {
            color: #6b7280 !important;
        }

        .stTextArea textarea:focus,
        .stTextInput input:focus {
            outline: none !important;
            border-color: #38bdf8 !important;
            box-shadow: 0 0 0 1px #38bdf8, 0 16px 40px rgba(8, 47, 73, 0.9) !important;
            background: rgba(15, 23, 42, 1) !important;
            transform: translateY(-1px);
        }

        /* ---------- BUTTONS ---------- */
        .btn-primary button {
            background: linear-gradient(135deg, #38bdf8, #2563eb);
            border: none;
            color: #f9fafb;
            font-weight: 600;
            border-radius: 999px;
            padding: 0.42rem 1.5rem;
            box-shadow: 0 15px 34px rgba(37, 99, 235, 0.7);
            transition: transform 140ms ease-out, box-shadow 140ms ease-out, filter 140ms ease-out;
        }
        .btn-primary button:hover {
            filter: brightness(1.05);
            transform: translateY(-1px);
            box-shadow: 0 20px 44px rgba(37, 99, 235, 0.9);
        }
        .btn-primary button:active {
            transform: translateY(0);
            box-shadow: 0 10px 24px rgba(37, 99, 235, 0.6);
        }

        .btn-secondary button {
            background: rgba(15, 23, 42, 0.96);
            color: #e5e7eb;
            border-radius: 999px;
            border: 1px solid rgba(75, 85, 99, 0.9);
            font-weight: 500;
            padding: 0.42rem 1.3rem;
            transition: background 140ms ease-out, transform 140ms ease-out, box-shadow 140ms ease-out, border-color 140ms ease-out;
        }
        .btn-secondary button:hover {
            background: rgba(15, 23, 42, 1);
            transform: translateY(-1px);
            border-color: rgba(148, 163, 184, 1);
            box-shadow: 0 14px 32px rgba(15, 23, 42, 0.9);
        }

        /* ---------- SIDEBAR ---------- */
        [data-testid="stSidebar"] {
            background: radial-gradient(circle at top, #020617 0, #020617 60%, #020617 100%) !important;
            border-right: 1px solid #020617;
        }
        [data-testid="stSidebar"] * {
            color: #e5e7eb !important;
        }
        [data-testid="stSidebar"] .stButton button {
            width: 100%;
            border-radius: 999px;
            border: 1px solid rgba(75, 85, 99, 0.9);
            background: rgba(15, 23, 42, 0.96);
            color: #e5e7eb;
            font-weight: 500;
            transition: background 140ms ease-out, box-shadow 140ms ease-out, transform 140ms ease-out, border-color 140ms ease-out;
        }
        [data-testid="stSidebar"] .stButton button:hover {
            background: rgba(15, 23, 42, 1);
            border-color: #38bdf8;
            box-shadow: 0 10px 26px rgba(15, 23, 42, 0.95);
            transform: translateY(-1px);
        }

        /* ---------- CODE BLOCKS ---------- */
        .stCode pre {
            border-radius: 12px !important;
            border: 1px solid rgba(148, 163, 184, 0.45) !important;
            background: #020617 !important;
            color: #e5e7eb !important;
            font-size: 13px !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- SCHEMA CONTEXT FOR GPT ----------

DATABASE_SCHEMA = """
Database Schema:

LOOKUP / DIMENSIONS:
- Region(RegionID SERIAL PRIMARY KEY, Region TEXT UNIQUE)
- Country(CountryID SERIAL PRIMARY KEY, Country TEXT UNIQUE, RegionID INTEGER FK -> Region)
- ProductCategory(ProductCategoryID SERIAL PRIMARY KEY, ProductCategory TEXT UNIQUE, ProductCategoryDescription TEXT)
- Product(ProductID SERIAL PRIMARY KEY, ProductName TEXT UNIQUE, ProductUnitPrice REAL, ProductCategoryID INTEGER FK -> ProductCategory)

CORE TABLES:
- Customer(CustomerID SERIAL PRIMARY KEY, FirstName TEXT, LastName TEXT, Address TEXT, City TEXT, CountryID INTEGER FK -> Country)
- OrderDetail(OrderID SERIAL PRIMARY KEY, CustomerID INTEGER FK -> Customer, ProductID INTEGER FK -> Product, OrderDate DATE, QuantityOrdered INTEGER)

Helpful joins:
- Country joins Region via Country.RegionID
- Customer joins Country via Customer.CountryID
- OrderDetail joins Customer and Product via their IDs
- Product joins ProductCategory via ProductCategoryID

Common calculations:
- Total revenue: SUM(QuantityOrdered * ProductUnitPrice)
- Order counts: COUNT(DISTINCT OrderID) or COUNT(*)
- Date filters: OrderDate is a DATE column
"""

# ---------- AUTH / LOGIN ----------

def login_screen():
    """Display login screen and authenticate user."""

    st.markdown(
        """
        <div class="top-nav">
            <div class="top-nav-left">
                <div class="logo-orb"></div>
                <div>Aurora</div>
            </div>
            <div class="top-nav-right">
                <span class="status-pill">Private</span>
                <span class="status-text">Secure SQL workspace</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="hero-kicker">
            <span class="hero-kicker-dot"></span>
            Password-protected session
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='brand-title'>Aurora Query Studio</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='brand-subtitle'>Sign in to turn natural language into safe, optimized SQL. Your session stays local to this browser.</p>",
        unsafe_allow_html=True,
    )

    password = st.text_input("Workspace password", type="password", key="login_password")
    col1, col2 = st.columns([1, 3])
    with col1:
        st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
        login_btn = st.button("🔓 Enter", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode("utf-8"), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful. Loading workspace…")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")

    st.caption("Passwords are verified with bcrypt. Close the tab or click Logout to end the session.")


def require_login():
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

# ---------- DB HELPERS ----------

@st.cache_resource
def get_db_url():
    POSTGRES_USERNAME = st.secrets["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD = st.secrets["POSTGRES_PASSWORD"]
    POSTGRES_SERVER = st.secrets["POSTGRES_SERVER"]
    POSTGRES_DATABASE = st.secrets["POSTGRES_DATABASE"]
    return f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"

DATABASE_URL = get_db_url()

@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=CONNECT_TIMEOUT_S)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s;", (STATEMENT_TIMEOUT_MS,))
            cur.execute("SET lock_timeout = %s;", (LOCK_TIMEOUT_MS,))
            cur.execute(
                "SET idle_in_transaction_session_timeout = %s;",
                (LOCK_TIMEOUT_MS * 10,),
            )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def _ensure_limit(sql, default_limit=QUERY_DEFAULT_LIMIT):
    pattern = re.compile(r"\\blimit\\b", re.IGNORECASE)
    if pattern.search(sql):
        return sql.strip()
    stripped = sql.strip().rstrip(";")
    return f"{stripped}\\nLIMIT {default_limit};"

def run_query(sql):
    conn = get_db_connection()
    if conn is None:
        return None
    safe_sql = _ensure_limit(sql)
    if safe_sql != sql:
        st.info(f"Added LIMIT {QUERY_DEFAULT_LIMIT} to keep the query responsive.")
    try:
        df = pd.read_sql_query(safe_sql, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# ---------- OPENAI HELPERS ----------

@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)

def extract_sql_from_response(response_text):
    clean_sql = re.sub(
        r"^```sql\\s*|\\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE
    ).strip()
    return clean_sql

def generate_sql_with_gpt(user_question):
    client = get_openai_client()
    prompt = f"""You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

{DATABASE_SCHEMA}

User Question: {user_question}

Requirements:
1. Generate ONLY the SQL query that I can directly use. No other response.
2. Use proper JOINs to get descriptive names from lookup tables
3. Use appropriate aggregations (COUNT, AVG, SUM, etc.) when needed
4. Add LIMIT clauses for queries that might return many rows (default LIMIT 100)
5. Use proper date/time functions for TIMESTAMP or DATE columns
6. Make sure the query is syntactically correct for PostgreSQL
7. Add helpful column aliases using AS

Generate the SQL query:"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a PostgreSQL expert who generates accurate SQL queries based on natural language questions.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=1000,
        )
        sql_query = extract_sql_from_response(response.choices[0].message.content)
        return sql_query
    except Exception as e:
        st.error(f"Error calling OpenAI API: {e}")
        return None

# ---------- MAIN APP ----------

def main():
    require_login()

    # Top bar
    st.markdown(
        """
        <div class="top-nav">
            <div class="top-nav-left">
                <div class="logo-orb"></div>
                <div>Aurora Query Studio</div>
            </div>
            <div class="top-nav-right">
                <span class="status-pill">Beta</span>
                <span class="status-text">PostgreSQL · Guarded by timeouts</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Hero
    st.markdown(
        """
        <div class="hero-kicker">
            <span class="hero-kicker-dot"></span>
            Natural language → SQL, in one workspace
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='brand-title'>Query Studio</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='brand-subtitle'>Describe the insight you want. Aurora turns it into audited SQL you can inspect, refine, and run with built-in guardrails.</p>",
        unsafe_allow_html=True,
    )

    # Sidebar
    with st.sidebar:
        st.header("🧭 Navigator")
        st.caption("Quick prompts to get started")
        st.markdown(
            """
            • Top 10 products by revenue  
            • Order volume trend by month  
            • Customers by region & country  
            • Gross margin by product category  
            • Repeat buyers by city  
            """
        )
        st.divider()
        st.info("Tip: keep scope tight (e.g., top 20, last 90 days) for faster results.")
        if st.button("Logout"):
            st.session_state.logged_in = False
            st.rerun()

    # State
    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "current_question" not in st.session_state:
        st.session_state.current_question = None

    # Workspace
    st.markdown("<div class='workspace'>", unsafe_allow_html=True)

    left, right = st.columns([1.9, 1], gap="large")

    with left:
        st.markdown("<div class='section-title'>Your question</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='section-caption'>Write in plain language. Mention products, regions, time ranges, and metrics.</div>",
            unsafe_allow_html=True,
        )

        user_question = st.text_area(
            label="",
            height=120,
            placeholder="e.g., Show revenue and order count by product category for the last 90 days.",
        )

        action_cols = st.columns([1, 1, 3])
        with action_cols[0]:
            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            generate_button = st.button(
                "⚡ Generate SQL", use_container_width=True, key="gen_sql_btn"
            )
            st.markdown("</div>", unsafe_allow_html=True)
        with action_cols[1]:
            st.markdown("<div class='btn-secondary'>", unsafe_allow_html=True)
            clear_button = st.button(
                "🧹 Clear", use_container_width=True, key="clear_btn"
            )
            st.markdown("</div>", unsafe_allow_html=True)

        if clear_button:
            st.session_state.query_history = []
            st.session_state.generated_sql = None
            st.session_state.current_question = None

        if generate_button and user_question:
            question = user_question.strip()
            if st.session_state.current_question != question:
                st.session_state.generated_sql = None
                st.session_state.current_question = None

            with st.spinner("🧠 Composing SQL…"):
                sql_query = generate_sql_with_gpt(question)
                if sql_query:
                    st.session_state.generated_sql = sql_query
                    st.session_state.current_question = question

        if st.session_state.generated_sql:
            st.markdown("---")
            st.markdown("<div class='section-title'>Generated SQL</div>", unsafe_allow_html=True)
            st.caption(f"Question: {st.session_state.current_question}")

            edited_sql = st.text_area(
                "Review and edit before execution:",
                value=st.session_state.generated_sql,
                height=220,
            )

            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            run_button = st.button(
                "▶️ Run query", use_container_width=True, key="run_btn"
            )
            st.markdown("</div>", unsafe_allow_html=True)

            if run_button:
                with st.spinner("Running against warehouse…"):
                    df = run_query(edited_sql)
                    if df is not None:
                        st.session_state.query_history.append(
                            {
                                "question": st.session_state.current_question,
                                "sql": edited_sql,
                                "rows": len(df),
                            }
                        )
                        st.success(f"✅ Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)

    with right:
        st.markdown("<div class='section-title'>Workspace stats</div>", unsafe_allow_html=True)
        stats_cols = st.columns(2)
        with stats_cols[0]:
            st.markdown(
                f"<div class='side-card'><div class='metric-label'>Default limit</div><div class='metric-value'>{QUERY_DEFAULT_LIMIT}</div></div>",
                unsafe_allow_html=True,
            )
        with stats_cols[1]:
            st.markdown(
                f"<div class='side-card'><div class='metric-label'>Statement timeout</div><div class='metric-value'>{int(STATEMENT_TIMEOUT_MS/1000)}s</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div class='section-title' style='margin-top:0.6rem;'>Schema primer</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='section-caption'>Use these anchors when you phrase your question.</div>",
            unsafe_allow_html=True,
        )
        st.code(
            "Region → Country → Customer → OrderDetail\nProductCategory → Product → OrderDetail",
            language="text",
        )

        if st.session_state.query_history:
            st.markdown("<div class='section-title' style='margin-top:0.6rem;'>Recent queries</div>", unsafe_allow_html=True)
            for idx, item in enumerate(reversed(st.session_state.query_history[-3:])):
                st.markdown(
                    f"**Q{len(st.session_state.query_history)-idx}:** {item['question']}"
                )
                st.code(item["sql"], language="sql")
                st.caption(f"Rows: {item['rows']}")

    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
