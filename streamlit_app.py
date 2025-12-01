import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from openai import OpenAI
import bcrypt

load_dotenv()  # reads variables from a .env file and sets them in os.environ

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
            background: #f5f5f7;
            color: #111827;
        }

        .block-container {
            padding: 1.8rem 2.5rem 3.2rem;
            max-width: 1320px;
        }

        a { text-decoration: none; }

        /* ---------- TOP NAV (APPLE-LIKE) ---------- */
        .top-nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 13px;
            color: #4b5563;
            margin-bottom: 1.4rem;
        }
        .top-nav-left {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 500;
        }
        .logo-dot {
            width: 18px;
            height: 18px;
            border-radius: 999px;
            background: radial-gradient(circle at 30% 20%, #60a5fa, #6366f1);
        }
        .top-nav-right {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .status-pill {
            padding: 4px 10px;
            border-radius: 999px;
            background: #e5f0ff;
            color: #1d4ed8;
            font-size: 11px;
            font-weight: 600;
            letter-spacing: .06em;
            text-transform: uppercase;
        }
        .status-text {
            color: #6b7280;
            font-size: 12px;
        }

        /* ---------- HERO ---------- */
        .hero-kicker {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 4px 10px;
            border-radius: 999px;
            background: #e5e7eb;
            font-size: 11px;
            color: #4b5563;
            margin-bottom: .55rem;
        }
        .hero-kicker-dot {
            width: 6px;
            height: 6px;
            border-radius: 999px;
            background: #10b981;
        }

        .brand-title {
            font-size: 36px;
            font-weight: 650;
            letter-spacing: -0.045em;
            color: #111827;
        }

        .brand-subtitle {
            color: #4b5563;
            font-size: 15px;
            max-width: 640px;
            margin-top: 0.35rem;
        }

        /* ---------- MAIN SHELL ---------- */
        .app-shell {
            margin-top: 1.9rem;
            border-radius: 32px;
            padding: 1px;
            background: linear-gradient(135deg, rgba(209, 213, 219, 0.9), rgba(148, 163, 184, 0.9));
        }

        .app-shell-inner {
            border-radius: 30px;
            background: #ffffff;
            padding: 24px 26px 26px;
            box-shadow: 0 28px 60px rgba(15, 23, 42, 0.12);
        }

        .section-title {
            font-size: 15px;
            font-weight: 600;
            margin-bottom: 0.1rem;
            color: #111827;
        }

        .section-caption {
            font-size: 13px;
            color: #6b7280;
            margin-bottom: 0.75rem;
        }

        .side-card {
            background: #f9fafb;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 12px 14px;
            margin-bottom: 0.7rem;
            transition: box-shadow 160ms ease-out, transform 160ms ease-out;
        }

        .side-card:hover {
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
            transform: translateY(-1px);
        }

        .metric-label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #9ca3af;
        }

        .metric-value {
            font-size: 20px;
            font-weight: 600;
            color: #111827;
        }

        /* ---------- TEXT INPUTS ---------- */
        .stTextArea textarea {
            border-radius: 16px !important;
            border: 1px solid #d1d5db !important;
            background: #f9fafb !important;
            color: #111827 !important;
            font-size: 15px !important;
            transition: box-shadow 160ms ease-out, border-color 160ms ease-out, background 160ms ease-out;
        }

        .stTextArea textarea::placeholder {
            color: #9ca3af !important;
        }

        .stTextInput input {
            border-radius: 14px !important;
            border: 1px solid #d1d5db !important;
            background: #f9fafb !important;
            color: #111827 !important;
            transition: box-shadow 160ms ease-out, border-color 160ms ease-out, background 160ms ease-out;
        }

        .stTextInput input::placeholder {
            color: #9ca3af !important;
        }

        .stTextArea textarea:focus,
        .stTextInput input:focus {
            outline: none !important;
            border-color: #0071e3 !important;
            box-shadow: 0 0 0 1px #0071e3, 0 10px 25px rgba(15, 23, 42, 0.12) !important;
            background: #ffffff !important;
        }

        /* ---------- BUTTONS ---------- */
        .btn-primary button {
            background: #0071e3;
            border: none;
            color: #ffffff;
            font-weight: 600;
            border-radius: 999px;
            padding: 0.45rem 1.35rem;
            box-shadow: 0 12px 24px rgba(0, 113, 227, 0.30);
            transition: transform 140ms ease-out, box-shadow 140ms ease-out, background 140ms ease-out;
        }
        .btn-primary button:hover {
            background: #005bb5;
            transform: translateY(-1px);
            box-shadow: 0 16px 30px rgba(0, 113, 227, 0.35);
        }
        .btn-primary button:active {
            transform: translateY(0);
            box-shadow: 0 8px 16px rgba(0, 113, 227, 0.25);
        }

        .btn-secondary button {
            background: #f3f4f6;
            color: #111827;
            border-radius: 999px;
            border: 1px solid #d1d5db;
            font-weight: 500;
            transition: background 140ms ease-out, transform 140ms ease-out, box-shadow 140ms ease-out;
        }
        .btn-secondary button:hover {
            background: #e5e7eb;
            transform: translateY(-1px);
            box-shadow: 0 10px 24px rgba(148, 163, 184, 0.4);
        }

        /* ---------- SIDEBAR ---------- */
        [data-testid="stSidebar"] {
            background: #f5f5f7 !important;
            border-right: 1px solid #e5e7eb;
        }
        [data-testid="stSidebar"] * {
            color: #111827 !important;
        }
        [data-testid="stSidebar"] .stButton button {
            width: 100%;
            border-radius: 999px;
            border: 1px solid #d1d5db;
            background: #ffffff;
            color: #111827;
            font-weight: 500;
            transition: background 140ms ease-out, box-shadow 140ms ease-out, transform 140ms ease-out;
        }
        [data-testid="stSidebar"] .stButton button:hover {
            background: #f3f4f6;
            box-shadow: 0 8px 18px rgba(148, 163, 184, 0.6);
            transform: translateY(-1px);
        }

        /* ---------- CODE BLOCKS ---------- */
        .stCode pre {
            border-radius: 14px !important;
            border: 1px solid #e5e7eb !important;
            background: #0f172a !important;
            color: #e5e7eb !important;
            font-size: 13px !important;
        }

        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {background: transparent;}
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
    # mini nav even on login
    st.markdown(
        """
        <div class="top-nav">
            <div class="top-nav-left">
                <div class="logo-dot"></div>
                <div>Aurora</div>
            </div>
            <div class="top-nav-right">
                <span class="status-text">Secure workspace</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="hero-kicker">
            <span class="hero-kicker-dot"></span>
            Private SQL assistant
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='brand-title'>Aurora Query Studio</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='brand-subtitle'>Sign in to your Aurora workspace to turn natural language into safe, optimized SQL for your analytics database.</p>",
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

    st.caption("Passwords are verified with bcrypt. Your session remains active until you logout or close this tab.")


def require_login():
    """Enforce login before showing main app."""
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

    DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"
    return DATABASE_URL


DATABASE_URL = get_db_url()


@st.cache_resource
def get_db_connection():
    """Create and cache database connection."""
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
    """Append a LIMIT if one is not present to keep queries fast/safe."""
    pattern = re.compile(r"\blimit\b", re.IGNORECASE)
    if pattern.search(sql):
        return sql.strip()

    stripped = sql.strip().rstrip(";")
    return f"{stripped}\nLIMIT {default_limit};"


def run_query(sql):
    """Execute SQL query and return results as DataFrame."""
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
    """Create and cache OpenAI client."""
    return OpenAI(api_key=OPENAI_API_KEY)


def extract_sql_from_response(response_text):
    clean_sql = re.sub(
        r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE
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

    # Top nav
    st.markdown(
        """
        <div class="top-nav">
            <div class="top-nav-left">
                <div class="logo-dot"></div>
                <div>Aurora Query Studio</div>
            </div>
            <div class="top-nav-right">
                <span class="status-pill">Beta</span>
                <span class="status-text">Connected to PostgreSQL • Guarded by timeouts</span>
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
            Natural language to SQL, in one workspace
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='brand-title'>Aurora Query Studio</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='brand-subtitle'>Describe the insight you want. Aurora turns it into audited SQL you can inspect, refine, and run with guardrails.</p>",
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

    # Init state
    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "current_question" not in st.session_state:
        st.session_state.current_question = None

    # Workspace card
    st.markdown("<div class='app-shell'><div class='app-shell-inner'>", unsafe_allow_html=True)
    left, right = st.columns([1.85, 1], gap="large")

    # LEFT: question + SQL + results
    with left:
        st.markdown("<div class='section-title'>Your question</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='section-caption'>Write in plain language. You can mention products, regions, time ranges, and metrics.</div>",
            unsafe_allow_html=True,
        )
        user_question = st.text_area(
            label="",
            height=110,
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
            user_question = user_question.strip()
            if st.session_state.current_question != user_question:
                st.session_state.generated_sql = None
                st.session_state.current_question = None

            with st.spinner("🧠 Composing SQL…"):
                sql_query = generate_sql_with_gpt(user_question)
                if sql_query:
                    st.session_state.generated_sql = sql_query
                    st.session_state.current_question = user_question

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

    # RIGHT: stats + schema + history
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

    st.markdown("</div></div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
