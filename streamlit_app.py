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

st.set_page_config(page_title="SQL Copilot", page_icon="🧭", layout="wide")

st.markdown(
    """
    <style>
        /* ---------- GLOBAL ---------- */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, sans-serif !important;
            background: radial-gradient(circle at top left, #020617 0, #020617 30%, #020617 60%, #020617 100%);
            color: #e5e7eb;
        }

        .block-container {
            padding: 1.8rem 2rem 3rem;
            max-width: 1350px;
        }

        /* ---------- CARDS / PANELS ---------- */
        .panel {
            background: #020617;
            border-radius: 18px;
            padding: 20px 20px 22px;
            border: 1px solid rgba(148, 163, 184, 0.35);
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.55);
        }

        .headline {
            font-size: 30px;
            font-weight: 650;
            letter-spacing: -0.02em;
            color: #f9fafb;
        }

        .subhead {
            color: #9ca3af;
            font-size: 14.5px;
        }

        .pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 12px;
            border-radius: 999px;
            background: radial-gradient(circle at 0 0, #38bdf8 0, #6366f1 40%, #a855f7 100%);
            color: #0b1120;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }

        /* ---------- TEXT INPUTS ---------- */
        .stTextArea textarea {
            border-radius: 14px !important;
            border: 1px solid rgba(148, 163, 184, 0.6) !important;
            background: rgba(15, 23, 42, 0.9) !important;
            color: #e5e7eb !important;
            font-size: 15px !important;
        }

        .stTextInput input {
            border-radius: 12px !important;
            border: 1px solid rgba(148, 163, 184, 0.7) !important;
            background: rgba(15, 23, 42, 0.9) !important;
            color: #e5e7eb !important;
        }

        /* ---------- BUTTONS ---------- */
        .btn-primary button {
            background: linear-gradient(135deg, #2563eb, #7c3aed);
            border: none;
            color: #f9fafb;
            font-weight: 600;
            border-radius: 999px;
            padding: 0.5rem 1rem;
            box-shadow: 0 12px 30px rgba(37, 99, 235, 0.4);
        }
        .btn-primary button:hover {
            filter: brightness(1.08);
        }

        .btn-secondary button {
            background: rgba(15, 23, 42, 0.85);
            color: #e5e7eb;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.6);
            font-weight: 500;
        }

        /* ---------- SIDEBAR ---------- */
        [data-testid="stSidebar"] {
            background: #020617 !important;
            border-right: 1px solid rgba(31, 41, 55, 0.9);
        }
        [data-testid="stSidebar"] * {
            color: #e5e7eb !important;
        }
        [data-testid="stSidebar"] .stButton button {
            width: 100%;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.6);
            background: rgba(15, 23, 42, 0.95);
            color: #e5e7eb;
            font-weight: 500;
        }
        [data-testid="stSidebar"] .stButton button:hover {
            border-color: #38bdf8;
        }

        /* ---------- METRIC CARDS / CODE ---------- */
        .metric-card {
            background: rgba(15, 23, 42, 0.95);
            border: 1px solid rgba(148, 163, 184, 0.45);
            border-radius: 14px;
            padding: 10px 12px;
        }

        .stCode pre {
            border-radius: 14px !important;
            border: 1px solid rgba(148, 163, 184, 0.4) !important;
            background: #020617 !important;
            color: #e5e7eb !important;
            font-size: 13px !important;
        }

        /* Hide default Streamlit menu/footer for a cleaner, app-like feel */
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
    st.markdown("<p class='pill'>Secure Workspace</p>", unsafe_allow_html=True)
    st.markdown("<div class='headline'>Sign in to SQL Copilot</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='subhead'>Enter your password to access the interactive SQL assistant for your analytics warehouse.</p>",
        unsafe_allow_html=True,
    )

    with st.container():
        password = st.text_input("Password", type="password", key="login_password")
        col1, col2 = st.columns([1, 3])
        with col1:
            with st.container():
                st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
                login_btn = st.button("🔓 Enter Workspace", use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode("utf-8"), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful. Loading your workspace…")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")

    st.caption("Passwords are verified with bcrypt. Your session stays active until you logout or close this tab.")


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
    pattern = re.compile(r"\\blimit\\b", re.IGNORECASE)
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

    st.markdown("<p class='pill'>SQL Copilot</p>", unsafe_allow_html=True)
    st.markdown("<div class='headline'>Ask. Inspect. Execute.</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='subhead'>Describe the insight you need in plain language. Review the generated SQL, tweak if needed, then run it with built-in guardrails.</p>",
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

    left, right = st.columns([1.8, 1], gap="large")

    # ---------- LEFT COLUMN: QUESTION + SQL + RESULTS ----------
    with left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)

        st.markdown("#### Your question")
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
            user_question = user_question.strip()
            if st.session_state.current_question != user_question:
                st.session_state.generated_sql = None
                st.session_state.current_question = None

            with st.spinner("🧠 Generating SQL…"):
                sql_query = generate_sql_with_gpt(user_question)
                if sql_query:
                    st.session_state.generated_sql = sql_query
                    st.session_state.current_question = user_question

        if st.session_state.generated_sql:
            st.divider()
            st.markdown("#### Generated SQL")
            st.caption(f"Question: {st.session_state.current_question}")

            edited_sql = st.text_area(
                "Review and edit before execution:",
                value=st.session_state.generated_sql,
                height=220,
            )

            st.markdown("<div class='btn-primary'>", unsafe_allow_html=True)
            run_button = st.button(
                "▶️ Run Query", use_container_width=True, key="run_btn"
            )
            st.markdown("</div>", unsafe_allow_html=True)

            if run_button:
                with st.spinner("Executing query…"):
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

        st.markdown("</div>", unsafe_allow_html=True)

    # ---------- RIGHT COLUMN: STATS / SCHEMA / HISTORY ----------
    with right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)

        st.markdown("#### Workspace stats")
        stat_cols = st.columns(2)
        with stat_cols[0]:
            st.markdown(
                f"<div class='metric-card'><div style='font-size:12px;color:#9ca3af;'>Default LIMIT</div><div style='font-size:20px;font-weight:600;color:#e5e7eb;'>{QUERY_DEFAULT_LIMIT}</div></div>",
                unsafe_allow_html=True,
            )
        with stat_cols[1]:
            st.markdown(
                f"<div class='metric-card'><div style='font-size:12px;color:#9ca3af;'>Statement timeout</div><div style='font-size:20px;font-weight:600;color:#e5e7eb;'>{int(STATEMENT_TIMEOUT_MS/1000)}s</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("#### Schema primer")
        st.caption("Use these anchors when you ask your question.")
        st.code(
            "Region → Country → Customer → OrderDetail\nProductCategory → Product → OrderDetail",
            language="text",
        )

        if st.session_state.query_history:
            st.markdown("#### Recent queries")
            for idx, item in enumerate(reversed(st.session_state.query_history[-3:])):
                st.markdown(
                    f"**Q{len(st.session_state.query_history)-idx}:** {item['question']}"
                )
                st.code(item["sql"], language="sql")
                st.caption(f"Rows: {item['rows']}")

        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
