import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from openai import OpenAI
import os
import bcrypt
import re


load_dotenv()  # reads variables from a .env file and sets them in os.environ

OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

QUERY_DEFAULT_LIMIT = 500
STATEMENT_TIMEOUT_MS = 15_000
LOCK_TIMEOUT_MS = 3_000
CONNECT_TIMEOUT_S = 5


# Database schema for context
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



def login_screen():
    """Display login screen and authenticate user."""
    st.title("🔐 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")
    
    password = st.text_input("Password", type="password", key="login_password")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
    
    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")
    
    st.markdown("---")
    st.info("""
    **Security Notice:**
    - Passwords are protected using bcrypt hashing
    - Your session is secure and isolated
    - You will remain logged in until you close the browser or click logout
    """)


def require_login():
    """Enforce login before showing main app."""
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

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
            cur.execute("SET idle_in_transaction_session_timeout = %s;", (LOCK_TIMEOUT_MS * 10,))
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def _ensure_limit(sql, default_limit=QUERY_DEFAULT_LIMIT):
    """Append a LIMIT if one is not present to keep queries fast/safe."""
    # crude check for existing LIMIT
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
    

@st.cache_resource
def get_openai_client():
    """Create and cache OpenAI client."""
    return OpenAI(api_key=OPENAI_API_KEY)

def extract_sql_from_response(response_text):
    clean_sql = re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()
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
5. Use proper date/time functions for TIMESTAMP columns
6. Make sure the query is syntactically correct for PostgreSQL
7. Add helpful column aliases using AS

Generate the SQL query:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a PostgreSQL expert who generates accurate SQL queries based on natural language questions."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1000
        )
        
        sql_query = extract_sql_from_response(response.choices[0].message.content)
        return sql_query
    
    except Exception as e:
        st.error(f"Error calling OpenAI API: {e}")
        return None, None

def main():
    require_login()
    st.title("🤖 AI-Powered SQL Query Assistant")
    st.markdown("Ask questions in natural language, and I will generate SQL queries for you to review and run!")
    st.markdown("---")


    st.sidebar.title("💡 Example Questions")
    st.sidebar.markdown("""
    Try asking questions like:
                        
    **Customers & Regions:**
    - Customer counts by country and region
    - Top 10 cities by number of customers
                        
    **Orders & Revenue:**
    - Total revenue by product category
    - Daily order counts over time
    - Best selling products by quantity
    """)
    st.sidebar.markdown("---")
    st.sidebar.info("""
        🩼**How it works:**
        1. Enter your question in plain English
        2. AI generates SQL query
        3. Review and optionally edit the query
        4. Click "Run Query" to execute           
    """)

    st.sidebar.markdown("---")
    if st.sidebar.button("🚪Logout"):
        st.session_state.logged_in = False
        st.rerun()

    # Init state

    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None


    # main input

    user_question = st.text_area(
        " What would you like to know?",
        height=100, 
        placeholder="What is the average length of stay?    "
    )

    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        generate_button = st.button(" Generate SQL", type="primary", width="stretch")

    with col2:
        if st.button(" Clear History", width="stretch"):
            st.session_state.query_history = []
            st.session_state.generated_sql = None
            st.session_state.current_question = None

    if generate_button and user_question:
        user_question = user_question.strip()

        if st.session_state.current_question != user_question:
            st.session_state.generated_sql = None
            st.session_state.current_question = None
            


        with st.spinner("🧠 AI is thinking and generating SQL..."):
            sql_query = generate_sql_with_gpt(user_question)
            if sql_query:        
                st.session_state.generated_sql = sql_query
                st.session_state.current_question = user_question

    if st.session_state.generated_sql:
        st.markdown("---")
        st.subheader("Generated SQL Query")
        st.info(f"**Question:** {st.session_state.current_question}")

        edited_sql = st.text_area(
            "Review and edit the SQL query if needed:", 
            value=st.session_state.generated_sql,
            height=200,
        )

        col1, col2 = st.columns([1, 5])

        with col1:
            run_button = st.button("Run Query", type="primary", width="stretch")

        if run_button:
            with st.spinner("Executing query ..."):
                df = run_query(edited_sql)
                
                if df is not None:
                    st.session_state.query_history.append(
                        {'question': user_question, 
                        'sql': edited_sql, 
                        'rows': len(df)}
                    )

                    st.markdown("---")
                    st.subheader("📊 Query Results")
                    st.success(f"✅ Query returned {len(df)} rows")
                    st.dataframe(df, width="stretch")


    if st.session_state.query_history:
        st.markdown('---')
        st.subheader("📜 Query History")
        for idx, item in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history)-idx}: {item['question'][:60]}..."):
                st.markdown(f"**Question:** {item['question']}")
                st.code(item["sql"], language="sql")
                st.caption(f"Returned {item['rows']} rows")
                if st.button(f"Re-run this query", key=f"rerun_{idx}"):
                    df = run_query(item["sql"])
                    if df is not None:
                        st.dataframe(df, width="stretch")


if __name__ == "__main__":
    main()
