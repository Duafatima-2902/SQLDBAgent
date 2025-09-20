"""
Advanced Analytics SQL Agent with Complex Query Capabilities (Using Gemini API)

This script demonstrates an advanced implementation of a secure SQL agent designed for
complex business analytics and reporting. It builds upon the security framework from
script 03 while adding sophisticated business logic and analytics capabilities.

Key Features:
🔒 Same security guardrails as script 03 (read-only, validation, etc.)
📊 Advanced analytics queries (revenue analysis, customer segmentation, etc.)
📈 Business intelligence capabilities (trends, rankings, aggregations)
🔄 Multi-turn conversation support for iterative analysis
📋 Comprehensive business logic documentation in system prompt
"""

# Load environment variables first (including GEMINI_API_KEY)
from dotenv import load_dotenv; load_dotenv()

# Core LangChain imports for agent functionality
from langchain_gemini import ChatGemini  # Gemini language model integration
from langchain.agents import initialize_agent, AgentType  # Agent creation and configuration
from langchain.schema import SystemMessage  # System message formatting for agents
from langchain_community.utilities import SQLDatabase  # Database schema inspection utilities

# Data validation and tool creation imports
from pydantic import BaseModel, Field  # Data validation and serialization
from langchain.tools import BaseTool  # Base class for creating custom tools
from typing import Type  # Type hinting for better code documentation

# Database and utility imports
import sqlalchemy  # Database engine and connection management
import re  # Regular expressions for SQL pattern matching and validation

# Database Configuration
DB_URL = "sqlite:///sql_agent_class.db"

# Create Database Engine
engine = sqlalchemy.create_engine(DB_URL)

class QueryInput(BaseModel):
    """
    Pydantic model for analytics query input validation.

    This model defines the expected input structure for complex analytics queries.
    It enforces the same security constraints as the basic safe agent while
    supporting more sophisticated analytical operations.

    Attributes:
        sql (str): A single read-only SELECT statement optimized for analytics
                  Supports complex JOINs, aggregations, and window functions
                  Automatically bounded with LIMIT for result set control
    """
    sql: str = Field(description="A single read-only SELECT statement, bounded with LIMIT when returning many rows.")

class SafeSQLTool(BaseTool):
    """
    Advanced Analytics SQL Tool - Secure Complex Query Execution

    This tool extends the basic SafeSQLTool with enhanced capabilities for complex
    analytics queries while maintaining the same security controls.

    Enhanced Features for Analytics:
    ✅ Support for complex JOINs across multiple tables
    ✅ Advanced aggregation functions (SUM, COUNT, AVG, etc.)
    ✅ Window functions and analytics operations
    ✅ Date/time functions for trend analysis
    ✅ Subqueries and CTEs for complex logic
    ✅ Performance optimization through automatic LIMIT injection

    Security Features (inherited):
    🔒 Input validation using regex patterns
    🔒 Whitelist approach - only SELECT statements allowed
    🔒 SQL injection protection through pattern matching
    🔒 Multiple statement prevention
    🔒 Comprehensive error handling
    🔒 Read-only operations only
    """

    name: str = "execute_sql"
    description: str = "Execute one read-only SELECT."
    args_schema: Type[BaseModel] = QueryInput

    def _run(self, sql: str) -> str | dict:
        """
        Execute complex analytics SQL with comprehensive security validation.

        Args:
            sql (str): The analytics SQL statement to validate and execute

        Returns:
            dict: For successful queries - {"columns": [...], "rows": [...]}
            str: For validation errors or SQL execution errors
        """
        s = sql.strip().rstrip(";")

        if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE)\b", s, re.I):
            return "ERROR: write operations are not allowed."

        if ";" in s:
            return "ERROR: multiple statements are not allowed."

        if not re.match(r"(?is)^\s*select\b", s):
            return "ERROR: only SELECT statements are allowed."

        if not re.search(r"\blimit\s+\d+\b", s, re.I) and not re.search(r"\bcount\(|\bgroup\s+by\b|\bsum\(|\bavg\(|\bmax\(|\bmin\(", s, re.I):
            s += " LIMIT 200"

        try:
            with engine.connect() as conn:
                result = conn.exec_driver_sql(s)
                rows = result.fetchall()
                cols = list(result.keys()) if result.keys() else []
                return {"columns": cols, "rows": [list(r) for r in rows]}
        except Exception as e:
            return f"ERROR: {e}"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError

# Advanced Database Schema Configuration
db = SQLDatabase.from_uri(DB_URL, include_tables=["customers", "orders", "order_items", "products", "refunds", "payments"])
schema_context = db.get_table_info()

# Advanced System Message with Business Logic
system = f"""You are a careful analytics engineer for SQLite.
Use only listed tables. Revenue = sum(quantity*unit_price_cents) - refunds.amount_cents.
\n\nSchema:\n{schema_context}"""

# Initialize Advanced Language Model
llm = ChatGemini(model="gemini-4-mini", temperature=0)

# Create Analytics Tool Instance
tool = SafeSQLTool()

# Create Advanced Analytics Agent
agent = initialize_agent(
    tools=[tool],
    llm=llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=True,
    agent_kwargs={"system_message": SystemMessage(content=system)}
)

# Complex Analytics Query Demonstrations

# Query 1: Product Revenue Analysis
print(agent.invoke({"input": "Top 5 products by gross revenue (before refunds). Include product name and total_cents."})["output"])

# Query 2: Time-Series Revenue Analysis
print(agent.invoke({"input": "Weekly net revenue for the last 6 weeks. Return week_start, net_cents."})["output"])

# Query 3: Customer Lifecycle Analysis
print(agent.invoke({"input": "For each customer, show their first_order_month, total_orders, last_order_date. Return 10 rows."})["output"])

# Query 4: Customer Lifetime Value Ranking
print(agent.invoke({"input": "Rank customers by lifetime net revenue (sum of items minus refunds). Show rank, customer, net_cents. Top 10."})["output"])

# Multi-Turn Conversation Demonstrations

# Turn 1: High-level category analysis
print(agent.invoke({"input": "What categories drive the most revenue?"})["output"])

# Turn 2: Drill-down analysis building on previous context
print(agent.invoke({"input": "Break the top category down by product with totals."})["output"])