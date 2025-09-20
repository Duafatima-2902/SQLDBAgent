"""
Safe SQL Agent with Security Guardrails (Using Gemini API)

This script demonstrates a SECURE implementation of a SQL agent that only allows
read-only SELECT operations and implements multiple layers of security controls.

This is the SAFE alternative to the dangerous agent in script 02.

Security Features Implemented:
✅ Input validation using regex patterns
✅ Whitelist approach - only SELECT statements allowed
✅ Automatic LIMIT injection to prevent large result sets
✅ SQL injection protection through pattern matching
✅ Multiple statement prevention
✅ Error handling for SQL execution failures
✅ Read-only operations only - no data modification possible

Educational Purpose: Shows best practices for SQL agent security.
This pattern should be used as a baseline for production implementations.
"""

import re  # Regular expressions for SQL pattern matching and validation
import sqlalchemy  # Database engine and connection management
from pydantic import BaseModel, Field  # Data validation and serialization
from langchain.tools import BaseTool  # Base class for creating custom tools
from langchain_gemini import ChatGemini  # Gemini language model integration
from langchain.agents import initialize_agent, AgentType  # Agent creation and configuration
from langchain_community.utilities import SQLDatabase  # Database schema inspection utilities
from langchain.schema import SystemMessage  # System message formatting for agents
from typing import Type  # Type hinting for better code documentation
from dotenv import load_dotenv; load_dotenv()  # Environment variable loading

# Database Configuration
# DB_URL: SQLite database connection string for local development
DB_URL = "sqlite:///sql_agent_class.db"

# Create Database Engine
# sqlalchemy.create_engine: Creates a database engine for connection management
# Used for direct SQL execution with our custom safety checks
engine = sqlalchemy.create_engine(DB_URL)

class QueryInput(BaseModel):
    """
    Pydantic model for safe SQL query input validation.

    This model defines the expected input structure for the safe SQL execution tool.
    It includes clear documentation about what types of queries are allowed.

    Attributes:
        sql (str): A single read-only SELECT statement with automatic LIMIT bounds
    """
    sql: str = Field(description="A single read-only SELECT statement, bounded with LIMIT when returning many rows.")

class SafeSQLTool(BaseTool):
    """
    SECURE SQL Tool - Only Allows Read-Only SELECT Operations

    This tool implements multiple layers of security to prevent dangerous SQL operations.
    It serves as a safe alternative to unrestricted SQL execution tools.

    Security Layers:
    1. Pattern-based validation using regex
    2. Whitelist approach (only SELECT allowed)
    3. Automatic LIMIT injection for result set control
    4. SQL injection pattern detection
    5. Multi-statement prevention
    6. Comprehensive error handling

    Attributes:
        name (str): Tool identifier for agent tool selection
        description (str): Clear description of tool capabilities and restrictions
        args_schema (Type[BaseModel]): Pydantic model for input validation
    """

    # Tool Configuration
    name: str = "execute_sql"
    description: str = "Execute exactly one SELECT statement; DML/DDL is forbidden."
    args_schema: Type[BaseModel] = QueryInput

    def _run(self, sql: str) -> str | dict:
        """
        Execute SQL with comprehensive security validation.

        This method implements multiple security checks before executing any SQL.
        It follows a security-first approach with validation at every step.

        Args:
            sql (str): The SQL statement to validate and execute

        Returns:
            dict: For successful SELECT queries - {"columns": [...], "rows": [...]}
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

# Database Schema Inspection
db = SQLDatabase.from_uri(DB_URL, include_tables=["customers", "orders", "order_items", "products", "refunds", "payments"])
schema_context = db.get_table_info()

# System Message Configuration
system = f"You are a careful analytics engineer for SQLite. Use only these tables.\n\n{schema_context}"

# Initialize Language Model
llm = ChatGemini(model="gemini-4-mini", temperature=0)

# Create Safe Tool Instance
safe_tool = SafeSQLTool()

# Create Secure Agent
agent = initialize_agent(
    tools=[safe_tool],
    llm=llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=True,
    agent_kwargs={"system_message": SystemMessage(content=system)}
)

# Test Safe Operations
print(agent.invoke({"input": "Show 5 customers with their sign-up dates and regions."})["output"])
print(agent.invoke({"input": "Delete all orders older than July 1, 2025."})["output"])