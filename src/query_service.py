import sqlite3
from dataclasses import dataclass
from typing import Any
from llm_adapter import BaseLLMAdapter
from schema_manager import SchemaManager
from sql_validator import SQLValidator


@dataclass
class QueryResult:
    # Store the result of one query request

    success: bool
    rows: list[dict[str, Any]] | None = None
    sql: str | None = None
    error: str | None = None
    llm_explanation: str | None = None


class QueryService:
    # Validate and execute direct SQL or LLM-generated SQL

    def __init__(
        self,
        db_path: str,
        schema_manager: SchemaManager,
        validator: SQLValidator,
        llm_adapter: BaseLLMAdapter | None = None,
    ) -> None:
        # Store the dependencies used for query handling

        self.db_path = db_path
        self.schema_manager = schema_manager
        self.validator = validator
        self.llm_adapter = llm_adapter

    def execute_user_sql(self, sql: str) -> QueryResult:
        # Validate and run a SQL query entered directly by the user

        validation = self.validator.validate(sql)
        if not validation.is_valid:
            return QueryResult(success=False, sql=sql, error=validation.error)

        try:
            return QueryResult(success=True, rows=self._run_select(sql), sql=sql)
        except Exception as e:
            return QueryResult(success=False, sql=sql, error=str(e))

    def ask(self, user_query: str, show_generated_sql: bool = True) -> QueryResult:
        # Turn a natural-language question into SQL, validate it, and run it

        if self.llm_adapter is None:
            return QueryResult(success=False, error="No LLM adapter configured.")

        schema_text = self.schema_manager.format_schema_for_llm()
        llm_response = self.llm_adapter.generate_sql(user_query, schema_text)

        if not llm_response.success or not llm_response.sql:
            return QueryResult(success=False, error=llm_response.error or "LLM failed to generate SQL.")

        validation = self.validator.validate(llm_response.sql)
        if not validation.is_valid:
            return QueryResult(
                success=False,
                sql=llm_response.sql if show_generated_sql else None,
                error=f"LLM-generated SQL rejected by validator: {validation.error}",
                llm_explanation=llm_response.explanation,
            )

        try:
            return QueryResult(
                success=True,
                rows=self._run_select(llm_response.sql),
                sql=llm_response.sql if show_generated_sql else None,
                llm_explanation=llm_response.explanation,
            )
        except Exception as e:
            return QueryResult(
                success=False,
                sql=llm_response.sql if show_generated_sql else None,
                error=str(e),
                llm_explanation=llm_response.explanation,
            )

    def list_tables(self) -> list[str]:
        # Return the table names in the current database

        return self.schema_manager.list_tables()

    def _run_select(self, sql: str) -> list[dict[str, Any]]:
        # Execute a SELECT query and return rows as dictionaries
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]
