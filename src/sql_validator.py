from dataclasses import dataclass, field
from schema_manager import SchemaManager
import re

@dataclass
class ValidationResult:
    # Store the result of validating one SQL query

    is_valid: bool
    error: str | None = None
    tables: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)


class SQLValidator:
    # Validate SQL before it reaches the database

    SQL_KEYWORDS = {
        "select", "from", "where", "group", "by", "order", "limit", "having",
        "join", "inner", "left", "right", "full", "outer", "on", "as", "and",
        "or", "not", "in", "is", "null", "like", "between", "distinct", "asc",
        "desc", "count", "sum", "avg", "min", "max",
    }
    DISALLOWED = {
        "insert", "update", "delete", "drop", "alter", "create",
        "replace", "truncate", "attach", "detach", "pragma",
    }
    SQLITE_MASTER_COLUMNS = {"type", "name", "tbl_name", "rootpage", "sql"}

    def __init__(self, schema_manager: SchemaManager) -> None:
        # Store the schema manager used for table and column checks

        self.schema_manager = schema_manager

    def validate(self, sql: str) -> ValidationResult:
        # Check whether a SQL query is safe and references known tables and columns

        sql = sql.strip()
        if not sql:
            return ValidationResult(False, "SQL cannot be empty.")

        lowered = sql.lower()
        if not lowered.startswith("select"):
            return ValidationResult(False, "Only SELECT queries are allowed.")

        for word in self.DISALLOWED:
            if re.search(rf"\b{word}\b", lowered):
                return ValidationResult(False, f"Disallowed SQL keyword detected: {word}")

        if sql.count(";") > 1 or (";" in sql[:-1]):
            return ValidationResult(False, "Multiple SQL statements are not allowed.")

        tables = self._extract_tables(sql)
        if not tables:
            return ValidationResult(False, "Could not determine referenced table(s).")

        known_tables = set(self.schema_manager.list_tables()) | {"sqlite_master"}
        unknown_tables = [table for table in tables if table not in known_tables]
        if unknown_tables:
            return ValidationResult(False, f"Unknown table(s): {unknown_tables}", tables=tables)

        return self._validate_columns(sql, tables)

    def _extract_tables(self, sql: str) -> list[str]:
        # Extract table names used after FROM and JOIN

        matches = re.findall(r'\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)\b', sql, flags=re.IGNORECASE)
        return list(dict.fromkeys(matches))

    def _validate_columns(self, sql: str, tables: list[str]) -> ValidationResult:
        # Check whether referenced columns exist in the referenced tables

        table_columns: dict[str, set[str]] = {}
        all_columns: set[str] = set()

        for table in tables:
            if table == "sqlite_master":
                cols = set(self.SQLITE_MASTER_COLUMNS)
            else:
                schema = self.schema_manager.get_table_schema(table)
                if schema is None:
                    return ValidationResult(False, f"Could not read schema for table '{table}'.")
                cols = {col.name for col in schema.columns}

            table_columns[table] = cols
            all_columns.update(cols)

        referenced = set(self._extract_selected_columns(sql)) | set(self._extract_identifiers(sql))
        cleaned = []
        for token in referenced:
            low = token.lower()
            if low in self.SQL_KEYWORDS or token in tables or token == "*" or token.isdigit():
                continue
            cleaned.append(token)

        unknown = []
        for col in cleaned:
            if "." in col:
                table_name, col_name = col.split(".", 1)
                if table_name in table_columns and col_name in table_columns[table_name]:
                    continue
                unknown.append(col)
            elif col not in all_columns:
                unknown.append(col)

        if unknown:
            return ValidationResult(False, f"Unknown column(s): {sorted(set(unknown))}", tables=tables, columns=sorted(set(cleaned)))

        return ValidationResult(True, None, tables=tables, columns=sorted(set(cleaned)))

    def _extract_selected_columns(self, sql: str) -> list[str]:
        # Extract the columns between SELECT and FROM

        match = re.search(r"\bselect\b(.*?)\bfrom\b", sql, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return []

        raw = match.group(1).strip()
        if raw == "*":
            return ["*"]

        columns = []
        for part in [piece.strip() for piece in raw.split(",")]:
            part = re.sub(r"\bas\b\s+[A-Za-z_][A-Za-z0-9_]*$", "", part, flags=re.IGNORECASE).strip()
            func_match = re.match(r"[A-Za-z_][A-Za-z0-9_]*\((.*?)\)", part)
            columns.append(func_match.group(1).strip() if func_match else part)
        return columns

    def _extract_identifiers(self, sql: str) -> list[str]:
        # Extract identifier-like tokens from the query
        
        sql = re.sub(r"'[^']*'|\"[^\"]*\"", " ", sql)
        return re.findall(r"[A-Za-z_][A-Za-z0-9_\.]*", sql)
