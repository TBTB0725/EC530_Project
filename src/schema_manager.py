import logging
import re
import sqlite3
import pandas as pd
from dataclasses import dataclass, field

# Write error into error_log.txt
logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


@dataclass
class ColumnSchema:
    # Describe one column in a table

    name: str
    data_type: str
    is_primary_key: bool = False
    is_nullable: bool = True


@dataclass
class TableSchema:
    # Describe the schema of one table

    table_name: str
    columns: list[ColumnSchema] = field(default_factory=list)


@dataclass
class SchemaMatchResult:
    # Store the result of comparing two schemas
    
    is_match: bool
    reason: str
    existing_only: list[str] = field(default_factory=list)
    incoming_only: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)


class SchemaManager:
    # Read, compare, and describe database schema information

    def __init__(self, db_path: str) -> None:
        # Store the SQLite database path

        self.db_path = db_path

    def list_tables(self) -> list[str]:
        # Return all tables in the database

        try:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    ORDER BY name
                    """
                ).fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logging.error(f"Failed to list tables: {e}")
            return []

    def get_table_schema(self, table_name: str) -> TableSchema | None:
        # Read the schema of an existing SQLite table

        try:
            table_name = self.normalize_name(table_name)
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()

            if not rows:
                return None

            columns = []
            for _, col_name, col_type, notnull, _, pk in rows:
                columns.append(
                    ColumnSchema(
                        name=col_name,
                        data_type=(col_type or "TEXT").upper(),
                        is_primary_key=(pk == 1),
                        is_nullable=(notnull == 0),
                    )
                )

            return TableSchema(table_name=table_name, columns=columns)
        except Exception as e:
            logging.error(f"Failed to get schema for table '{table_name}': {e}")
            return None

    def infer_schema_from_dataframe(self, df: pd.DataFrame, table_name: str) -> TableSchema:
        # Infer a table schema from a DataFrame and always add an id primary key

        table_name = self.normalize_name(table_name)
        column_names = [self.normalize_name(col) for col in df.columns]

        if len(set(column_names)) != len(column_names):
            error = "Column names become duplicated after normalization."
            logging.error(error)
            raise ValueError(error)

        columns = [ColumnSchema(name="id", data_type="INTEGER", is_primary_key=True, is_nullable=False)]
        for original_name, normalized_name in zip(df.columns, column_names):
            columns.append(
                ColumnSchema(
                    name=normalized_name,
                    data_type=self._infer_sqlite_type(df[original_name]),
                )
            )

        return TableSchema(table_name=table_name, columns=columns)

    def generate_create_table_sql(self, schema: TableSchema) -> str:
        # Build a CREATE TABLE statement from a TableSchema object

        parts = []
        for col in schema.columns:
            if col.name == "id" and col.is_primary_key and col.data_type.upper() == "INTEGER":
                parts.append('"id" INTEGER PRIMARY KEY AUTOINCREMENT')
                continue

            col_sql = f'"{col.name}" {col.data_type}'
            if col.is_primary_key:
                col_sql += " PRIMARY KEY"
            if not col.is_nullable and not col.is_primary_key:
                col_sql += " NOT NULL"
            parts.append(col_sql)

        return f'CREATE TABLE "{schema.table_name}" ({", ".join(parts)})'

    def compare_schemas(self, existing: TableSchema, incoming: TableSchema) -> SchemaMatchResult:
        # Compare two schemas and ignore the auto-generated id column

        try:
            existing_map = {
                self.normalize_name(col.name): col.data_type.upper()
                for col in existing.columns
                if not (col.name == "id" and col.is_primary_key)
            }
            incoming_map = {
                self.normalize_name(col.name): col.data_type.upper()
                for col in incoming.columns
                if not (col.name == "id" and col.is_primary_key)
            }

            existing_only = sorted(set(existing_map) - set(incoming_map))
            incoming_only = sorted(set(incoming_map) - set(existing_map))

            type_mismatches = []
            for name in sorted(set(existing_map) & set(incoming_map)):
                if existing_map[name] != incoming_map[name]:
                    type_mismatches.append(
                        f"{name}: existing={existing_map[name]}, incoming={incoming_map[name]}"
                    )

            if not existing_only and not incoming_only and not type_mismatches:
                return SchemaMatchResult(True, "Column names and data types match exactly.")

            reason_parts = []
            if existing_only:
                reason_parts.append(f"Existing-only columns: {existing_only}")
            if incoming_only:
                reason_parts.append(f"Incoming-only columns: {incoming_only}")
            if type_mismatches:
                reason_parts.append(f"Type mismatches: {type_mismatches}")

            return SchemaMatchResult(
                is_match=False,
                reason="; ".join(reason_parts),
                existing_only=existing_only,
                incoming_only=incoming_only,
                type_mismatches=type_mismatches,
            )
        except Exception as e:
            logging.error(f"Failed to compare schemas: {e}")
            return SchemaMatchResult(False, str(e))

    def format_schema_for_llm(self) -> str:
        # Return the current database schema as plain text

        try:
            tables = self.list_tables()
            if not tables:
                return "No tables found in the database."

            lines = []
            for table_name in tables:
                schema = self.get_table_schema(table_name)
                if schema is None:
                    continue

                lines.append(f"Table: {schema.table_name}")
                for col in schema.columns:
                    extras = []
                    if col.is_primary_key:
                        extras.append("PRIMARY KEY")
                    if not col.is_nullable:
                        extras.append("NOT NULL")
                    suffix = f" ({', '.join(extras)})" if extras else ""
                    lines.append(f"  - {col.name}: {col.data_type}{suffix}")

            return "\n".join(lines)
        except Exception as e:
            logging.error(f"Failed to format schema for LLM: {e}")
            return "Failed to read database schema."

    def resolve_conflict_interactive(self, table_name: str) -> str:
        # Ask the user what to do when schemas do not match

        while True:
            choice = input(
                f"Schema conflict for table '{table_name}'. Choose [overwrite / rename / skip]: "
            ).strip().lower()
            if choice in {"overwrite", "rename", "skip"}:
                return choice
            print("Invalid choice. Please enter overwrite, rename, or skip.")

    def normalize_name(self, name: str) -> str:
        # Convert table and column names into SQLite-friendly identifiers

        name = str(name).strip()
        if not name:
            raise ValueError("Identifier cannot be empty.")

        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        if re.match(r"^\d", name):
            name = "_" + name
        return name

    def _infer_sqlite_type(self, series: pd.Series) -> str:
        # Map pandas column types to simple SQLite types

        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        if pd.api.types.is_float_dtype(series):
            return "REAL"
        if pd.api.types.is_bool_dtype(series):
            return "INTEGER"
        return "TEXT"
