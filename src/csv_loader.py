import os
import sqlite3
from dataclasses import dataclass

import pandas as pd

from schema_manager import SchemaManager


@dataclass
class LoadResult:
    # Store the result of loading one CSV file
    
    success: bool
    table_name: str | None = None
    rows_inserted: int = 0
    columns: list[str] | None = None
    error: str | None = None


class CSVLoader:
    # Load CSV files into SQLite tables

    def __init__(self, db_path: str, schema_manager: SchemaManager | None = None) -> None:
        self.db_path = db_path
        self.schema_manager = schema_manager or SchemaManager(db_path)

    def load_csv(
        self,
        csv_path: str,
        table_name: str | None = None,
        if_exists: str = "fail",
    ) -> LoadResult:
        # Read a CSV file, create or reuse a table, and insert the rows

        if not os.path.exists(csv_path):
            return LoadResult(success=False, error=f"CSV file not found: {csv_path}")

        if if_exists not in {"fail", "replace", "append"}:
            return LoadResult(success=False, error="if_exists must be one of: fail, replace, append")

        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            return LoadResult(success=False, error="CSV file is empty.")
        except Exception as e:
            return LoadResult(success=False, error=str(e))

        if df.empty and len(df.columns) == 0:
            return LoadResult(success=False, error="CSV file is empty or invalid.")

        try:
            raw_table_name = table_name or os.path.splitext(os.path.basename(csv_path))[0]
            final_table_name = self.schema_manager.normalize_name(raw_table_name)
            df.columns = [self.schema_manager.normalize_name(col) for col in df.columns]
            incoming_schema = self.schema_manager.infer_schema_from_dataframe(df, final_table_name)
        except Exception as e:
            return LoadResult(success=False, error=str(e))

        if len(set(df.columns)) != len(df.columns):
            return LoadResult(success=False, error="Column names become duplicated after sanitization.")

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                if self._table_exists(cursor, final_table_name):
                    if if_exists == "fail":
                        return LoadResult(success=False, error=f"Table '{final_table_name}' already exists.")

                    if if_exists == "replace":
                        cursor.execute(f'DROP TABLE IF EXISTS "{final_table_name}"')
                        cursor.execute(self.schema_manager.generate_create_table_sql(incoming_schema))

                    else:
                        existing_schema = self.schema_manager.get_table_schema(final_table_name)
                        if existing_schema is None:
                            return LoadResult(success=False, error=f"Could not read schema for '{final_table_name}'.")

                        match = self.schema_manager.compare_schemas(existing_schema, incoming_schema)
                        if not match.is_match:
                            choice = self.schema_manager.resolve_conflict_interactive(final_table_name)

                            if choice == "skip":
                                return LoadResult(success=False, error=f"Skipped load: {match.reason}")

                            if choice == "overwrite":
                                cursor.execute(f'DROP TABLE IF EXISTS "{final_table_name}"')
                                cursor.execute(self.schema_manager.generate_create_table_sql(incoming_schema))
                            else:
                                final_table_name = self._next_available_table_name(cursor, f"{final_table_name}_new")
                                incoming_schema.table_name = final_table_name
                                cursor.execute(self.schema_manager.generate_create_table_sql(incoming_schema))
                else:
                    cursor.execute(self.schema_manager.generate_create_table_sql(incoming_schema))

                rows_inserted = self._insert_rows(cursor, final_table_name, df)
                conn.commit()

            return LoadResult(
                success=True,
                table_name=final_table_name,
                rows_inserted=rows_inserted,
                columns=df.columns.tolist(),
            )
        except Exception as e:
            return LoadResult(success=False, error=str(e))

    def _table_exists(self, cursor: sqlite3.Cursor, table_name: str) -> bool:
        # Check whether a table already exists in SQLite

        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?
            """,
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _insert_rows(self, cursor: sqlite3.Cursor, table_name: str, df: pd.DataFrame) -> int:
        # Insert every row from the DataFrame into the target table

        columns = df.columns.tolist()
        quoted_columns = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'

        rows_inserted = 0
        for _, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isna(value):
                    values.append(None)
                elif hasattr(value, "item"):
                    try:
                        values.append(value.item())
                    except Exception:
                        values.append(value)
                else:
                    values.append(value)
            cursor.execute(insert_sql, values)
            rows_inserted += 1
        return rows_inserted

    def _next_available_table_name(self, cursor: sqlite3.Cursor, base_name: str) -> str:
        # Find a new table name when the original name is already taken
        
        name = base_name
        index = 1
        while self._table_exists(cursor, name):
            name = f"{base_name}_{index}"
            index += 1
        return name
