from abc import ABCMeta, abstractmethod
from typing import Iterable, List, Optional

from sqlalchemy import create_engine, inspect, text

from datagpt.log import logger


class TableInfo:
    name: str
    comment: str

    def __init__(self, name: str, comment: Optional[str] = None):
        self.name = name
        self.comment = comment

    def __str__(self):
        return f"TableInfo(name={self.name}, comment={self.comment})"


class FieldInfo:
    col_name: str
    col_type: str
    col_length: Optional[int]
    col_default: str
    is_nullable: bool
    col_comment: str

    def __init__(
        self,
        col_name: str,
        col_type: str,
        col_length: Optional[int],
        col_default: str,
        is_nullable: bool = False,
        col_comment: Optional[str] = None,
    ):
        self.col_name = col_name
        self.col_type = col_type
        self.col_length = col_length
        self.col_default = col_default
        self.is_nullable = is_nullable
        self.col_comment = col_comment

    def __str__(self):
        return f"FieldInfo(col_name={self.col_name}, col_type={self.col_type}, col_length={self.col_length}, col_default={self.col_default}, is_nullable={self.is_nullable}, col_comment={self.col_comment})"


class RDBMS(metaclass=ABCMeta):
    @abstractmethod
    def get_type(self) -> str:
        pass

    @abstractmethod
    def get_tables(self) -> Iterable[TableInfo]:
        pass

    @abstractmethod
    def get_fields(
        self, table_name: str, schema: str = "public"
    ) -> Iterable[FieldInfo]:
        pass

    @abstractmethod
    def run(self, command: str) -> List:
        pass


class Engine(RDBMS):
    def __init__(self, uri: str):
        if uri.startswith("postgresql"):
            if uri.startswith("postgresql://"):
                uri = uri.replace("postgresql://", "postgresql+psycopg://")
            self._type = "PostgreSQL"
        elif uri.startswith("mysql") or uri.startswith("mariadb"):
            self._type = "MySQL"
        else:
            self._type = "Unknown"

        self._uri = uri
        self._engine = create_engine(uri)
        self._inspector = inspect(self._engine)

    def get_type(self) -> str:
        return self._type

    def get_tables(self) -> Iterable[TableInfo]:
        table_names = self._inspector.get_table_names()

        return [
            TableInfo(name, self._inspector.get_table_comment(name))
            for name in table_names
        ]

    def get_fields(
        self, table_name: str, schema: str = "public"
    ) -> Iterable[FieldInfo]:
        if self._type != "PostgreSQL":
            schema = None

        field_records = self._inspector.get_columns(table_name, schema)

        table_fields: List[FieldInfo] = []

        for field in field_records:
            f = FieldInfo(
                field["name"],
                field["type"],
                None,
                field["default"],
                field["nullable"],
                field["comment"],
            )
            if hasattr(field["type"], "__visit_name__"):
                f.col_type = field["type"].__visit_name__

            if hasattr(field["type"], "length"):
                f.col_length = field["type"].length

            table_fields.append(f)

        return table_fields

    def run(self, command: str) -> List:
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(command))
                rows = result.fetchall()
                cols = result.keys()
                rows.insert(0, tuple([col for col in cols]))
                return rows
        except Exception as e:
            logger.error("run sql error: " + str(e))
            return "SqlError"
