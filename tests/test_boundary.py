import ast
import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).parent.parent / "python" / "rust_dwos"
SOURCES = sorted(PACKAGE.glob("*.py"))
DATABASE_DRIVERS = {"asyncpg", "mysql", "psycopg", "psycopg2", "pymysql", "sqlalchemy", "sqlite3"}
SQL = re.compile(r"\b(SELECT\b.+\bFROM|INSERT\s+INTO|UPDATE\b.+\bSET|DELETE\s+FROM|CREATE\s+TABLE|DROP\s+TABLE)\b", re.DOTALL)
STUB = ast.parse((PACKAGE / "_native.pyi").read_text())
# A datasource runs the user's own transaction on the user's own SQLAlchemy engine, never the system database.
APP_DATABASE_IMPORTS = {"datasources.py": {"sqlalchemy"}}


def _declared(body: list[ast.stmt]) -> set[str]:
    names = set()
    for node in body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def _methods() -> set[str]:
    names = set()
    for node in STUB.body:
        if isinstance(node, ast.ClassDef):
            names |= _declared(node.body)
    return names


@pytest.mark.parametrize("path", SOURCES, ids=lambda path: path.name)
def test_python_does_not_talk_to_a_database(path: Path) -> None:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [node.module or ""]
        else:
            continue
        for module in modules:
            root = module.split(".")[0]
            allowed = APP_DATABASE_IMPORTS.get(path.name, set())
            assert root not in DATABASE_DRIVERS - allowed, f"{path.name}:{node.lineno} imports {module}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda path: path.name)
def test_python_contains_no_sql(path: Path) -> None:
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not SQL.search(node.value), f"{path.name}:{node.lineno} contains SQL: {node.value[:80]}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda path: path.name)
def test_python_only_uses_declared_native_calls(path: Path) -> None:
    module_names = _declared(STUB.body) | {node.target.id for node in STUB.body if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)}
    methods = _methods()
    for node in ast.walk(ast.parse(path.read_text())):
        if not isinstance(node, ast.Attribute):
            continue
        owner = node.value
        if isinstance(owner, ast.Name) and owner.id == "_native":
            assert node.attr in module_names, f"{path.name}:{node.lineno} uses undeclared _native.{node.attr}"
        if isinstance(owner, ast.Attribute) and owner.attr == "_engine":
            assert node.attr in methods, f"{path.name}:{node.lineno} calls undeclared _engine.{node.attr}"


def test_the_checks_can_fail() -> None:
    assert SQL.search("SELECT status FROM workflow_status")
    assert SQL.search("UPDATE workflow_status SET status = 'x'")
    assert not SQL.search("Update the status, then select a file from disk")
