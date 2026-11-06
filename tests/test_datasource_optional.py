import subprocess
import sys

# None in sys.modules makes any import of SQLAlchemy fail, as if it were not installed.
_WITHOUT_SQLALCHEMY = """
import sys
sys.modules["sqlalchemy"] = None
import yado
from yado import *
try:
    yado.SQLAlchemyDatasource
except ImportError as error:
    assert "yado[sqlalchemy]" in str(error), error
else:
    raise SystemExit("SQLAlchemyDatasource loaded without SQLAlchemy")
"""


def test_yado_imports_without_sqlalchemy() -> None:
    subprocess.run([sys.executable, "-c", _WITHOUT_SQLALCHEMY], check=True, timeout=60)  # noqa: S603
