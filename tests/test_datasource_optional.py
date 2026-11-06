import subprocess
import sys

# None in sys.modules makes any import of SQLAlchemy fail, as if it were not installed.
_WITHOUT_SQLALCHEMY = """
import sys
sys.modules["sqlalchemy"] = None
import rust_dwos
from rust_dwos import *
try:
    rust_dwos.SQLAlchemyDatasource
except ImportError as error:
    assert "rust-dwos[sqlalchemy]" in str(error), error
else:
    raise SystemExit("SQLAlchemyDatasource loaded without SQLAlchemy")
"""


def test_rust_dwos_imports_without_sqlalchemy() -> None:
    subprocess.run([sys.executable, "-c", _WITHOUT_SQLALCHEMY], check=True, timeout=60)  # noqa: S603
