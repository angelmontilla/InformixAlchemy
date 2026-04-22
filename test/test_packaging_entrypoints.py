from pathlib import Path


def test_setup_registers_all_supported_sqlalchemy_dialects():
    setup_py = Path(__file__).resolve().parents[1] / "setup.py"
    contents = setup_py.read_text(encoding="utf-8")

    assert "informix = IfxAlchemy.pyodbc:IfxDialect_pyodbc" in contents
    assert "informix.pyodbc = IfxAlchemy.pyodbc:IfxDialect_pyodbc" in contents
    assert "informix.ifxpy = IfxAlchemy.IfxPy:IfxDialect_IfxPy" in contents
