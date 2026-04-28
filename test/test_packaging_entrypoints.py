from pathlib import Path


def _pyproject_text():
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    return pyproject.read_text(encoding="utf-8")


def test_pyproject_registers_supported_sqlalchemy_dialects():
    contents = _pyproject_text()

    assert 'informix = "IfxAlchemy.pyodbc:IfxDialect_pyodbc"' in contents
    assert (
        '"informix.pyodbc" = "IfxAlchemy.pyodbc:IfxDialect_pyodbc"'
        in contents
    )
    assert "informix.ifxpy" not in contents


def test_pyproject_uses_readme_md_as_package_description():
    contents = _pyproject_text()

    assert 'readme = "README.md"' in contents


def test_pyproject_sqlalchemy_range_supports_20_and_21():
    contents = _pyproject_text()

    assert '"SQLAlchemy>=2.0.45,<2.2"' in contents
    assert '"SQLAlchemy>=2.0.20,<2.2"' not in contents
    assert '"SQLAlchemy>=2.0,<2.2"' not in contents
    assert '"SQLAlchemy>=2.0.45,<2.1"' not in contents
    assert '"SQLAlchemy>=2.0,<2.3"' not in contents


def test_pyproject_does_not_pin_sqlalchemy_beta():
    contents = _pyproject_text()

    assert "SQLAlchemy==2.1.0b2" not in contents
    assert "--pre" not in contents
