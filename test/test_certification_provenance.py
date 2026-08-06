from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from tools.certification import (
    collect_runtime_provenance,
    junit_properties,
    normalise_dburi,
    render_safe_url,
    validate_matrix_coverage,
    write_provenance,
)


class _Result:
    def __init__(self, scalar_value=None, row=None):
        self.scalar_value = scalar_value
        self.row = row

    def scalar(self):
        return self.scalar_value

    def first(self):
        return self.row


class _DbapiConnection:
    def getinfo(self, info_type):
        return {
            1: "iclit09b.so",
            2: "4.50.FC10",
            3: "Informix",
            4: "14.10.FC12",
            5: "03.80",
            6: "ifx_test",
        }[info_type]


class _Connection:
    connection = SimpleNamespace(driver_connection=_DbapiConnection())

    def exec_driver_sql(self, sql, params=()):
        normalized = " ".join(sql.split()).upper()
        if "DBINFO('DBNAME')" in normalized:
            return _Result("ifxalchemy_test")
        if "DBINFO('VERSION', 'FULL')" in normalized:
            return _Result("IBM Informix Dynamic Server Version 14.10.FC12")
        if "SYSMASTER:SYSDATABASES" in normalized:
            return _Result(row=(0, 1))
        if "SBSPACENAME" in normalized:
            return _Result("sbspace")
        raise AssertionError(sql)


def test_provenance_redacts_url_and_records_exact_runtime(monkeypatch, tmp_path):
    fake_pyodbc = SimpleNamespace(
        SQL_DRIVER_NAME=1,
        SQL_DRIVER_VER=2,
        SQL_DBMS_NAME=3,
        SQL_DBMS_VER=4,
        SQL_ODBC_VER=5,
        SQL_DATA_SOURCE_NAME=6,
    )
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    monkeypatch.setenv("DB_LOCALE", "en_US.utf8")
    monkeypatch.setenv("CLIENT_LOCALE", "en_US.utf8")
    monkeypatch.setenv("DELIMIDENT", "Y")
    monkeypatch.setenv("IFXALCHEMY_SERVER_PROFILE", "14.10-latest-fixpack")

    report = collect_runtime_provenance(
        url=(
            "informix+pyodbc://informix:top-secret@127.0.0.1/"
            "ifxalchemy_test?server=informix"
        ),
        connection=_Connection(),
    )
    output = write_provenance(tmp_path / "provenance.json", report)
    serialized = output.read_text()

    assert "top-secret" not in serialized
    assert "***" in report["connection"]["safe_url"]
    assert report["informix"]["server_version_full"].endswith("14.10.FC12")
    assert report["informix"]["ansi"] is False
    assert report["informix"]["sbspace_configured"] is True
    assert report["odbc"]["driver_version"] == "4.50.FC10"
    assert report["labels"]["server"] == "14.10-latest-fixpack"

    properties = junit_properties(report)
    assert properties["informix.server_version"].endswith("14.10.FC12")
    assert properties["odbc.driver_version"] == "4.50.FC10"
    assert properties["matrix.server"] == "14.10-latest-fixpack"


def test_matrix_validator_reports_only_uncovered_labels():
    matrix = {
        "required_axes": {
            "server": ["14.10", "15.x"],
            "database": ["ansi", "non-ansi"],
        }
    }
    reports = [
        {"labels": {"server": "14.10", "database": "ansi"}},
        {"labels": {"server": "15.x", "database": "ansi"}},
    ]

    assert validate_matrix_coverage(matrix, reports) == {
        "database": ["non-ansi"]
    }


def test_provenance_json_is_machine_readable(tmp_path):
    path = write_provenance(
        tmp_path / "report.json",
        collect_runtime_provenance(),
    )
    loaded = json.loads(path.read_text())

    assert loaded["schema_version"] == 1
    assert loaded["project"]["name"] == "IfxAlchemy"
    assert loaded["runtime"]["python"]
    assert loaded["packages"]["SQLAlchemy"]



def test_normalise_dburi_accepts_string():
    url = "informix+pyodbc://user:password@localhost/database"

    assert normalise_dburi(url) == url


def test_normalise_dburi_accepts_single_item_sequence():
    url = "informix+pyodbc://user:password@localhost/database"

    assert normalise_dburi([url]) == url


def test_normalise_dburi_accepts_url_object():
    from sqlalchemy.engine import make_url

    url = make_url("informix+pyodbc://user:password@localhost/database")

    assert normalise_dburi(url) is url


def test_normalise_dburi_returns_none_for_empty_sequence():
    assert normalise_dburi([]) is None


def test_normalise_dburi_rejects_multiple_urls():
    import pytest

    with pytest.raises(ValueError, match="exactly one --dburi"):
        normalise_dburi(
            [
                "informix+pyodbc://user:password@localhost/database_1",
                "informix+pyodbc://user:password@localhost/database_2",
            ]
        )


def test_render_safe_url_redacts_password():
    rendered = render_safe_url(
        "informix+pyodbc://informix:top-secret@localhost/database"
    )

    assert rendered is not None
    assert "top-secret" not in rendered
    assert "***" in rendered
