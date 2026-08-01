from __future__ import annotations

import pytest


pytestmark = [
    pytest.mark.requires_informix,
    pytest.mark.optional_probe_isolation,
]


def _metadata_value(row, attribute, index):
    value = getattr(row, attribute, None)
    if value is not None:
        return value
    return row[index]


def _error_details(error: Exception) -> dict[str, object]:
    return {
        "class": type(error).__name__,
        "args": getattr(error, "args", ()),
        "text": str(error),
    }


def test_lvarchar_pyodbc_binding_diagnostic(engine, name_factory):
    """Record the real IBM Informix ODBC behavior for LVARCHAR parameters.

    IBM documents LVARCHAR as ODBC SQL_VARCHAR and TEXT as
    SQL_LONGVARCHAR. The test records the driver's native metadata and keeps
    inference plus alternative codes as diagnostic evidence, while requiring
    SQL_VARCHAR to round-trip short, long, and multibyte Python strings.
    """

    table_name = name_factory("sa_lvarchar_diag_")
    short_value = "LVARCHAR corto"
    long_value = "x" * 512
    multibyte_value = "áéíóúñ" * 80
    max_value = "x" * 32739
    samples = {
        "short": short_value,
        "over_255_bytes": long_value,
        "multibyte_over_255_bytes": multibyte_value,
        "max_32739_bytes": max_value,
    }

    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"CREATE TABLE {table_name} (payload LVARCHAR(32739))"
        )

    raw_connection = engine.raw_connection()
    try:
        import pyodbc

        metadata_row = None
        metadata_cursor = raw_connection.cursor()
        try:
            for metadata_table_name in (table_name, table_name.upper()):
                metadata_row = metadata_cursor.columns(
                    table=metadata_table_name,
                    column="payload",
                ).fetchone()
                if metadata_row is not None:
                    break
        finally:
            metadata_cursor.close()

        assert metadata_row is not None

        odbc_data_type = _metadata_value(metadata_row, "data_type", 4)
        native_type_name = _metadata_value(metadata_row, "type_name", 5)
        declared_column_size = _metadata_value(metadata_row, "column_size", 6)
        char_octet_length = _metadata_value(
            metadata_row,
            "char_octet_length",
            15,
        )

        print("LVARCHAR cursor.columns metadata:")
        print(f"  data_type (ODBC code): {odbc_data_type!r}")
        print(f"  type_name: {native_type_name!r}")
        print(f"  column_size: {declared_column_size!r}")
        print(f"  char_octet_length: {char_octet_length!r}")

        candidates: list[tuple[str, int | None]] = [("inference", None)]
        for constant_name in (
            "SQL_VARCHAR",
            "SQL_LONGVARCHAR",
            "SQL_WVARCHAR",
            "SQL_WLONGVARCHAR",
        ):
            code = getattr(pyodbc, constant_name, None)
            if code is not None:
                candidates.append((constant_name, code))

        if isinstance(odbc_data_type, int) and all(
            code != odbc_data_type for _, code in candidates
        ):
            candidates.append(("cursor.columns.data_type", odbc_data_type))

        results: dict[str, dict[str, object]] = {}

        for candidate_name, candidate_code in candidates:
            candidate_results: dict[str, object] = {}

            for sample_name, value in samples.items():
                cursor = raw_connection.cursor()
                try:
                    cursor.execute(f"DELETE FROM {table_name}")
                    if candidate_code is not None:
                        cursor.setinputsizes(
                            [
                                (
                                    candidate_code,
                                    len(value.encode("utf-8")),
                                    0,
                                )
                            ]
                        )

                    cursor.execute(
                        f"INSERT INTO {table_name} (payload) VALUES (?)",
                        value,
                    )
                    cursor.execute(
                        f"SELECT FIRST 1 payload FROM {table_name}"
                    )
                    row = cursor.fetchone()
                    received = row[0]
                    description = cursor.description
                    raw_connection.commit()

                    candidate_results[sample_name] = {
                        "ok": True,
                        "sent_python_type": type(value).__name__,
                        "sent_characters": len(value),
                        "sent_bytes_utf8": len(value.encode("utf-8")),
                        "received_python_type": type(received).__name__,
                        "received_characters": len(received),
                        "cursor_description": description,
                    }

                    assert received == value
                    assert isinstance(received, str)
                except Exception as error:
                    raw_connection.rollback()
                    candidate_results[sample_name] = {
                        "ok": False,
                        "setinputsizes_code": candidate_code,
                        "error": _error_details(error),
                    }
                finally:
                    cursor.close()

            results[candidate_name] = candidate_results

        print("LVARCHAR pyodbc binding matrix:")
        for candidate_name, candidate_results in results.items():
            print(f"  {candidate_name}:")
            for sample_name, result in candidate_results.items():
                print(f"    {sample_name}: {result!r}")

        sql_varchar_results = results.get("SQL_VARCHAR")
        assert sql_varchar_results is not None
        assert all(result["ok"] for result in sql_varchar_results.values())

        # Inference remains diagnostic only.  pyodbc may classify long
        # Python strings as SQL_LONGVARCHAR, which Informix maps to TEXT.
        # The dialect therefore requires the explicit SQL_VARCHAR path above.

        # cursor.description is part of the requested evidence and reports the
        # Python-facing result type selected by pyodbc for the native column.
        description = sql_varchar_results["short"]["cursor_description"]
        assert description
        assert description[0][1] is str
    finally:
        raw_connection.close()
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP TABLE {table_name}")
