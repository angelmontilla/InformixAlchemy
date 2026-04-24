from __future__ import annotations

from sqlalchemy.engine import URL, make_url

from IfxAlchemy.pyodbc import IfxDialect_pyodbc, _quote_odbc_value


def _connect_string(url):
    args, kwargs = IfxDialect_pyodbc().create_connect_args(make_url(url))

    assert kwargs == {}
    assert len(args) == 1
    return args[0]


def test_uid_pwd_connection_string_uses_informix_keywords():
    connstr = _connect_string(
        "informix+pyodbc://informix:in4mix@127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&protocol=onsoctcp"
        "&server=informix"
        "&service=9088"
    )

    assert connstr == (
        "DRIVER={IBM INFORMIX ODBC DRIVER (64-bit)};"
        "HOST=127.0.0.1;"
        "SERVICE=9088;"
        "SERVER=informix;"
        "DATABASE=stores;"
        "PROTOCOL=onsoctcp;"
        "UID=informix;"
        "PWD=in4mix;"
        "NeedODBCTypesOnly=1"
    )
    assert "Trusted_Connection" not in connstr


def test_dsn_connection_string():
    connstr = _connect_string(
        "informix+pyodbc://scott:tiger@/?DSN=ifx_dev"
    )

    assert connstr == (
        "DSN=ifx_dev;UID=scott;PWD=tiger;NeedODBCTypesOnly=1"
    )


def test_odbc_connect_passthrough_is_not_modified():
    connstr = _connect_string(
        "informix+pyodbc:///?odbc_connect="
        "DRIVER%3D%7BExisting%7D%3BHOST%3Difx%3BUID%3Duser"
    )

    assert connstr == "DRIVER={Existing};HOST=ifx;UID=user"


def test_odbc_connect_preserves_encoded_plus():
    dialect = IfxDialect_pyodbc()
    url = make_url(
        "informix+pyodbc:///?odbc_connect="
        "DRIVER%3D%7BIBM%7D%3BPWD%3Da%2Bb"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["DRIVER={IBM};PWD=a+b"]
    assert kwargs == {}


def test_odbc_connect_preserves_literal_plus_when_already_decoded_by_url():
    dialect = IfxDialect_pyodbc()
    url = URL.create(
        "informix+pyodbc",
        query={"odbc_connect": "PWD=a+b"},
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["PWD=a+b"]
    assert kwargs == {}


def test_trusted_context_alias_renders_tctx_keyword():
    connstr = _connect_string(
        "informix+pyodbc://127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&protocol=onsoctcp"
        "&server=informix"
        "&service=9088"
        "&trusted_context=true"
    )

    assert "TCTX=1" in connstr
    assert "Trusted_Connection" not in connstr
    assert "UID=" not in connstr
    assert "PWD=" not in connstr


def test_tctx_query_keyword_renders_trusted_context():
    connstr = _connect_string(
        "informix+pyodbc://127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&tctx=1"
    )

    assert "TCTX=1" in connstr
    assert "Trusted_Connection" not in connstr


def test_no_auth_keywords_are_added_without_user_or_trusted_context():
    connstr = _connect_string(
        "informix+pyodbc://127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&protocol=onsoctcp"
        "&server=informix"
        "&service=9088"
    )

    assert "Trusted_Connection" not in connstr
    assert "UID=" not in connstr
    assert "PWD=" not in connstr
    assert "TCTX=" not in connstr


def test_need_odbc_types_only_is_not_duplicated_case_insensitively():
    connstr = _connect_string(
        "informix+pyodbc://informix:in4mix@127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&needodbctypesonly=0"
    )

    assert connstr.count("NeedODBCTypesOnly=") == 1
    assert "NeedODBCTypesOnly=0" in connstr


def test_known_query_keywords_are_case_insensitive():
    connstr = _connect_string(
        "informix+pyodbc://informix:in4mix@127.0.0.1/stores"
        "?DRIVER=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&delimident=Y"
        "&autotranslate=No"
    )

    assert "DRIVER={IBM INFORMIX ODBC DRIVER (64-bit)}" in connstr
    assert "DELIMIDENT=Y" in connstr
    assert "AutoTranslate=No" in connstr


def test_odbc_value_escaping_for_semicolon_braces_and_outer_spaces():
    assert _quote_odbc_value("has;semicolon") == "{has;semicolon}"
    assert _quote_odbc_value("has{open") == "{has{open}"
    assert _quote_odbc_value("has}close") == "{has}}close}"
    assert _quote_odbc_value(" padded ") == "{ padded }"


def test_connection_string_escapes_values_with_special_characters():
    connstr = _connect_string(
        "informix+pyodbc://informix:p%7Dwd@127.0.0.1/stores"
        "?driver=IBM+INFORMIX+ODBC+DRIVER+(64-bit)"
        "&server=srv%3Bone"
        "&service=%209088%20"
    )

    assert "PWD={p}}wd}" in connstr
    assert "SERVER={srv;one}" in connstr
    assert "SERVICE={ 9088 }" in connstr
