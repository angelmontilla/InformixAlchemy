import pytest
from sqlalchemy import bindparam, outparam, text
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing import config, fixtures


class OutParamTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        if config.db.dialect.driver == "pyodbc":
            pytest.skip("pyodbc does not expose DBAPI callproc/out parameters")

        with config.db.begin() as conn:
            conn.exec_driver_sql("""
                    create procedure foo(x_in integer, OUT x_out integer, OUT y_out integer, OUT z_out varchar(20))
                    LET x_out = 10;
                    LET y_out = x_in * 15;
                    LET z_out = NULL;
                    END PROCEDURE
                        """)

    def test_out_params(self):
        with config.db.begin() as conn:
            stmt = text('call foo(:x_in, :x_out, :y_out, :z_out)').bindparams(
                bindparam('x_in'),
                outparam('x_out'),
                outparam('y_out'),
                outparam('z_out'),
            )
            result = conn.execute(
                stmt,
                {'x_in': 5, 'x_out': 0, 'y_out': 0, 'z_out': ''},
            )
        eq_(result.out_parameters, {'x_out': 10, 'y_out': 75, 'z_out': None})
        assert isinstance(result.out_parameters['x_out'], int)

    @classmethod
    def teardown_class(cls):
        with config.db.begin() as conn:
            conn.exec_driver_sql("DROP PROCEDURE foo")
