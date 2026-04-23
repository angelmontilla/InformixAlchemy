from setuptools import setup, find_packages

setup(
    name="IfxAlchemy",
    version="1.0.1",
    description="Informix dialect for SQLAlchemy using pyodbc",
    author="Angel Montilla",
    python_requires=">=3.10",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "SQLAlchemy>=2.0,<2.3",
        "pyodbc>=5.0",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "informix.pyodbc = IfxAlchemy.pyodbc:IfxDialect_pyodbc",
            "informix.ifxpy = IfxAlchemy.IfxPy:IfxDialect_IfxPy",
            "informix = IfxAlchemy.pyodbc:IfxDialect_pyodbc",
        ]
    },
    zip_safe=False,
)
