# Changelog

## 1.2.0

### Corregido

- `INTERVAL` deja de reflejarse como `sqlalchemy.Interval` y de recompilarse
  erróneamente como `DATETIME YEAR TO FRACTION(5)`.
- Nuevo tipo nativo `IfxAlchemy.INTERVAL` con calificadores y precisiones.
- Procesadores de valores para intervalos año-mes y día-tiempo.
- Reflexión enriquecida mediante ODBC `SQLColumns` y fallback de
  `SYSCOLUMNS.collength`.
- Comparación y renderizado específicos para Alembic autogenerate.
- Los tests live son opt-in mediante `--run-informix`; `pytest` funciona sin
  servidor.
- El rango estable se limita a `SQLAlchemy>=2.0.45,<2.1`.
- Contrato explícito entre banderas del dialecto y `SuiteRequirements`.
- Procedencia JSON/JUnit y matriz mínima de certificación.
- Eliminación de `.env` reales y artefactos generados del entregable.
- Constructor de ZIP reproducible por lista blanca y escáner de secretos.
- El constructor valida archivos obligatorios y funciona como módulo o script.
- El escáner inspecciona ZIP, wheel y tar comprimido con límites de seguridad.
- El CI marca SQLAlchemy 2.1 como experimental y valida los artefactos finales.
- Se fija un umbral de cobertura offline del 75 % y una puerta estática con Ruff.
