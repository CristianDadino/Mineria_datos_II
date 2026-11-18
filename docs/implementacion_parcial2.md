# Implementación — Parcial 2

Este documento registra lo que se ejecutó en Google Colab y en AstraDB para la segunda entrega. 


`No presenta como finalizada la carga en AstraDB ya que por cuestiones personales no he podido finalizarlo.`

## Resumen

• Se procesaron tres CSV maestros a Bronze, se leyó el conjunto de eventos JSONL mediante Structured Streaming y se generaron Silver, cuarentena y el mart Gold `org_daily_usage_by_service`. 

• AstraDB se creó como base Serverless no vectorial. 

• Desde Colab se validó la conexión usando un token de aplicación y un Secure Connect Bundle almacenados fuera del notebook. 

## Matriz de cumplimiento


| Componente mínimo | Evidencia/resultado observado | Estado |
|---|---|---|
| Correcciones y arquitectura | Se aclaró la separación Landing/Raw y Bronze y se ajustó el requisito de consulta de top servicios por organización. El diagrama de diseño representa el flujo batch/streaming y serving. | Actualizado en el diseño; falta adjuntar la evidencia de ejecución al repo |
| Batch a Bronze | `customers_orgs.csv`: 80 filas; `users.csv`: 800; `billing_monthly.csv`: 240. Se escribieron en Parquet con esquema explícito y columnas `ingest_ts`, `ingest_date`, `source_file`, particionadas por `ingest_date`. | Ejecutado y validado |
| Streaming a Bronze | 120 archivos JSONL, 43.200 eventos de entrada. La corrida final con watermark de 60 días escribió 43.200 eventos. Se verificó `event_id` distinto para los 43.200 del input y cero `event_id` nulos en la salida. | Ejecutado y validado para este dataset |
| Silver y enriquecimiento | 41.162 eventos quedaron en Silver después del control de unidad y del join con organizaciones; 2.038 registros fueron enviados a cuarentena. Silver se enriqueció con atributos de organización. | Ejecutado y validado |
| Calidad | 0 `event_id` nulos; 0 `org_id` sin correspondencia; 211 costos menores que -0,01 marcados como anomalía; 2.038 eventos con `value` presente y `unit` faltante enviados a cuarentena. | Ejecutado y validado |
| Gold | 11.050 grupos de organización, fecha y servicio; la suma de `events_count` es 41.162. Se representaron 206 anomalías de costo en Gold. | Ejecutado y validado |
| Serving Cassandra/AstraDB | Base activa; conexión Colab correcta; keyspace `cloud_provider` confirmado; sentencia de creación condicional de `org_daily_usage_by_service` ejecutada. Aún no se cargaron ni verificaron filas. | Parcial |
| Consultas CQL y resultados | No ejecutadas todavía. | Pendiente |
| Analítica o ML | No implementado todavía en esta instancia. | Pendiente |
| Quickstart, notebook E2 y evidencia | Carga parcial. | Pendiente |

## Flujo implementado

```text
CSV (3 maestros) ── batch PySpark ──> Bronze Parquet
JSONL (43.200 eventos) ── Structured Streaming ──> Bronze Parquet
Bronze ── reglas de calidad y join con organizaciones ──> Silver + cuarentena
Silver ── agregación diaria por organización y servicio ──> Gold
Gold ──> AstraDB (carga pendiente)
```

Los archivos de origen se mantienen en Landing/Raw. Bronze registra los datos tipados y los metadatos de ingesta. Silver conserva los eventos utilizables, marca anomalías y separa los registros que incumplen la regla de unidad. Gold genera una fila por organización, día y servicio.

## Parámetros y decisiones observadas

- **Watermark:** se usó 60 días en la corrida final para abarcar el rango temporal del dataset reproducido. Con 1 día, el procesamiento de archivos fuera del orden de event-time dejó una salida parcial de 7.811 eventos. El valor de 60 días es una decisión que se tomo para poder abarcar todo el rango de registros. En producción se debería establecer el retraso máximo aceptable con el negocio y medir el costo de estado.
- **Deduplicación:** `event_id` se utilizó como identificador del evento. En la entrada del dataset no se observaron duplicados: 43.200 identificadores distintos para 43.200 eventos. Por eso, en esta muestra no se cuantificó una reducción por deduplicación. La deduplicación queda como protección para reintentos y duplicados futuros.
- **Anomalías de costo:** los valores menores que -0,01 se conservaron con marca para revisión, no se descartaron automáticamente. Gold representa 206 anomalías; cinco de los 211 eventos marcados no aparecen allí porque también fueron enviados a cuarentena por falta de unidad.
- **Unidad faltante:** 2.038 eventos tenían `value` presente y `unit` faltante. Se separaron en cuarentena; no se afirmaron como registros limpios de Silver.
- **Carga idempotente a AstraDB:** todavía no verificada. La clave primaria de la tabla busca que volver a escribir la misma organización, mes, fecha y servicio actualice la misma fila en vez de crear una copia, pero debe demostrarse con una prueba de re-ejecución y conteos.

## Serving en AstraDB

La siguiente tabla se diseñó para el patrón de consulta de uso/costo por organización y período mensual. La partición usa `(org_id, usage_month)` y las filas se ordenan por fecha y servicio.

```sql
CREATE TABLE IF NOT EXISTS cloud_provider.org_daily_usage_by_service (
    org_id text,
    usage_month text,
    usage_date date,
    service text,
    org_name text,
    daily_cost_usd double,
    requests double,
    genai_tokens bigint,
    carbon_kg double,
    events_count int,
    cost_anomaly_events int,
    PRIMARY KEY ((org_id, usage_month), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
```

La conexión probada desde Colab utiliza el Cassandra Python driver. El token se guarda en Colab Secrets (`ASTRA_TOKEN`) y el SCB permanece como archivo privado de la sesión

### Consultas CQL previstas

Pendiente.

## Pendientes para cerrar la entrega

- [ ] Leer Gold desde Parquet, agregar `usage_month` y adaptar los nombres/tipos a la tabla CQL.
- [ ] Cargar el dataframe de Spark a AstraDB y verificar filas de muestra.
- [ ] Ejecutar las dos consultas CQL mínimas y guardar capturas sin secretos.
- [ ] Probar una re-ejecución de la carga para demostrar idempotencia.
- [ ] Incorporar el notebook ejecutable E2 al repositorio y documentar las dependencias/orden de ejecución.
- [ ] Completar la evidencia en `evidencia/parcial2/` y actualizar este estado según los resultados reales.
- [ ] Resolver el componente de analítica/ML o documentar una alternativa aceptada y su plan de cierre.
- [ ] Revisar el Quickstart desde una sesión limpia antes de darlo por reproducible.

## Protección de credenciales

No incluir en el repositorio ni en capturas el valor del token, el Secure Connect Bundle, variables de entorno con secretos ni celdas de salida que revelen credenciales. El nombre del secreto puede documentarse; su valor no.
