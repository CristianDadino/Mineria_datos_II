# Cloud Provider Analytics

Proyecto integrador de Minería de Datos II — ISTEA, 2C 2026.

## Integrante

Dadino Cristian

## Objetivo

Construir un MVP reproducible para integrar fuentes batch CSV y eventos JSONL, procesarlos con PySpark y publicar métricas de uso y costos para consultas analíticas.

## Estado de la segunda entrega

El procesamiento principal del dataset fue ejecutado en Google Colab. El informe de avance detalla resultados comprobados y tareas pendientes. La base AstraDB está activa, la conexión desde Colab fue comprobada, existe el keyspace `cloud_provider` y se ejecutó la creación condicional de la tabla de serving. **La carga de las filas del mart a AstraDB y las consultas CQL todavía están pendientes de verificación.**

| Componente | Estado resumido |
|---|---|
| Batch a Bronze (3 CSV) | Ejecutado y validado |
| Streaming a Bronze | Ejecutado sobre 43.200 eventos con watermark de 60 días; corrida final escribió 43.200 |
| Silver y cuarentena | Ejecutado: 41.162 en Silver y 2.038 en cuarentena |
| Calidad | Reglas ejecutadas; resultados y límites documentados |
| Gold | Ejecutado: 11.050 grupos diarios por organización y servicio |
| AstraDB / serving | Conexión, keyspace y DDL comprobados; carga y CQL pendientes |
| Notebook E2, evidencias y Quickstart reproducible | Pendientes de incorporar al repositorio |

## Documentación

- [Diseño del proyecto](docs/diseno.md)
- [Perfilado de datos](docs/perfilado_datos.md)
- [Avance e implementación de la segunda entrega](docs/implementacion_parcial2.md)

## Fuentes

Los CSV y archivos JSONL se entregan como dataset del proyecto y no se incluyen en este repositorio. Consultar `docs/perfilado_datos.md` para el inventario y los hallazgos observados.

## Evidencias

La carpeta `evidencia/` conserva capturas de la exploración de la primera entrega. Agregar en `evidencia/parcial2/` capturas del notebook ejecutado, los conteos de validación y las consultas CQL cuando la carga en AstraDB quede completa. No incluir tokens, Secure Connect Bundles (SCB), archivos de datos privados ni salidas que expongan credenciales.

## Reproducibilidad

El notebook de implementación de E2 y sus instrucciones de ejecución deben incorporarse antes de considerar cerrado el Quickstart. El estado actual, los parámetros relevantes y las tareas pendientes están detallados en [docs/implementacion_parcial2.md](docs/implementacion_parcial2.md).
