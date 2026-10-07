# Perfilado inicial

## 1. Objetivo y alcance

Este documento resume la exploración inicial de las fuentes entregadas para el proyecto **Cloud Provider Analytics**. El objetivo es describir su estructura, volumen de muestra, tipos, valores nulos, posibles duplicados, rangos temporales y anomalías que deberían tenerse en cuenta en el diseño.

El análisis se realizó en Google Colab con Python, pandas y la biblioteca estándar `json`. Se cargaron los CSV como texto para preservar los valores originales y luego se probaron conversiones numéricas en los campos seleccionados. Los espacios vacíos se interpretaron como valores nulos.

## 2. Inventario y calidad inicial de los CSV

Se analizaron siete archivos, con **4.112 filas en total**. El chequeo de duplicados del notebook busca filas completamente idénticas: no encontró duplicados completos en ninguno de los CSV. Esto no demuestra por sí solo la unicidad de cada clave de negocio.

| Archivo | Filas × columnas | Valores nulos encontrados | Observaciones |
|---|---:|---|---|
| `billing_monthly.csv` | 240 × 8 | `credits`: 137 | Hay 13 valores negativos en `subtotal` (En currency: 2 ARS, 1 EUR y 10 USD). Pueden corresponder a ajustes o reversos; podría requerir una validación sobre si estos conceptos deberían aplicar aca. |
| `customers_orgs.csv` | 80 × 11 | `nps_score`: 11 | `nps_score` tiene valores negativos y un máximo de 101. Confirmar si representa un NPS agregado u otra medida. |
| `marketing_touches.csv` | 1.500 × 7 | Ninguno | No se observaron filas completamente duplicadas. |
| `nps_surveys.csv` | 92 × 4 | `nps_score`: 19; `comment`: 10 | `nps_score` contiene valores negativos. Confirmar qué escala o indicador representa. |
| `resources.csv` | 400 × 7 | `tags_json`: 83 | El campo de etiquetas puede estar ausente. |
| `support_tickets.csv` | 1.000 × 8 | `resolved_at`: 240; `csat`: 254 | `csat` varía entre 0 y 7. La escala debe confirmarse antes de clasificar valores como inválidos. |
| `users.csv` | 800 × 7 | `last_login`: 139 | La ausencia de inicio de sesión puede ser válida para usuarios nuevos o inactivos; requiere interpretación funcional. |

Los campos numéricos revisados en los CSV (`nps_score`, `csat`, `subtotal`, `credits`, `taxes` y `exchange_rate_to_usd`) no presentaron valores no convertibles entre los valores informados.

### 2.1 Valores numéricos que requieren interpretación

- En `customers_orgs.csv`, `nps_score` va de -38 a 101; se observaron 16 valores negativos y un valor por encima de 100. Habria que definir si la escala va de -100 a 100 o estandarizar.
- En `nps_surveys.csv`, `nps_score` va de -16 a 68; se observaron 3 valores negativos. Misma situacion que al anteriior.
- En `support_tickets.csv`, `csat` va de 0 a 7. La escala usada no queda definida por el perfilado; no se consideran errores sin confirmación pero podemos suponer que es una escala de 0 a 10 en principio.
- En `billing_monthly.csv`, `subtotal` tiene valores negativos en las tres monedas. No deben eliminarse automáticamente: podrían representar ajustes.

## 3. Perfil de eventos JSONL

Se encontraron **120 archivos JSONL**, con **360 eventos por archivo**, para un total de **43.200 eventos**. La distribución uniforme es consistente con la fragmentación de archivos utilizada para simular micro-lotes.

El campo `timestamp` pudo convertirse en todos los registros. El rango observado es **2025-07-03 00:02 UTC a 2025-08-31 23:58 UTC**.

| Versión de esquema | Eventos | Período observado (UTC) | Campos opcionales relevantes |
|---|---:|---|---|
| `schema_version = 1` | 10.800 | 2025-07-03 00:02 a 2025-07-17 23:56 | No contiene `carbon_kg` ni `genai_tokens`. |
| `schema_version = 2` | 32.400 | 2025-07-18 00:01 a 2025-08-31 23:58 | Contiene `carbon_kg` en los 32.400 eventos y `genai_tokens` en 3.132. |

El campo `value` representa la medida, `metric` indica qué se midió y `unit` expresa la unidad:

| Métrica | Eventos | `value` informado | `value` ausente | `unit` ausente |
|---|---:|---:|---:|---:|
| `cpu_hours` | 10.692 | 10.497 | 195 | 505 |
| `requests` | 19.512 | 19.113 | 399 | 916 |
| `storage_gb_hours` | 12.996 | 12.713 | 283 | 654 |
| **Total** | **43.200** | **42.323** | **877** | **2.075** |

`value` aparece como número decimal en 41.014 casos y como texto en 1.309; los valores de texto revisados pudieron convertirse a número. `carbon_kg` también aparece con tipos enteros y decimales, ambos numéricos.
En el notebook hay mas evidencia sobre aque tipo de servicio le correspondía la falta de unidades, o de valores.

## 4. Valores extremos y costos

Se usó el límite superior de 1,5 veces el rango intercuartílico (IQR) para **señalar posibles valores atípicos**, no para declarar errores ni eliminarlos automáticamente.

En `value`, los casos por encima del límite IQR fueron: 41 en `cpu_hours`, 106 en `requests` y 51 en `storage_gb_hours`. No se observaron valores negativos en esas tres métricas, en `carbon_kg` ni en `genai_tokens`.

En `cost_usd_increment` se observaron **216 valores negativos**, con un mínimo de -154,4608. Como pueden corresponder a ajustes, deben investigarse antes de definir una regla de rechazo.

| Servicio | Eventos | Costos negativos | P99 | Máximo | Límite superior IQR | Posibles spikes sobre IQR |
|---|---:|---:|---:|---:|---:|---:|
| `compute` | 12.498 | 56 | 13,4853 | 302,3459 | 22,8021 | 30 |
| `networking` | 6.921 | 32 | 1,6755 | 29,6319 | 2,9151 | 15 |
| `storage` | 7.614 | 44 | 3,3390 | 62,9922 | 5,7572 | 14 |
| `database` | 7.419 | 44 | 8,3680 | 149,9091 | 14,3456 | 14 |
| `analytics` | 4.590 | 19 | 11,6100 | 249,3861 | 20,1051 | 9 |
| `genai` | 4.158 | 21 | 19,7118 | 317,4308 | 34,6157 | 5 |

Por ejemplo, en `analytics` el máximo de `cost_usd_increment` es 249,3861; el P99 es 11,6100 y el límite superior IQR es 20,1051. Esto evidencia posibles spikes que merecen análisis, pero no explica por sí solo su causa.

## 5. Relaciones y grano de las fuentes

Los CSV representan entidades o procesos distintos (organizaciones, usuarios, recursos, facturación, tickets, encuestas y contactos de marketing). Los JSONL representan eventos de uso. No se deben tratar como si los eventos ya estuvieran distribuidos dentro de los CSV.

Los campos `org_id` y `resource_id` permiten proponer relaciones entre fuentes cuando sus claves correspondan. **El notebook adjunto no contiene una comprobación explícita de claves únicas ni de referencias huérfanas**; agregar esa validación antes de afirmar que las relaciones están confirmadas. Lo mismo aplica a la unicidad de `event_id`.

## 6. Implicaciones para el diseño

- Conservar los archivos de origen sin modificaciones en Landing/Raw.
- Registrar `schema_version` y tratar la presencia de `carbon_kg` y `genai_tokens` según la versión y el contexto del evento.
- Normalizar tipos en una zona procesada y definir reglas para `value` y `unit` antes de calcular métricas.
- Marcar costos negativos y posibles spikes para revisión; no eliminarlos automáticamente.
- Distinguir facturación mensual (`billing_monthly.csv`) de costos incrementales (`cost_usd_increment` en eventos).
- El tamaño de la muestra permite explorar su calidad, pero no demuestra por sí solo el volumen productivo ni el rendimiento a escala.

## 7. Evidencia y reproducibilidad

- Notebook de exploración: `evidencia/exploracion_de_datos.ipynb`.

## 8. Pendientes de validación

- Seleccionar una sola ubicación de CSV en el notebook y regenerar los resultados.
- Validar unicidad de claves (`org_id`, `user_id`, `resource_id`, `ticket_id`, `invoice_id`, `touch_id` y `event_id`) según corresponda.
- Validar relaciones entre `org_id` y `resource_id` con sus tablas de referencia.
- Confirmar las escalas de `nps_score` y `csat`.
- Confirmar el significado de subtotales y costos negativos.
- Revisar una muestra de los spikes con contexto de organización, recurso, servicio y fecha.
