# Cloud Provider Analytics

## 1. Objetivo

Diseñar una solución de datos para analizar costos, consumo y soporte
de un proveedor de servicios cloud.

## 2. Inventario y perfil inicial de fuentes

A continuación se resume la exploración inicial de las fuentes.
La tabla de la consigna describe qué contiene cada archivo; aquí se
agregan los resultados observados al revisar los datos.

| Fuente | Filas | Grano probable / clave | Período observado | Hallazgos de calidad |
|---|---:|---|---|---|
| `customers_orgs.csv` | 80 | Una organización / `org_id` | `signup_date`: 04/05–02/07/2025 | 11 `nps_score` nulos. El rango del puntaje requiere aclarar si es una respuesta individual o un índice agregado. |
| `users.csv` | 800 | Un usuario / `user_id` | `created_at`: 04/05–11/08/2025 | 139 `last_login` nulos. |
| `resources.csv` | 400 | Un recurso / `resource_id` | `created_at`: 04/05–21/08/2025 | 83 `tags_json` nulos. |
| `support_tickets.csv` | 1.000 | Un ticket / `ticket_id` | Creación: 09/05–31/08/2025 | 240 `resolved_at` y 254 `csat` nulos. `csat` va de 0 a 7; hay que validar su escala. |
| `marketing_touches.csv` | 1.500 | Una interacción / `touch_id` | 04/05–31/08/2025 | No se encontraron nulos ni duplicados completos. |
| `nps_surveys.csv` | 92 | Una encuesta por organización y fecha | 24/05–31/08/2025 | 19 `nps_score` y 10 comentarios nulos. Los valores de NPS requieren confirmar su significado. |
| `billing_monthly.csv` | 240 | Una factura por organización y mes / `invoice_id` | Junio–agosto de 2025 | 137 `credits` nulos y 13 `subtotal` negativos. Conviene validar el significado de los negativos. |
| `usage_events_stream/*.jsonl` | 43.200 | Un evento; `event_id` es candidato a identificador | Incluye `timestamp`; desde 2025-07-03 a 2025-08-31 (corte en 2025-07-17 por cambio de version) | `value` llega como decimal o texto, aunque los textos pudieron convertirse a número; faltan 877 valores `value` y 2.075 `unit`. Encontramos spikes en la mayoria de los services. |
