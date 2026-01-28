# Requerimiento: Integración Shipment Tracking - GCP BigQuery → Supabase

## 🎯 Objetivo

Crear una Cloud Function que consulte periódicamente BigQuery para obtener eventos de tracking de órdenes y los envíe a Supabase (tabla `shipment_tracking`) para alimentar el módulo Last Mile de PlugUp.

---

## 📦 Contexto

**PlugUp** es una plataforma multi-tenant de logística para e-commerce en Chile. Actualmente procesa webhooks de 4 marketplaces (MELI, FALA, WALM, CENC) y almacena data enriquecida en BigQuery.

**Módulo Last Mile** (en producción) necesita visibilidad en tiempo casi-real del estado de envíos para:
- Mostrar timeline de eventos por orden
- Calcular riesgo de entregas (% a tiempo)
- Monitorear órdenes en tránsito

**Flujo actual:**
```
Marketplace Webhook → GCP Cloud Run → n8n → Supabase (tabla orders)
                                    ↓
                              BigQuery (data enriquecida)
```

**Flujo requerido (nuevo):**
```
BigQuery (data enriquecida) → Cloud Function (cada 15 min) → Supabase Edge Function → shipment_tracking table
```

---

## 🗄️ Tabla Destino: `shipment_tracking`

**Ubicación:** Supabase PostgreSQL  
**Esquema:**
```sql
CREATE TABLE shipment_tracking (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id),
  company_id UUID NOT NULL REFERENCES companies(id),
  event_status TEXT NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  event_location TEXT,
  courier_name TEXT,
  tracking_number TEXT,
  notes TEXT,
  received_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Indexes existentes:**
- `idx_shipment_tracking_order_id` (order_id)
- `idx_shipment_tracking_company_id` (company_id)
- `idx_shipment_tracking_event_timestamp` (event_timestamp)

**RLS Policies:** Filtrado por `company_id` (multi-tenant)

---

## 🔧 Estados Normalizados

**Importante:** Normalizar estados de cada marketplace a valores estándar de PlugUp.

| Estado PlugUp | MELI | FALA | WALM | CENC |
|---------------|------|------|------|------|
| `pending` | `pending` | `pending` | `Created` | (inicial) |
| `ready_to_ship` | `handling`, `ready_to_ship` | `ready_to_ship` | `Acknowledged` | (post-picking) |
| `dispatched` | `shipped` (inicio) | `shipped` (inicio) | `Shipped` (inicio) | (courier asignado) |
| `in_transit` | `shipped` (en ruta) | `shipped` (en ruta) | `Shipped` (en ruta) | (en tránsito) |
| `out_for_delivery` | (no distingue) | (no distingue) | (no distingue) | (no distingue) |
| `delivered` | `delivered` | `delivered` | `Delivered` | (entregado) |
| `delivery_failed` | `not_delivered` | (ver `ReasonDetail`) | (ver `cancellationReason`) | (excepción) |
| `cancelled` | `cancelled` | `canceled` | `Cancelled` | (cancelado) |
| `returned` | (ver `substatus`) | `returned` | (no aplica) | (devuelto) |

**Nota:** Algunos estados intermedios (`in_transit`, `out_for_delivery`) NO son distinguibles con data actual de marketplaces. Se infieren o se omiten en Plan A.

---

## 🚀 Requerimiento Técnico

### **Cloud Function: `sync-shipment-tracking`**

**Trigger:** Cloud Scheduler (cada 15 minutos)  
**Runtime:** Python 3.11 (o el que uses actualmente)  
**Región:** us-central1 (o donde tengas BigQuery)

**Flujo:**

1. **Consultar BigQuery** para órdenes con cambios de estado en últimos 20 minutos
   - Query: Buscar registros con `last_updated_at > NOW() - INTERVAL 20 MINUTE`
   - Filtrar por órdenes que ya existen en Supabase (tabla `orders`)

2. **Normalizar estados** según tabla de mapeo (arriba)

3. **Preparar payload** para Supabase Edge Function:
```json
   {
     "events": [
       {
         "marketplace_order_id": "MELI-123456",
         "marketplace": "meli",
         "company_id": "uuid-bamo",
         "event_status": "delivered",
         "event_timestamp": "2026-01-26T14:30:00Z",
         "event_location": "Santiago, Región Metropolitana",
         "courier_name": "Mercado Envíos Flex",
         "tracking_number": "SHIP-789",
         "notes": "Entregado exitosamente"
       }
     ]
   }
```

4. **Enviar a Supabase Edge Function** (POST `/functions/v1/process-shipment-tracking`)
   - Incluir header `Authorization: Bearer {SUPABASE_SERVICE_ROLE_KEY}`
   - Manejar reintentos (max 3) con backoff exponencial

5. **Logging:** Registrar en GCP Cloud Logging:
   - Cantidad de eventos procesados
   - Errores de consulta BQ o envío a Supabase
   - Órdenes sin `order_id` válido en Supabase (skip)

---

## 🔐 Seguridad

**Credenciales requeridas:**
- `SUPABASE_URL`: URL del proyecto Supabase
- `SUPABASE_SERVICE_ROLE_KEY`: Service role key (permisos RLS bypass)
- BigQuery ya tiene acceso por defecto (Service Account de Cloud Function)

**Almacenamiento:** variables de entorno

---

## 📈 Métricas de Éxito

**KPIs a monitorear:**
1. **Latencia:** < 5 segundos por ejecución de Cloud Function
2. **Eventos procesados:** ~50-200 eventos por ejecución (depende de volumen de órdenes)
3. **Tasa de error:** < 1% (reintentos incluidos)


---

## 📝 Entregables

1. **Cloud Function:** `sync-shipment-tracking` (Python)
2. **Cloud Scheduler Job:** Configurado para ejecutar cada 15 min
3. **Logs estructurados:** Cloud Logging con nivel INFO/ERROR

---

## ✅ Checklist de Implementación

- [ ] Crear Cloud Function `sync-shipment-tracking`
- [ ] Configurar Cloud Scheduler (cada 15 min)
- [ ] Implementar lógica de normalización de estados
- [ ] Implementar lógica de vinculación `marketplace_order_id → order_id`
- [ ] Configurar reintentos y error handling
