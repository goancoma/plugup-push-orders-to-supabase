# BigQuery Tracking Events Queries

Este directorio contiene las queries SQL para extraer eventos de tracking de órdenes desde BigQuery para cada marketplace, destinadas a alimentar la tabla `shipment_tracking` en Supabase.

## 📋 Resumen de Queries

| Marketplace | Archivo | Complejidad | Casos Especiales |
|-------------|---------|-------------|------------------|
| **MELI** | [`meli_tracking_events.sql`](meli_tracking_events.sql) | Media | logistic_type mapping, shipping_id interno |
| **FALA** | [`fala_tracking_events.sql`](fala_tracking_events.sql) | Alta | Array de items, prioridad de status |
| **WALM** | [`walm_tracking_events.sql`](walm_tracking_events.sql) | Alta | Order lines, múltiples statuses |
| **CENC** | [`cenc_tracking_events.sql`](cenc_tracking_events.sql) | Muy Alta | facilityConfigId mapping, sub-orders |

## 🗺️ Mapeo de Campos

### Campos Comunes (Todos los Marketplaces)

| Campo Supabase | Descripción | Fuente BigQuery |
|----------------|-------------|-----------------|
| `order_id` | UUID de orden en Supabase | Vinculación por `marketplace_order_id` |
| `company_id` | UUID de empresa | `company_id` (directo) |
| `event_status` | Estado crudo del envío | Extraído de JSON según marketplace |
| `event_timestamp` | Timestamp del evento | `processed_at` (proxy del evento) |
| `event_location` | Ciudad + región/estado | Combinación de campos de dirección |
| `courier_name` | Nombre del courier | Mapeo específico por marketplace |
| `tracking_number` | Número de seguimiento | Extraído de JSON, puede ser NULL |
| `notes` | Notas adicionales | substatus, cancel_reason, etc. |

## 📊 Detalles por Marketplace

### 🛒 MELI (MercadoLibre)

**Estructura de Datos:**
- Datos de envío en `shipping_info` JSON
- `logistic_type` indica el tipo de servicio de envío
- `receiver_address` contiene ubicación de entrega

**Campos Específicos:**
```sql
-- Estado del envío
JSON_EXTRACT_SCALAR(shipping_info, '$.status') as event_status

-- Ubicación (ciudad + estado)
CONCAT(
  JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.city'), 
  ', ', 
  JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.state')
) as event_location

-- Courier (mapeo de logistic_type)
CASE JSON_EXTRACT_SCALAR(shipping_info, '$.logistic_type')
  WHEN 'fulfillment' THEN 'Mercado Envíos Full'
  WHEN 'self_service' THEN 'Mercado Envíos Flex'
  WHEN 'cross_docking' THEN 'Mercado Envíos Colecta'
  -- ...
END as courier_name
```

**Casos Especiales:**
- `tracking_number` puede ser `shipping_id` interno de MELI
- `logistic_type` requiere mapeo a nombres descriptivos
- Solo órdenes con `enrichment_status = 'COMPLETE'`

### 🏪 FALA (Falabella)

**Estructura de Datos:**
- Array de `items` en JSON, cada item tiene su `status`
- `shipping_address` está en `buyer_info`
- Múltiples items pueden tener diferentes status

**Campos Específicos:**
```sql
-- Status más avanzado (lógica de prioridad)
CASE 
  WHEN EXISTS (SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
               WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'delivered')
  THEN 'delivered'
  -- ... más casos
END as prioritized_status

-- Ubicación del shipping_address
CONCAT(
  JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.City'), 
  ', ', 
  JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.State')
) as event_location
```

**Casos Especiales:**
- **Array de Items**: Cada item tiene su propio status
- **Prioridad de Status**: Se usa el status más avanzado según lógica de negocio
- **Cancel Reasons**: Se extraen de items individuales
- **Tracking Code**: Puede estar en `shipping_info.tracking_code`

### 🛍️ WALM (Walmart)

**Estructura de Datos:**
- Array de `items` (order lines) con `orderLineStatuses`
- `shipping_info` contiene `postal_address`
- Tracking info puede estar a nivel de item o general

**Campos Específicos:**
```sql
-- Status más avanzado de order lines
CASE 
  WHEN EXISTS (SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
               WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'delivered')
  THEN 'Delivered'
  -- ... más casos
END as prioritized_status

-- Ubicación del postal_address
CONCAT(
  JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.city'), 
  ', ', 
  JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.state')
) as event_location
```

**Casos Especiales:**
- **Order Lines**: Cada línea de orden puede tener múltiples statuses
- **Carrier Info**: Puede estar a nivel de item individual
- **Tracking Number**: Prioridad item > shipping_info general
- **Method Code**: Se mapea a courier names

### 🏬 CENC (Cencosud/Paris)

**Estructura de Datos:**
- Array de `items` (sub-orders) representan unidades de fulfillment
- `facilityConfigId` requiere mapeo específico a couriers
- `shipping_address` está en cada sub-order individual

**Campos Específicos:**
```sql
-- Status de sub-orders (en español)
CASE 
  WHEN EXISTS (SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
               WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%entregada%')
  THEN 'Entregada'
  -- ... más casos
END as prioritized_status

-- Mapeo de facilityConfigId
CASE facility_config_id
  WHEN 1 THEN 'Dropshipping'
  WHEN 3 THEN 'Blue Express'
  WHEN 4 THEN 'Intangibles'
  WHEN 7 THEN 'Paris Fulfillment'
END as courier_name
```

**Casos Especiales:**
- **Sub-Orders**: Representan unidades de fulfillment, no productos individuales
- **facilityConfigId**: Mapeo crítico para identificar courier
- **Tracking Number**: Frecuentemente NULL
- **Status en Español**: Requiere parsing de texto descriptivo
- **Enrichment Status**: Siempre PARTIAL por limitaciones de API

## 🔄 Transformaciones Requeridas en Python

### 1. Normalización de Estados

Cada marketplace usa diferentes valores para `event_status`. La Cloud Function debe normalizar a estados estándar de PlugUp:

```python
PLUGUP_STATUS_MAPPING = {
    'pending': ['pending', 'pendiente', 'created', 'acknowledged'],
    'ready_to_ship': ['ready_to_ship', 'lista para despacho', 'handling'],
    'dispatched': ['shipped', 'enviado', 'en tránsito'],
    'in_transit': ['shipped', 'en tránsito', 'transit'],
    'delivered': ['delivered', 'entregada', 'entregado'],
    'cancelled': ['cancelled', 'canceled', 'cancelada', 'cancelado'],
    'returned': ['returned', 'devuelto']
}
```

### 2. Validación de Órdenes

```python
# Verificar que marketplace_order_id existe en Supabase
def validate_order_exists(marketplace_order_id: str, marketplace: str) -> bool:
    # Query a Supabase orders table
    # WHERE marketplace_order_id = ? AND marketplace = ?
    pass
```

### 3. Manejo de Tracking Numbers

```python
def process_tracking_number(tracking_number: str, marketplace: str, order_data: dict) -> str:
    if tracking_number:
        return tracking_number
    
    # Fallbacks por marketplace
    if marketplace == 'MELI':
        return order_data.get('shipping_id', 'N/A')
    elif marketplace == 'CENC':
        return order_data.get('sub_order_number', 'N/A')
    
    return 'N/A'
```

## ⚠️ Limitaciones y Consideraciones

### Limitaciones por Marketplace

| Marketplace | Limitación | Impacto | Solución |
|-------------|------------|---------|----------|
| **MELI** | tracking_number puede ser interno | Tracking limitado | Usar shipping_id como fallback |
| **FALA** | Múltiples items con diferentes status | Complejidad de status | Lógica de prioridad implementada |
| **WALM** | Order lines vs productos | Granularidad | Usar order line como unidad |
| **CENC** | facilityConfigId mapping | Courier desconocido | Tabla de mapeo actualizable |

### Consideraciones Técnicas

1. **Event Timestamp**: Se usa `processed_at` como proxy del timestamp real del evento
2. **Filtro de Tiempo**: 20 minutos para capturar eventos recientes
3. **Enrichment Status**: CENC siempre es PARTIAL, otros deben ser COMPLETE
4. **Performance**: Queries optimizadas con clustering por marketplace y company_id

## 🚀 Uso en Cloud Function

### Estructura Recomendada

```python
async def extract_tracking_events():
    events = []
    
    for marketplace in ['MELI', 'FALA', 'WALM', 'CENC']:
        query = load_query(f"{marketplace.lower()}_tracking_events.sql")
        results = await bigquery_client.query(query)
        
        for row in results:
            event = {
                'marketplace_order_id': row.marketplace_order_id,
                'company_id': row.company_id,
                'event_status': normalize_status(row.event_status, marketplace),
                'event_timestamp': row.event_timestamp,
                'event_location': row.event_location,
                'courier_name': row.courier_name,
                'tracking_number': process_tracking_number(row.tracking_number, marketplace, row),
                'notes': row.notes
            }
            events.append(event)
    
    return events
```

### Payload para Supabase Edge Function

```json
{
  "events": [
    {
      "marketplace_order_id": "MELI-123456",
      "marketplace": "meli",
      "company_id": "uuid-bamo",
      "event_status": "delivered",
      "event_timestamp": "2026-01-27T14:30:00Z",
      "event_location": "Santiago, Región Metropolitana",
      "courier_name": "Mercado Envíos Flex",
      "tracking_number": "SHIP-789",
      "notes": "Entregado exitosamente"
    }
  ]
}
```

## 📝 Mantenimiento

### Actualizaciones Requeridas

1. **Mapeo de Couriers**: Actualizar cuando se agreguen nuevos facilityConfigId en CENC
2. **Estados Nuevos**: Agregar nuevos status según evolución de APIs
3. **Campos Adicionales**: Modificar queries si se requieren nuevos campos
4. **Performance**: Monitorear y optimizar queries según volumen de datos

### Monitoreo

- **Eventos Procesados**: ~50-200 eventos por ejecución
- **Latencia**: < 5 segundos por query
- **Cobertura**: 100% de órdenes activas deben tener eventos
- **Errores**: < 1% de tasa de error