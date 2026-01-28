-- =====================================================
-- CENC (Cencosud/Paris) Tracking Events Query
-- =====================================================
-- 
-- Extrae eventos de tracking de órdenes CENC de los últimos 20 minutos
-- para enviar a Supabase shipment_tracking table
--
-- Campos mapeados:
-- - marketplace_order_id: marketplace_order_id (order number)
-- - company_id: company_id (del seller)
-- - event_status: item.status_name (sin normalizar, extraído del array items/sub-orders)
-- - event_timestamp: processed_at (timestamp del evento)
-- - event_location: CONCAT(city, region) del shipping_address
-- - courier_name: facilityConfigId mapeado a nombre del courier
-- - tracking_number: item.tracking_number (puede ser NULL)
-- - notes: item.status_description o fulfillment notes
--
-- Casos especiales:
-- - CENC usa sub-orders como items, cada sub-order es una unidad de fulfillment
-- - facilityConfigId requiere mapeo específico a courier names
-- - tracking_number puede no existir nativamente
-- - shipping_address está dentro de cada sub-order/item
--

WITH raw_enriched_orders AS (
  SELECT
    *
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY marketplace_order_id ORDER BY processed_at DESC) AS rn
    FROM `plugup_data.orders_enriched`
  )
  WHERE rn = 1
), 
cenc_facility_mapping AS (
  -- Tabla de mapeo para facilityConfigId -> courier_name
  -- Esto debería idealmente estar en una tabla separada en BigQuery
  SELECT facility_id, courier_name FROM UNNEST([
    STRUCT(1 as facility_id, 'Dropshipping' as courier_name),
    STRUCT(3 as facility_id, 'Blue Express' as courier_name),
    STRUCT(4 as facility_id, 'Intangibles' as courier_name),
    STRUCT(7 as facility_id, 'Paris Fulfillment' as courier_name)
  ])
),

cenc_suborder_statuses AS (
  SELECT 
    order_id,
    marketplace_order_id,
    company_id,
    processed_at,
    
    -- Extraer todos los sub-orders/items para determinar el status más avanzado
    JSON_EXTRACT_ARRAY(items) as items_array,
    
    -- Determinar el status más avanzado usando lógica de prioridad
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%entregada%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%delivered%'
      ) THEN 'Entregada'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%tránsito%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%shipped%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%transit%'
      ) THEN 'En Tránsito'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%lista%despacho%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%ready%'
      ) THEN 'Lista para Despacho'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%cancelada%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%cancelled%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%deleted%'
      ) THEN 'Cancelada'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%pendiente%'
           OR LOWER(JSON_EXTRACT_SCALAR(item, '$.status_name')) LIKE '%pending%'
      ) THEN 'Pendiente'
      ELSE 'Desconocido'
    END as prioritized_status,
    
    -- Extraer facilityConfigId del primer sub-order
    CAST((
      SELECT JSON_EXTRACT_SCALAR(item, '$.facility_config_id')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.facility_config_id') IS NOT NULL
      LIMIT 1
    ) AS INT64) as facility_config_id,
    
    -- Extraer tracking_number si existe
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.tracking_number')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.tracking_number') IS NOT NULL
      LIMIT 1
    ) as tracking_number,
    
    -- Extraer ubicación del shipping_address del primer sub-order
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.shipping_address.city')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.shipping_address.city') IS NOT NULL
      LIMIT 1
    ) as shipping_city,
    
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.shipping_address.region')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.shipping_address.region') IS NOT NULL
      LIMIT 1
    ) as shipping_region,
    
    -- Extraer notas del status_description o fulfillment
    (
      SELECT COALESCE(
        JSON_EXTRACT_SCALAR(item, '$.status_description'),
        JSON_EXTRACT_SCALAR(item, '$.fulfillment.notes')
      )
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE (JSON_EXTRACT_SCALAR(item, '$.status_description') IS NOT NULL
             OR JSON_EXTRACT_SCALAR(item, '$.fulfillment.notes') IS NOT NULL)
      LIMIT 1
    ) as status_notes,
    JSON_EXTRACT_SCALAR(shipping_info, '$.carrier') as carrier

  FROM raw_enriched_orders
  WHERE 
    marketplace = 'CENC'
    AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 minute)
    AND enrichment_status IN ('COMPLETE', 'PARTIAL')  -- CENC siempre es PARTIAL por limitaciones API
    AND items IS NOT NULL
    AND JSON_EXTRACT_ARRAY(items) IS NOT NULL
)

SELECT 
  -- Identificadores principales
  s.marketplace_order_id,
  s.company_id,
  
  -- Estado del envío (campo crudo sin normalizar)
  s.prioritized_status as event_status,
  
  -- Timestamp del evento (usamos processed_at como proxy del evento)
  s.processed_at as event_timestamp,
  
  -- Ubicación del evento (ciudad + región del shipping_address)
  CASE 
    WHEN s.shipping_city IS NOT NULL AND s.shipping_region IS NOT NULL
    THEN CONCAT(s.shipping_city, ', ', s.shipping_region)
    WHEN s.shipping_city IS NOT NULL
    THEN s.shipping_city
    ELSE NULL
  END as event_location,
  
  -- Nombre del courier (mapeo de facilityConfigId)
  carrier as courier_name,
  
  -- Tracking number (puede ser NULL - indicar en comentario)
  s.tracking_number,
  
  -- Notas adicionales (status_description o fulfillment notes)
  s.status_notes as notes,
  'cenc' as marketplace

FROM cenc_suborder_statuses s
LEFT JOIN cenc_facility_mapping m ON s.facility_config_id = m.facility_id

WHERE 
  -- Solo órdenes con status válido
  s.prioritized_status NOT IN ('', 'null', 'Desconocido')

ORDER BY s.processed_at DESC

-- =====================================================
-- NOTAS IMPORTANTES:
-- =====================================================
--
-- 1. SUB-ORDERS: CENC usa sub-orders como unidades de fulfillment, no productos individuales
-- 2. FACILITY_CONFIG_ID: Requiere mapeo específico a courier names (tabla incluida en query)
-- 3. TRACKING_NUMBER: Frecuentemente NULL - CENC no siempre proporciona tracking nativo
-- 4. ENRICHMENT_STATUS: CENC siempre es PARTIAL por limitaciones de API (falta SKU, product name)
-- 5. SHIPPING_ADDRESS: Está dentro de cada sub-order individual
-- 6. STATUS_NAME: Viene en español, contiene descripciones detalladas del estado
--
-- CASO ESPECIAL - FACILITY_CONFIG_ID MAPPING:
-- - 1: Dropshipping
-- - 3: Blue Express (Fulfillment by Blue Express)
-- - 4: Intangibles
-- - 7: Paris Fulfillment
--
-- TRANSFORMACIONES REQUERIDAS EN PYTHON:
-- - Normalización de event_status a estados PlugUp estándar
-- - Validación de que marketplace_order_id existe en Supabase orders table
-- - Manejo de tracking_number NULL (usar sub_order_number como fallback si es necesario)
-- - Actualización de facility_config_id mapping según nuevos couriers
--
-- RECOMENDACIÓN: Crear tabla separada cenc_facility_config en BigQuery para el mapeo