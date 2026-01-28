-- =====================================================
-- WALM (Walmart) Tracking Events Query
-- =====================================================
-- 
-- Extrae eventos de tracking de órdenes WALM de los últimos 20 minutos
-- para enviar a Supabase shipment_tracking table
--
-- Campos mapeados:
-- - marketplace_order_id: marketplace_order_id (purchaseOrderId)
-- - company_id: company_id (del seller)
-- - event_status: item.status (sin normalizar, extraído del array items)
-- - event_timestamp: processed_at (timestamp del evento)
-- - event_location: CONCAT(city, state) del postal_address en shipping_info
-- - courier_name: Extraído de shipment.carrierName o método de envío
-- - tracking_number: JSON_EXTRACT_SCALAR de trackingInfo.trackingNo
-- - notes: cancellationReason si aplica
--
-- Casos especiales:
-- - WALM tiene array de items (order lines), cada uno con orderLineStatuses
-- - Se usa el status más avanzado según prioridad
-- - shipping_info contiene postal_address con ubicación
-- - tracking info puede estar en diferentes niveles del JSON
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
walm_item_statuses AS (
  SELECT 
    order_id,
    marketplace_order_id,
    company_id,
    processed_at,
    shipping_info,
    
    -- Extraer todos los statuses de items para determinar el más avanzado
    JSON_EXTRACT_ARRAY(items) as items_array,
    
    -- Determinar el status más avanzado usando lógica de prioridad
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('delivered', 'entregado')
      ) THEN 'Delivered'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('shipped', 'enviado')
      ) THEN 'Shipped'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('acknowledged', 'confirmado')
      ) THEN 'Acknowledged'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('created', 'creado')
      ) THEN 'Created'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('cancelled', 'canceled', 'cancelado')
      ) THEN 'Cancelled'
      ELSE 'Unknown'
    END as prioritized_status,
    
    -- Extraer información de tracking del primer item que la tenga
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.tracking_number')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.tracking_number') IS NOT NULL
      LIMIT 1
    ) as item_tracking_number,
    
    -- Extraer razón de cancelación si existe
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.cancellation_reason')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.cancellation_reason') IS NOT NULL
      LIMIT 1
    ) as cancellation_reason,
    
    -- Extraer información del carrier si está disponible
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.carrier_name')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.carrier_name') IS NOT NULL
      LIMIT 1
    ) as item_carrier_name

  FROM raw_enriched_orders
  WHERE 
    marketplace = 'WALM'
    AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE)
    AND enrichment_status = 'COMPLETE'
    AND items IS NOT NULL
    AND JSON_EXTRACT_ARRAY(items) IS NOT NULL
)

SELECT 
  -- Identificadores principales
  marketplace_order_id,
  company_id,
  
  -- Estado del envío (campo crudo sin normalizar)
  prioritized_status as event_status,
  
  -- Timestamp del evento (usamos processed_at como proxy del evento)
  processed_at as event_timestamp,
  
  -- Ubicación del evento (ciudad + estado del postal_address)
  CASE 
    WHEN JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.city') IS NOT NULL 
         AND JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.state') IS NOT NULL
    THEN CONCAT(
      JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.city'), 
      ', ', 
      JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.state')
    )
    WHEN JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.city') IS NOT NULL
    THEN JSON_EXTRACT_SCALAR(shipping_info, '$.postal_address.city')
    ELSE NULL
  END as event_location,
  
  -- Nombre del courier (prioridad: item carrier > shipping method > default)
  COALESCE(
    item_carrier_name,
    JSON_EXTRACT_SCALAR(shipping_info, '$.method_code'),
    'Walmart Logistics'
  ) as courier_name,
  
  -- Tracking number (prioridad: item tracking > shipping info tracking)
  COALESCE(
    item_tracking_number,
    JSON_EXTRACT_SCALAR(shipping_info, '$.tracking_number')
  ) as tracking_number,
  
  -- Notas adicionales (razón de cancelación)
  cancellation_reason as notes,
  'walm' as marketplace

FROM walm_item_statuses

WHERE 
  -- Solo órdenes con información de envío o status válido
  (shipping_info IS NOT NULL OR prioritized_status != 'Unknown')
  
  -- Filtrar estados vacíos o nulos
  AND prioritized_status NOT IN ('', 'null', 'Unknown')

ORDER BY processed_at DESC

-- =====================================================
-- NOTAS IMPORTANTES:
-- =====================================================
--
-- 1. ORDER LINES: WALM almacena items como JSON array, cada item representa una order line
-- 2. ORDER LINE STATUSES: Cada item puede tener múltiples statuses, se toma el más avanzado
-- 3. POSTAL_ADDRESS: Ubicación está en shipping_info.postal_address
-- 4. TRACKING_NUMBER: Puede estar a nivel de item o shipping_info
-- 5. CARRIER_NAME: Puede estar en items individuales o como method_code general
--
-- TRANSFORMACIONES REQUERIDAS EN PYTHON:
-- - Normalización de event_status a estados PlugUp estándar
-- - Validación de que marketplace_order_id existe en Supabase orders table
-- - Mapeo de method_code a nombres de courier más descriptivos
-- - Manejo de purchaseOrderId vs customerOrderId (usar purchaseOrderId como principal)