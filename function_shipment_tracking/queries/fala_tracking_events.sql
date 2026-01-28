-- =====================================================
-- FALA (Falabella) Tracking Events Query
-- =====================================================
-- 
-- Extrae eventos de tracking de órdenes FALA de los últimos 20 minutos
-- para enviar a Supabase shipment_tracking table
--
-- Campos mapeados:
-- - marketplace_order_id: marketplace_order_id
-- - company_id: company_id (del seller)
-- - event_status: item.status (sin normalizar, extraído del array items)
-- - event_timestamp: processed_at (timestamp del evento)
-- - event_location: CONCAT(city, region) del shipping_address
-- - courier_name: JSON_EXTRACT_SCALAR(shipping_info, '$.shipping_provider_type')
-- - tracking_number: JSON_EXTRACT_SCALAR(shipping_info, '$.tracking_code')
-- - notes: Combinación de cancel_reason y reason_detail si aplica
--
-- Casos especiales:
-- - FALA tiene array de items, cada uno con su status
-- - Se usa el status más avanzado según prioridad
-- - shipping_address está en buyer_info JSON
-- - tracking_code está en shipping_info JSON
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
fala_item_statuses AS (
  SELECT 
    order_id,
    marketplace_order_id,
    company_id,
    processed_at,
    shipping_info,
    buyer_info,
    
    -- Extraer todos los statuses de items para determinar el más avanzado
    JSON_EXTRACT_ARRAY(items) as items_array,
    
    -- Determinar el status más avanzado usando lógica de prioridad
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'delivered'
      ) THEN 'delivered'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'shipped'
      ) THEN 'shipped'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'ready_to_ship'
      ) THEN 'ready_to_ship'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) IN ('canceled', 'cancelled')
      ) THEN 'canceled'
      WHEN EXISTS (
        SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
        WHERE LOWER(JSON_EXTRACT_SCALAR(item, '$.status')) = 'pending'
      ) THEN 'pending'
      ELSE 'unknown'
    END as prioritized_status,
    
    -- Extraer notas de cancelación si existen
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.cancel_reason')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.cancel_reason') IS NOT NULL
      LIMIT 1
    ) as cancel_reason,
    
    (
      SELECT JSON_EXTRACT_SCALAR(item, '$.reason_detail')
      FROM UNNEST(JSON_EXTRACT_ARRAY(items)) as item 
      WHERE JSON_EXTRACT_SCALAR(item, '$.reason_detail') IS NOT NULL
      LIMIT 1
    ) as reason_detail

  FROM raw_enriched_orders
  WHERE 
    marketplace = 'FALA'
    AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 minute)
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
  
  -- Ubicación del evento (ciudad + región del shipping_address)
  CASE 
    WHEN JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.City') IS NOT NULL 
         AND JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.State') IS NOT NULL
    THEN CONCAT(
      JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.City'), 
      ', ', 
      JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.State')
    )
    WHEN JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.City') IS NOT NULL
    THEN JSON_EXTRACT_SCALAR(buyer_info, '$.shipping_address.City')
    ELSE NULL
  END as event_location,
  
  -- Nombre del courier (ShippingProviderType o ShipmentProvider)
  COALESCE(
    JSON_EXTRACT_SCALAR(shipping_info, '$.shipping_provider_type'),
    JSON_EXTRACT_SCALAR(shipping_info, '$.shipment_provider'),
    'Falabella Logistics'
  ) as courier_name,
  
  -- Tracking number (TrackingCode)
  JSON_EXTRACT_SCALAR(shipping_info, '$.tracking_code') as tracking_number,
  
  -- Notas adicionales (combinación de cancel_reason y reason_detail)
  CASE 
    WHEN cancel_reason IS NOT NULL AND reason_detail IS NOT NULL
    THEN CONCAT(cancel_reason, ' - ', reason_detail)
    WHEN cancel_reason IS NOT NULL
    THEN cancel_reason
    WHEN reason_detail IS NOT NULL
    THEN reason_detail
    ELSE NULL
  END as notes,
  'fala' as marketplace

FROM fala_item_statuses

WHERE 
  -- Solo órdenes con información de envío o status válido
  (shipping_info IS NOT NULL OR prioritized_status != 'unknown')
  
  -- Filtrar estados vacíos o nulos
  AND prioritized_status NOT IN ('', 'null', 'unknown')

ORDER BY processed_at DESC

-- =====================================================
-- NOTAS IMPORTANTES:
-- =====================================================
--
-- 1. ARRAY DE ITEMS: FALA almacena items como JSON array, cada item tiene su status
-- 2. PRIORIDAD DE STATUS: Se usa el status más avanzado según lógica de negocio
-- 3. SHIPPING_ADDRESS: Está dentro de buyer_info.shipping_address
-- 4. TRACKING_CODE: Puede estar en shipping_info.tracking_code
-- 5. CANCEL_REASON: Se extrae del primer item que tenga información de cancelación
--
-- TRANSFORMACIONES REQUERIDAS EN PYTHON:
-- - Normalización de event_status a estados PlugUp estándar
-- - Validación de que marketplace_order_id existe en Supabase orders table
-- - Manejo de múltiples items con diferentes status (ya resuelto en query)
-- - Mapeo de ShippingProviderType a nombres de courier más descriptivos