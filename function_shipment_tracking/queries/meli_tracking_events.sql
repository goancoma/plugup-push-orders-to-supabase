-- =====================================================
-- MELI (MercadoLibre) Tracking Events Query
-- =====================================================
-- 
-- Extrae eventos de tracking de órdenes MELI de los últimos 20 minutos
-- para enviar a Supabase shipment_tracking table
--
-- Campos mapeados:
-- - marketplace_order_id: JSON_EXTRACT_SCALAR(shipping_info, '$.shipping_id') 
-- - company_id: company_id (del seller)
-- - event_status: JSON_EXTRACT_SCALAR(shipping_info, '$.status') (sin normalizar)
-- - event_timestamp: processed_at (timestamp del evento)
-- - event_location: CONCAT(city, ', ', state) del receiver_address
-- - courier_name: logistic_type mapeado a nombre del courier
-- - tracking_number: JSON_EXTRACT_SCALAR(shipping_info, '$.tracking_number')
-- - notes: JSON_EXTRACT_SCALAR(shipping_info, '$.substatus')
--
-- Casos especiales:
-- - logistic_type se mapea a courier names específicos
-- - receiver_address se extrae del shipping_info JSON
-- - Solo órdenes con shipping_info válido
--

WITH raw_enriched_orders AS (
  SELECT
    *
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY marketplace_order_id ORDER BY processed_at DESC) AS rn
    FROM `plugup_data.orders_enriched`
  )
  WHERE rn = 1
)

SELECT 
  -- Identificadores principales
  marketplace_order_id,
  company_id,
  
  -- Estado del envío (campo crudo sin normalizar)
  JSON_EXTRACT_SCALAR(shipping_info, '$.status') as event_status,
  
  -- Timestamp del evento (usamos processed_at como proxy del evento)
  processed_at as event_timestamp,
  
  -- Ubicación del evento (ciudad + región del destinatario)
  CASE 
    WHEN JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.city') IS NOT NULL 
         AND JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.state') IS NOT NULL
    THEN CONCAT(
      JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.city'), 
      ', ', 
      JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.state')
    )
    WHEN JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.city') IS NOT NULL
    THEN JSON_EXTRACT_SCALAR(shipping_info, '$.receiver_address.city')
    ELSE NULL
  END as event_location,
  
  -- Nombre del courier (mapeo de logistic_type)
  CASE JSON_EXTRACT_SCALAR(shipping_info, '$.logistic_type')
    WHEN 'fulfillment' THEN 'Mercado Envíos Full'
    WHEN 'self_service' THEN 'Mercado Envíos Flex'
    WHEN 'cross_docking' THEN 'Mercado Envíos Colecta'
    WHEN 'xd_drop_off' THEN 'Mercado Envíos Centro'
    WHEN 'drop_off' THEN 'Mercado Envíos Drop Off'
    ELSE COALESCE(JSON_EXTRACT_SCALAR(shipping_info, '$.logistic_type'), 'Mercado Envíos')
  END as courier_name,
  
  -- Tracking number (puede ser shipping_id interno de MELI)
  JSON_EXTRACT_SCALAR(shipping_info, '$.tracking_number') as tracking_number,
  
  -- Notas adicionales (substatus)
  JSON_EXTRACT_SCALAR(shipping_info, '$.substatus') as notes,
  'meli' as marketplace

FROM raw_enriched_orders

WHERE 
  -- Solo marketplace MELI
  marketplace = 'MELI'
  
  -- Solo registros actualizados en los últimos 20 minutos
  AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 minute)
  
  -- Solo órdenes con información de envío válida
  AND shipping_info IS NOT NULL
  AND JSON_EXTRACT_SCALAR(shipping_info, '$.shipping_id') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(shipping_info, '$.status') IS NOT NULL
  
  -- Excluir registros con enrichment_status != 'COMPLETE'
  AND enrichment_status = 'COMPLETE'
  
  -- Solo órdenes que tienen estado de envío (no pending inicial)
  AND JSON_EXTRACT_SCALAR(shipping_info, '$.status') NOT IN ('', 'null')

  -- Solo órdenes con order id
  AND marketplace_order_id is not null

ORDER BY processed_at DESC

-- =====================================================
-- NOTAS IMPORTANTES:
-- =====================================================
--
-- 1. TRACKING NUMBER: MELI usa shipping_id interno, no siempre hay tracking_number externo
-- 2. EVENT_TIMESTAMP: Usamos processed_at como proxy del timestamp del evento
-- 3. LOGISTIC_TYPE: Campo agregado recientemente, mapea a diferentes tipos de envío MELI
-- 4. RECEIVER_ADDRESS: Extraído del JSON shipping_info.receiver_address
-- 5. FILTROS: Solo COMPLETE para evitar datos parciales
--
-- TRANSFORMACIONES REQUERIDAS EN PYTHON:
-- - Normalización de event_status a estados PlugUp estándar
-- - Validación de que marketplace_order_id existe en Supabase orders table
-- - Manejo de tracking_number nulo (usar shipping_id como fallback)