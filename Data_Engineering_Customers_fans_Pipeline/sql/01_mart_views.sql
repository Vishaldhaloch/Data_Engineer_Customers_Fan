CREATE OR REPLACE VIEW mart.v_customer_fan_score AS
WITH oa AS (
  SELECT
    customer_id,
    max(order_ts) FILTER (WHERE order_ts IS NOT NULL) AS last_activity,
    COUNT(*) FILTER (WHERE order_ts >= now() - interval '90 days') AS freq_90d,
    COUNT(*) FILTER (WHERE activity_type IN ('wishlist','review','like') AND order_ts >= now() - interval '90 days') AS engage_90d,
    COALESCE(SUM(
      CASE WHEN activity_type = 'purchase' AND order_ts >= now() - interval '180 days'
           THEN total_amount * COALESCE(cr.rate_to_usd, 1)
           ELSE 0 END
    ),0) AS monetary_usd_180
  FROM core.order_activity oa
  LEFT JOIN core.currency_rates cr ON oa.currency = cr.currency_code
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.country,
  COALESCE( (EXTRACT(epoch FROM (now() - oa.last_activity))/86400)::int, 9999 ) AS recency_days,
  (1 - LEAST( COALESCE((EXTRACT(epoch FROM (now() - oa.last_activity))/86400)::numeric, 9999) / 90.0, 1.0))::numeric(12,6) AS recency_score,
  oa.freq_90d,
  LEAST(oa.freq_90d::numeric / 10.0, 1.0)::numeric(12,6) AS freq_score,
  oa.monetary_usd_180,
  LEAST(oa.monetary_usd_180 / 500.0, 1.0)::numeric(12,6) AS monetary_score,
  oa.engage_90d,
  LEAST(oa.engage_90d::numeric / 10.0, 1.0)::numeric(12,6) AS engage_score,
  ROUND(
    (0.35 * LEAST(oa.freq_90d::numeric / 10.0,1.0)
     + 0.25 * LEAST(oa.monetary_usd_180 / 500.0,1.0)
     + 0.25 * LEAST(oa.engage_90d::numeric / 10.0,1.0)
     + 0.15 * (1 - LEAST( COALESCE((EXTRACT(epoch FROM (now() - oa.last_activity))/86400)::numeric,9999) / 90.0, 1.0 ))
    ) * 100
  , 2) AS fan_score
FROM core.customer c
LEFT JOIN oa ON oa.customer_id = c.customer_id;

CREATE OR REPLACE VIEW mart.v_fanbase_by_country AS
SELECT
  country,
  COUNT(*) FILTER (WHERE fan_score >= 60) AS fans_count,
  ROUND(AVG(fan_score) FILTER (WHERE fan_score >= 60), 2) AS avg_fan_score,
  ROUND(
    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fan_score) FILTER (WHERE fan_score >= 60))::numeric,
    2
  ) AS median_fan_score
FROM mart.v_customer_fan_score
WHERE country IS NOT NULL
GROUP BY country
ORDER BY avg_fan_score DESC, fans_count DESC;
