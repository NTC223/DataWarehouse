-- Index for Top Customers ranking
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_amount 
  ON dwh.fact_sales(customer_key, total_amount DESC);

-- Index for Drill-through details (time based)
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_time 
  ON dwh.fact_sales(customer_key, time_key DESC);

-- Index for customer dimension lookups
CREATE INDEX IF NOT EXISTS idx_dim_customer_location 
  ON dwh.dim_customer(location_key);
