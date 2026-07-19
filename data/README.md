# Sales analytics — source data

Synthetic sales dataset simulating a mid-size retail company's daily source system exports.
Designed to feed an Azure medallion pipeline: ADF -> ADLS Gen2 (raw) -> Databricks Autoloader (bronze) -> silver -> gold (star schema) -> Synapse.

## Structure

```
data/
  dimensions/
    customers.csv                     -- 400 customers (initial snapshot)
    customers_updates_2024-01-19.csv  -- delta file: 15 customers changed (SCD Type 2 source)
    products.csv                      -- 132 products across 5 categories / 25 subcategories
    stores.csv                        -- 18 stores across 5 regions
    sales_reps.csv                    -- 40 sales reps, each tied to a store
  facts/
    sales_orders/orders_YYYY-MM-DD.csv        -- one file per day, 2024-01-15 to 2024-01-21
    order_items/order_items_YYYY-MM-DD.csv    -- line-item level, one file per day
    returns/returns_YYYY-MM-DD.csv            -- not every day (returns lag behind orders)
```

## Known data quality issues (intentional — this is what silver-layer cleaning should catch)

| Day | File | Issue |
|---|---|---|
| 2024-01-16 | orders | 3 duplicate order_id rows (simulated upstream retry) |
| 2024-01-17 | orders | ~6% of rows have a blank customer_id (unlinked walk-in sales) |
| 2024-01-18 | orders | order_date is in MM/DD/YYYY format instead of YYYY-MM-DD (source system regional glitch) |
| 2024-01-19 | customers_updates | delta file with 15 customers whose address/segment changed — SCD2 test case |
| 2024-01-20 | order_items | a few rows with negative/zero quantity; 3 orphan order_items referencing an order_id that doesn't exist in any orders file |
| 2024-01-21 | orders | order_status has inconsistent casing and stray whitespace (`"  Completed  "`, `"REFUNDED"`) |

Each fact file is meant to be dropped incrementally, one day at a time, into ADLS Gen2 `raw` to exercise Databricks Autoloader's file-notification-based incremental ingestion.
