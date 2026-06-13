# Instacart Data Contract

This directory is the local development version of the project data lake.
Production-style cloud storage will use the same layer names in S3:

```text
raw -> bronze -> silver -> gold
```

In data warehouse terms, `raw` is the landing zone. It is similar to an
unmodeled source schema: we validate the files exist and match the source
contract, but we do not clean, join, or aggregate them here.

## Source

- Dataset: Instacart Market Basket Analysis
- Source system for this project: Kaggle
- Kaggle slug used by the implementation guide:
  `yasserh/instacart-online-grocery-basket-analysis-dataset`
- License stated in the guide: CC BY 4.0

## Raw Files

Place these files directly under `data/raw/`:

| File | Expected rows | Grain | Primary key / natural grain |
| --- | ---: | --- | --- |
| `orders.csv` | 3,421,083 | One row per order | `order_id` |
| `order_products__prior.csv` | 32,434,489 | One row per prior order-product pair | `order_id`, `product_id` |
| `order_products__train.csv` | 1,384,617 | One row per train order-product pair | `order_id`, `product_id` |
| `products.csv` | 49,688 | One row per product | `product_id` |
| `aisles.csv` | 134 | One row per aisle | `aisle_id` |
| `departments.csv` | 21 | One row per department | `department_id` |

## Expected Schemas

### `orders.csv`

| Column | Description |
| --- | --- |
| `order_id` | Unique order identifier |
| `user_id` | Customer identifier |
| `eval_set` | Dataset split: `prior`, `train`, or `test` |
| `order_number` | Customer's nth order |
| `order_dow` | Day of week encoded as integer |
| `order_hour_of_day` | Hour of day encoded as `0` through `23` |
| `days_since_prior_order` | Days since previous order; null for a user's first order |

### `order_products__prior.csv` and `order_products__train.csv`

| Column | Description |
| --- | --- |
| `order_id` | Foreign key to `orders.order_id` |
| `product_id` | Foreign key to `products.product_id` |
| `add_to_cart_order` | Position in basket |
| `reordered` | `1` if the customer previously ordered the product, else `0` |

### `products.csv`

| Column | Description |
| --- | --- |
| `product_id` | Unique product identifier |
| `product_name` | Product display name |
| `aisle_id` | Foreign key to `aisles.aisle_id` |
| `department_id` | Foreign key to `departments.department_id` |

### `aisles.csv`

| Column | Description |
| --- | --- |
| `aisle_id` | Unique aisle identifier |
| `aisle` | Aisle name |

### `departments.csv`

| Column | Description |
| --- | --- |
| `department_id` | Unique department identifier |
| `department` | Department name |

## Known Source Limitations

- The dataset has no absolute timestamp column.
- `order_dow`, `order_hour_of_day`, and `days_since_prior_order` are useful
  behavioral time signals, but they are not calendar dates.
- True date-based forecasting, holiday joins, promotion calendars, and weather
  enrichment are not possible from this raw dataset alone.
- `eval_set = 'test'` orders do not have matching order-product rows because
  the original competition held those labels back.

## Local Validation

After downloading the CSVs, run:

```bash
.venv/bin/python scripts/validate_raw_data.py
```

The validator checks file presence, header order, and row counts without
loading the 32M-row order-product file into pandas memory.
