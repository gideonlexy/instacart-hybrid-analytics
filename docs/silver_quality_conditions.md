# Silver Quality Conditions

The Silver layer converts Bronze Delta tables into typed, validated tables that are safe for Gold marts and ML feature engineering.

## Enforced Conditions

1. `orders` exposes an explicit schema contract.
2. `orders` required fields must not be null, except `days_since_prior_order`.
3. `orders.eval_set` must be one of `prior`, `train`, or `test`.
4. `orders.order_number` must be greater than or equal to 1.
5. `orders.order_dow` must be between 0 and 6.
6. `orders.order_hour_of_day` must be between 0 and 23.
7. `orders.days_since_prior_order` may be null only for first orders, otherwise it must be between 0 and 30.
8. `order_products` line items must have valid `add_to_cart_order` and `reordered` values.
9. Combined `order_products` preserves ML semantics with `source_set` values of `prior` or `train`.
10. `product_catalog` joins products, aisles, and departments with required lookup fields present.
11. Product, aisle, and department identifiers must be greater than or equal to 1.
12. `orders.order_id` must be unique.
13. `product_catalog.product_id` must be unique.
14. Combined `order_products` must be unique at `(order_id, product_id, source_set)`.
15. Every `order_products.order_id` must exist in `orders`.
16. Every `order_products.product_id` must exist in `product_catalog`.

## Design Notes

- Separate `order_products_prior` and `order_products_train` outputs are retained for ML/evaluation workflows.
- The combined `order_products` table adds `source_set` so Gold marts can use one line-item table without losing dataset meaning.
- Product lookup tables are exposed as one `product_catalog` dimension to simplify analytics joins downstream.
- Persisted Silver outputs are read back and audited before the job completes, so downstream layers only run after key and relationship checks pass.
