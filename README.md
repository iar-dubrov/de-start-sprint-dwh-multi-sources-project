# Проектирование источника данных курьерской службы
 
## 1. Какие поля нужны в витрине `cdm.dm_courier_ledger`
 
- `courier_id`, `courier_name` — кто курьер.
- `settlement_year`, `settlement_month` — за какой месяц отчёт.
- `orders_count`, `orders_total_sum` — сколько заказов и на какую сумму.
- `rate_avg` — средний рейтинг курьера за месяц.
- `order_processing_fee` — 25% от `orders_total_sum`.
- `courier_order_sum` — выплата курьеру за заказы (по тарифу, который зависит от `rate_avg`).
- `courier_tips_sum` — чаевые за месяц.
- `courier_reward_sum` — `courier_order_sum + courier_tips_sum * 0.95`.
- 
## 2. Откуда брать эти поля в DDS
 
- `courier_id`, `courier_name` — из новой таблицы `dds.dm_couriers`.
- `settlement_year`, `settlement_month` — из `dds.dm_timestamps` (дата уже есть у заказа через `dds.dm_orders.timestamp_id`).
- связь заказа с курьером — новая колонка `courier_id` в уже существующей `dds.dm_orders`.
- `orders_count`, `orders_total_sum` — считаем из `dds.fct_product_sales` по заказам со статусом `CLOSED` (это уже есть).
- `rate_avg`, `courier_tips_sum` — из новой таблицы `dds.fct_deliveries`.
- `order_processing_fee`, `courier_order_sum`, `courier_reward_sum` — нигде не хранятся, считаются на лету в ETL.
Итого новое в DDS: таблица `dds.dm_couriers`, таблица `dds.fct_deliveries`, и колонка `courier_id` в `dds.dm_orders`.
 
## 3. Что грузить из API
 
Метод `/restaurants` не нужен — рестораны уже есть из MongoDB, а витрина курьеров по ресторанам не группируется.
 
**`/couriers`** → `stg.deliverysystem_couriers`:
- `_id` — id курьера.
- `name` — имя курьера.
**`/deliveries`** → `stg.deliverysystem_deliveries`:
- `order_id` — какой это заказ.
- `delivery_id` — id доставки (по нему не задваиваем строки при перезагрузке).
- `courier_id` — какой курьер доставил.
- `rate` — оценка доставки.
- `tip_sum` — чаевые.
Остальные поля из `/deliveries` (`order_ts`, `address`, `delivery_ts`, `sum`) не грузим: дата заказа уже есть из MongoDB, адрес не нужен для расчётов, а `sum` дублирует то, что и так считается из `dds.fct_product_sales`.
 
Отчёт строится по дате заказа, а не доставки — она уже есть в хранилище, поэтому `order_ts` из API тоже не нужен.
