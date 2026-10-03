# ДЗ 5 — Customer-level feature store на Olist (CTE, Recursive CTE, Materialized View, EXPLAIN)

Цей проєкт присвячений побудові відтворюваного SQL-pipeline для підготовки ознак клієнтів (feature engineering для ML) на датасеті **Olist Brazilian E-Commerce**. Локальна PostgreSQL розгортається в Google Colab; далі реалізовано typed-схему, CTE-chain із 7 stages, recursive CTE для referral-графа, Materialized View з `REFRESH ... CONCURRENTLY` та аналіз планів виконання через `EXPLAIN (ANALYZE, BUFFERS)`.

**Файл роботи:** `rdb_hw_05.ipynb` (виконується в Google Colab, output клітинок збережено).

---

## 📦 Джерело датасету та спосіб отримання файлів

* **Джерело даних:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) на Kaggle.
* **Спосіб отримання файлів:** CSV завантажуються програмно в Colab через бібліотеку `kagglehub`:
  ```python
  dataset_dir = kagglehub.dataset_download('olistbr/brazilian-ecommerce')
  ```
  CSV-файли в репозиторій не додаються — вони відтворювано завантажуються з Kaggle при запуску ноутбука.
* **Fallback:** якщо Kaggle недоступний, ноутбук автоматично перемикається на **stub-schema** (`USE_STUB = True`: 30 customers, 50 orders, 100 order_items). Усі наступні кроки працюють без змін у коді.
* **Розмір вибірки:** використано повний датасет (2016–2018):

| Таблиця | Рядків |
|---|---|
| `olist_customers` | 99,441 |
| `olist_orders` | 99,441 |
| `olist_order_items` | 112,650 |
| `olist_products` | 32,951 |
| `olist_sellers` | 3,095 |

---

## 🗄 Raw- та typed-шар

**Raw-шар** (`olist_*_raw`) — проміжні таблиці, які дзеркалять CSV «як є». Створюються через `pandas.to_sql` і не використовуються в аналітичних запитах.

**Typed-шар** (`olist_*`) — контрольована схема з типами, ключами та обмеженнями; саме вона використовується у всіх завданнях. Дані переносяться з raw через `INSERT ... SELECT` з явним приведенням типів (`::INTEGER`, `::TIMESTAMP`, `::NUMERIC(12,2)`, `::CHAR(2)`).

| Таблиця | Ключ | Основні обмеження |
|---|---|---|
| `olist_customers` | `customer_id` | `customer_unique_id`, `customer_state` — `NOT NULL` |
| `olist_orders` | `order_id` | FK → customers; `CHECK` на допустимі `order_status` |
| `olist_order_items` | `(order_id, order_item_id)` | FK → orders, products, sellers; `CHECK price >= 0`, `freight_value >= 0` |
| `olist_products` | `product_id` | `CHECK >= 0` для розмірів, ваги, довжин |
| `olist_sellers` | `seller_id` | `seller_state` — `NOT NULL` |

> У CSV назви колонок мають помилку (`product_name_lenght`, `product_description_lenght`). У typed-схемі вони нормалізовані до `product_name_length` / `product_description_length`; у `SELECT` з raw використано фактичні назви з CSV.

Службова таблиця ДЗ: **`hw5_referrals`** — синтетичний referral-граф (`referrer_customer_id → referred_customer_id`), оскільки в Olist referral-таблиці немає. Для службових об'єктів використовується префікс `hw5_`, для таблиць Olist — `olist_`.

Після завантаження виконується smoke-test кількості рядків: typed-таблиці містять стільки ж рядків, скільки й raw (`diff = 0`).

---

## 🧱 Опис CTE-stages (Завдання 2)

Pipeline складається з `reference_date` та 7 named stages; кожен stage має одну відповідальність і коментар `-- Stage N: ...`. Pipeline визначено один раз у ноутбуці (`FEATURE_CTE` + `FEATURE_SELECT`) і повторно використано для звичайного запиту, Materialized View та EXPLAIN — тому результати гарантовано ідентичні.

| Stage | Призначення | Результат |
|---|---|---|
| `reference_date` (0) | Стабільна дата відліку: `MAX(order_purchase_timestamp)::DATE + 1` (замість `CURRENT_DATE`, щоб результат був відтворюваним) | `as_of_date` |
| `stage1_orders_total` | Замовлення + сума позицій (`price + freight_value`) + статус | `order_total` |
| `stage2_rfm` | RFM-ознаки по клієнту | `recency_days`, `frequency`, `monetary`, `first_order_date` |
| `stage3_cohort` | Квартал першої покупки | `cohort_quarter` = `DATE_TRUNC('quarter', first_order_date)::DATE` |
| `stage4_rolling_aov` | Ковзне середнє `order_total` за 30 днів (віконна функція `RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW`) | `avg_order_value_30d` |
| `stage5_cancel_rate` | Частка скасованих замовлень | `cancellation_rate` |
| `stage6_category_revenue` | Виручка за парою (клієнт, категорія товару) | `category_revenue` |
| `stage7_preferred_category` | Top-1 категорія клієнта за виручкою (`ROW_NUMBER`) | `preferred_category` |

Фінальний `SELECT` з'єднує `olist_customers` з усіма stages через `LEFT JOIN`, тому в результаті є всі клієнти (з `COALESCE` для клієнтів без замовлень).

**Обов'язкові derived features:** RFM, `cohort_quarter`, `avg_order_value_30d`, `cancellation_rate`, `preferred_category`.

> У Olist `customer_id` унікальний для кожного замовлення, тому на рівні `customer_id` усі `frequency = 1`. Pipeline реалізовано за `customer_id`, як вимагає завдання; для реальної поведінки повторних покупців групують за `customer_unique_id`.

---

## 🔁 Recursive CTE (Завдання 3)

* **Anchor:** кореневі клієнти, яких ніхто не запрошував; anti-join через `NOT EXISTS` (NULL-safe, на відміну від `NOT IN`).
* **Recursive part:** перехід від `referrer` до `referred` з обчисленням `depth` і `path_text`.
* **Захист від циклів:** масив відвіданих `path_ids` з перевіркою `NOT next_id = ANY(path_ids)` + depth guard `depth < 50`.
* У ноутбуці додано демо циклу `A → B → A` (з path-guard, лише з depth guard, та через PostgreSQL `CYCLE` clause).

---

## 💾 Materialized View (Завдання 4)

* `mv_olist_customer_features` створено на основі **повного** CTE-pipeline.
* `UNIQUE INDEX` на `customer_id` (необхідний для `REFRESH ... CONCURRENTLY`).
* `REFRESH MATERIALIZED VIEW CONCURRENTLY` виконується без помилок; вміст MV збігається з результатом живого запиту.

---

## ⏱ EXPLAIN та Reflection (Завдання 5)

У ноутбуці збережено `EXPLAIN (ANALYZE, BUFFERS)` для: повного CTE-pipeline, RFM-частини, lookup з MV за `customer_id`, top-N читання з MV (з індексом `monetary DESC` та з виправленим `monetary DESC NULLS LAST`).

Орієнтовні результати (повний Olist):

| Сценарій | Execution Time |
|---|---|
| Повний CTE-pipeline (top-10) | ≈ 4 635 мс |
| Lookup з MV за `customer_id` (Index Scan) | ≈ 0,1 мс |
| Top-10 з MV, індекс `NULLS LAST` (Index Scan) | ≈ 0,13 мс |

Reflection наприкінці ноутбука відповідає на питання про вартість обчислень, доцільність MV та refresh-cycle, термінацію recursive CTE і trade-offs.

---

## 🚀 Інструкція із запуску

1. Відкрийте `rdb_hw_05.ipynb` у **Google Colab**.
2. (За потреби) налаштуйте доступ до Kaggle; якщо `kagglehub` недоступний, ноутбук автоматично використає stub-schema.
3. Виберіть **Runtime → Restart session and run all** (Restart & Run All).
4. Перша секція самостійно встановить PostgreSQL через `apt`, створить користувача `colab` і базу `olist`, та виведе версію PostgreSQL (`SELECT version()`).
5. Далі виконуються: завантаження даних → typed-схема → CTE-pipeline → recursive CTE → Materialized View → EXPLAIN → Reflection. Ручні правки шляхів або SQL не потрібні.

**Технології:** Python, PostgreSQL 16, SQLAlchemy, psycopg2, pandas, kagglehub.
