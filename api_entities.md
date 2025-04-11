# Проект построения витрины данных для расчёта выплат курьерам

## Общее описание проекта
Проект представляет собой систему ETL-процессов для автоматизации расчёта и выплаты вознаграждений курьерам. Система собирает данные из различных источников, трансформирует их и формирует итоговые витрины для расчёта выплат.

### Архитектура проекта
```
src/
├── dags/                    # DAG-файлы для Airflow
│   ├── stg/                 # DAG'и загрузки данных
│   │   ├── api_system_delivery/      # API-источник (курьеры, доставки)
│   │   ├── order_system_restaurants_dag/  # MongoDB-источник (заказы, рестораны)
│   │   ├── bonus_system_ranks_dag/   # PostgreSQL-источник (бонусная система)
│   │   └── init_schema_dag/          # Инициализация схемы
│   ├── dds/                 # DAG'и трансформации данных
│   │   └── dds_dag/         # Загрузка в DDS-слой
│   └── cdm/                 # DAG'и формирования витрин
│       └── cdm_dag.py       # Формирование итоговых витрин
├── examples/                # Вспомогательные модули
│   ├── stg/                 # Модули для STG-слоя
│   ├── dds/                 # Модули для DDS-слоя
│   └── cdm/                 # Модули для CDM-слоя
└── lib/                     # Общие библиотеки
```

### Технологический стек
- **Оркестрация**: Apache Airflow
- **Хранилище данных**: PostgreSQL
- **Источники данных**:
  - REST API (курьеры, доставки)
  - MongoDB (заказы, рестораны)
  - PostgreSQL (бонусная система)
- **Языки программирования**:
  - Python
  - SQL
- **Инструменты**:
  - psycopg2 для работы с PostgreSQL
  - pymongo для работы с MongoDB
  - requests для работы с API

### Процесс обработки данных
1. **Загрузка данных (STG)**
   - API-источник: курьеры и доставки
   - MongoDB: заказы, рестораны, пользователи
   - PostgreSQL: события бонусной системы

2. **Трансформация (DDS)**
   - Нормализация данных
   - Создание связей между сущностями
   - Подготовка данных для витрин

3. **Формирование витрин (CDM)**
   - Расчёт метрик по ресторанам
   - Расчёт выплат курьерам
   - Формирование итоговых отчётов

### Особенности реализации
- Модульная архитектура с разделением на слои
- Независимые DAG'и для каждого источника данных
- Последовательная загрузка с учётом зависимостей
- Обработка ошибок и повторные попытки
- Логирование всех этапов
- Отслеживание прогресса загрузки

## Построение витрины данных для расчёта выплат курьерам

## Описание
Данная витрина предназначена для автоматизации процесса расчёта и выплаты вознаграждений курьерам на основе их работы за определённый период. Витрина агрегирует данные о заказах, рейтингах и чаевых для формирования итоговой суммы к выплате.

## Состав витрины (cdm.courier_payouts)

### Основные поля
| Поле | Тип | Описание |
|------|-----|----------|
| **id** | SERIAL | Уникальный идентификатор записи |
| **courier_id** | INTEGER | Идентификатор курьера (FK к dds.dm_courier) |
| **courier_name** | TEXT | Ф. И. О. курьера |
| **settlement_year** | INTEGER | Год отчёта |
| **settlement_month** | INTEGER | Месяц отчёта (1 — январь, 12 — декабрь) |

### Метрики
| Поле | Тип | Описание |
|------|-----|----------|
| **orders_count** | INTEGER | Количество выполненных заказов за период |
| **orders_total_sum** | NUMERIC(10,2) | Общая стоимость заказов |
| **rate_avg** | NUMERIC(3,2) | Средний рейтинг курьера по оценкам пользователей |
| **order_processing_fee** | NUMERIC(10,2) | Комиссия за обработку заказов (25% от orders_total_sum) |
| **courier_order_sum** | NUMERIC(10,2) | Сумма к выплате за доставленные заказы |
| **courier_tips_sum** | NUMERIC(10,2) | Сумма чаевых от пользователей |
| **courier_reward_sum** | NUMERIC(10,2) | Итоговая сумма к перечислению |

### Ограничения
- Уникальный ключ по полям: (courier_id, settlement_year, settlement_month)
- Проверка settlement_month: значение от 1 до 12

## Этапы построения витрины

### 1. Загрузка данных (STG)
#### API Data Loader DAG (api_system_dag.py)
- Загрузка данных из API с пагинацией (limit=50)
- Поддерживаемые ресурсы:
  - stg.couriers (данные о курьерах)
  - stg.deliveries (данные о доставках)
- Особенности реализации:
  - Сортировка по полю name
  - Сохранение прогресса загрузки в srv_wf_settings
  - Обработка конфликтов при повторной загрузке
  - Логирование ошибок

#### Order System DAG (order_system_restaurants_dag.py)
- Загрузка данных из MongoDB
- Поддерживаемые ресурсы:
  - stg.ordersystem_restaurants (данные о ресторанах)
  - stg.ordersystem_users (данные о пользователях)
  - stg.ordersystem_orders (данные о заказах)
- Особенности реализации:
  - Последовательная загрузка: restaurants → users → orders
  - Использование MongoReader для чтения данных
  - Сохранение через PgSaver

#### Bonus System DAG (bonus_system_ranks_dag.py)
- Загрузка данных из PostgreSQL
- Поддерживаемые ресурсы:
  - stg.bonussystem_ranks (ранги пользователей)
  - stg.bonussystem_users (пользователи бонусной системы)
  - stg.bonussystem_events (события бонусной системы)
- Особенности реализации:
  - Последовательная загрузка: ranks → users → events
  - Прямое подключение к источнику данных

#### Schema Initialization DAG (init_schema_dag.py)
- Создание и обновление схемы базы данных
- Особенности реализации:
  - Управление DDL-скриптами
  - Создание таблиц и индексов
  - Настройка ограничений

### 2. Подготовка DDS-слоя
#### DDS Loader DAG (dds_loader_dag.py)
- Задачи загрузки:
  1. **dm_users**
     - Загрузка данных о пользователях из stg.ordersystem_users
     - Маппинг полей: id, user_id, user_login, user_name
     - Обработка конфликтов по id

  2. **dm_restaurants**
     - Загрузка данных о ресторанах из stg.ordersystem_restaurants
     - Маппинг полей: id, restaurant_id, restaurant_name
     - Управление версионностью через active_from/active_to
     - Обработка конфликтов по id

  3. **dm_timestamps**
     - Загрузка временных меток из stg.ordersystem_orders
     - Разложение timestamp на компоненты: year, month, day, time, date
     - Проверки на корректность значений (месяц 1-12, день 1-31, год ≥ 2022)
     - Обработка конфликтов по id

  4. **dm_products**
     - Загрузка данных о продуктах из stg.ordersystem_restaurants
     - Обработка JSON-массива menu для извлечения продуктов
     - Маппинг полей: restaurant_id, product_id, product_name, product_price
     - Управление версионностью через active_from/active_to
     - Проверка на неотрицательную цену
     - Обработка конфликтов по (restaurant_id, product_id)

  5. **dm_courier**
     - Загрузка данных о курьерах из stg.couriers
     - Маппинг полей: id_courier, name
     - Обработка конфликтов по id_courier

  6. **dm_delivery_address**
     - Загрузка уникальных адресов доставки из stg.deliveries
     - Маппинг поля: address
     - Обработка конфликтов по address

  7. **dm_delivery_ts**
     - Загрузка уникальных временных меток доставки из stg.deliveries
     - Маппинг поля: delivery_ts
     - Сортировка по delivery_ts
     - Обработка конфликтов по delivery_ts

  8. **dm_deliveries**
     - Загрузка информации о доставках из stg.deliveries
     - Связи с таблицами:
       - dm_courier (FK: courier_id)
       - dm_delivery_address (FK: address_id)
       - dm_delivery_ts (FK: delivery_ts_id)
     - Маппинг полей: delivery_key, courier_id, address_id, delivery_ts_id
     - Обработка конфликтов по delivery_key

  9. **dm_orders**
     - Загрузка данных о заказах из stg.ordersystem_orders
     - Связи с таблицами:
       - dm_users (FK: user_id)
       - dm_restaurants (FK: restaurant_id)
       - dm_timestamps (FK: timestamp_id)
       - dm_deliveries (FK: delivery_id)
     - Обработка JSON-массива statuses для определения текущего статуса
     - Маппинг полей: user_id, restaurant_id, timestamp_id, order_key, order_status, delivery_id
     - Обработка конфликтов по order_key

  10. **fct_product_sales**
      - Загрузка фактов продаж продуктов из stg.bonussystem_events
      - Связи с таблицами:
        - dm_products (FK: product_id)
        - dm_orders (FK: order_id)
      - Обработка JSON-массива product_payments
      - Расчёт total_sum = price * quantity
      - Маппинг полей: product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
      - Проверки на неотрицательные значения
      - Обработка конфликтов по (product_id, order_id)

  11. **fct_delivery**
      - Загрузка фактов доставки из stg.deliveries
      - Связь с таблицей dm_deliveries (FK: delivery_id)
      - Маппинг полей: delivery_id, sum, rate, tip_sum
      - Обработка конфликтов по delivery_id

- Особенности реализации:
  - Последовательность загрузки с учётом зависимостей:
    ```
    [dm_courier, dm_delivery_ts, dm_delivery_address, users, restaurants, timestamps, products] 
    >> dm_deliveries 
    >> orders 
    >> [product_sales, fct_delivery]
    ```
  - Отслеживание максимальных timestamp'ов для каждой таблицы
  - Обработка конфликтов при повторной загрузке
  - Логирование процесса загрузки
  - Параметры DAG:
    - schedule_interval: None (ручной запуск)
    - start_date: 2022-05-05
    - catchup: False
    - is_paused_upon_creation: True

### 3. Формирование витрины (CDM)
#### CDM Loader DAG (cdm_dag.py)
- Задачи формирования:
  1. **dm_settlement_report**
     - Агрегация данных по ресторанам
     - Расчёт метрик:
       - orders_count: количество заказов
       - orders_total_sum: общая сумма
       - orders_bonus_payment_sum: сумма бонусных платежей
       - orders_bonus_granted_sum: сумма начисленных бонусов
       - order_processing_fee: комиссия (25% от orders_total_sum)
       - restaurant_reward_sum: итоговая сумма для ресторана

  2. **courier_payouts**
     - Расчёт выплат курьерам
     - Формулы расчёта:
       - order_processing_fee = orders_total_sum * 0.25
       - courier_order_sum: процент от суммы заказа в зависимости от рейтинга:
         - rate < 4: 5% (min 100)
         - 4 ≤ rate < 4.5: 7% (min 150)
         - 4.5 ≤ rate < 4.9: 8% (min 175)
         - rate ≥ 4.9: 10% (min 200)
       - courier_reward_sum = courier_order_sum + (courier_tips_sum * 0.95)

- Особенности реализации:
  - Группировка по курьеру и периоду
  - Обработка конфликтов при обновлении

### 4. Автоматизация процесса (DAGs)
#### DAG-файлы:
1. **api_system_dag.py**
   - Загрузка данных из API
   - Валидация и сохранение в STG
   - Параметры запуска:
     - schedule_interval: None (ручной запуск)
     - start_date: 2025-04-02
     - catchup: False
     - is_paused_upon_creation: True

2. **order_system_restaurants_dag.py**
   - Загрузка данных из MongoDB
   - Поддерживаемые ресурсы:
     - stg.ordersystem_restaurants
     - stg.ordersystem_users
     - stg.ordersystem_orders
   - Параметры запуска:
     - schedule_interval: None
     - start_date: 2022-05-05
     - catchup: False
     - is_paused_upon_creation: True
   - Последовательность загрузки:
     1. restaurants
     2. users
     3. orders

3. **bonus_system_ranks_dag.py**
   - Загрузка данных из PostgreSQL
   - Поддерживаемые ресурсы:
     - stg.bonussystem_ranks
     - stg.bonussystem_users
     - stg.bonussystem_events
   - Параметры запуска:
     - schedule_interval: None
     - start_date: 2022-05-05
     - catchup: False
     - is_paused_upon_creation: True
   - Последовательность загрузки:
     1. ranks
     2. users
     3. events

4. **init_schema_dag.py**
   - Инициализация схемы базы данных
   - Создание таблиц и индексов
   - Параметры запуска:
     - schedule_interval: None
     - start_date: 2022-05-05
     - catchup: False
     - is_paused_upon_creation: True

5. **dds_loader_dag.py**
   - Трансформация данных в DDS-слой
   - Параметры запуска:
     - schedule_interval: None
     - start_date: 2022-05-05
     - catchup: False
     - is_paused_upon_creation: True
   - Последовательность загрузки:
     1. dm_courier, dm_delivery_ts, dm_delivery_address, users, restaurants, timestamps, products
     2. dm_deliveries
     3. orders
     4. product_sales, fct_delivery

6. **cdm_dag.py**
   - Формирование витрин
   - Параметры запуска:
     - schedule_interval: None
     - start_date: 2022-05-05
     - catchup: False
     - is_paused_upon_creation: True
   - Последовательность формирования:
     1. dm_settlement_report
     2. courier_payouts

7. **master_dag.py**
   - Оркестрация всех этапов обработки данных
   - Параметры запуска:
     - schedule_interval: 0 * * * * (каждый час)
     - start_date: 2025-04-02
     - catchup: False
     - is_paused_upon_creation: True
   - Последовательность запуска:
     1. Параллельный запуск источников данных:
        - api_data_loader_dag
        - stg_bonus_system_ranks_dag
        - stg_order_system
     2. dds_dag (после завершения всех источников)
     3. cdm_dag (после завершения dds_dag)
   - Особенности реализации:
     - Использование TriggerDagRunOperator для запуска DAG'ов
     - Ожидание завершения каждого DAG'а (wait_for_completion=True)
     - Проверка статуса выполнения (allowed_states=["success"])
     - Обработка ошибок (failed_states=["failed", "skipped"])
     - Периодическая проверка статуса (poke_interval=30)
     - Сброс состояния DAG'а перед запуском (reset_dag_run=True)

## Мониторинг и поддержка
- Логирование всех этапов обработки
- Отслеживание ошибок и повторных попыток
- Метрики качества данных
- Система оповещений при сбоях

## Планирование запусков
- Автоматический запуск через master_dag каждый час
- Ручной запуск отдельных DAG'ов при необходимости
- Отслеживание прогресса загрузки через srv_wf_settings
- Механизм отката изменений при ошибках


## ВАЖНО
- Пришлось менять yaml для докера, airflow не поддерживал параллельность.

