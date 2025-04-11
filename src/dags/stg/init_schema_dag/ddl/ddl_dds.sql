-- DROP SCHEMA dds;

CREATE SCHEMA if NOT EXISTS dds;

-- DROP SEQUENCE dds.dm_courier_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_courier_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_deliveries_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_deliveries_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_delivery_adress_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_delivery_adress_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_delivery_ts_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_delivery_ts_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_orders_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_orders_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_products_sequence;

CREATE SEQUENCE if NOT EXISTS dds.dm_products_sequence
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.dm_timestamps_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.dm_timestamps_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.fct_delivery_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.fct_delivery_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.fct_product_sales_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.fct_product_sales_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE dds.srv_wf_settings_id_seq;

CREATE SEQUENCE if NOT EXISTS dds.srv_wf_settings_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- dds.dm_courier определение

-- Drop table

-- DROP TABLE dds.dm_courier;

CREATE TABLE if NOT EXISTS dds.dm_courier (
	id serial4 NOT NULL,
	id_courier varchar NOT NULL,
	"name" text NOT NULL,
	update_ts timestamp DEFAULT now() NULL,
	CONSTRAINT dm_courier_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_unique UNIQUE (id_courier)
);


-- dds.dm_delivery_address определение

-- Drop table

-- DROP TABLE dds.dm_delivery_address;

CREATE TABLE if NOT EXISTS dds.dm_delivery_address (
	id int4 DEFAULT nextval('dds.dm_delivery_adress_id_seq'::regclass) NOT NULL,
	address varchar NOT NULL,
	CONSTRAINT dm_delivery_adress_pkey PRIMARY KEY (id),
	CONSTRAINT dm_delivery_adress_unique UNIQUE (address)
);


-- dds.dm_delivery_ts определение

-- Drop table

-- DROP TABLE dds.dm_delivery_ts;

CREATE TABLE if NOT EXISTS dds.dm_delivery_ts (
	id serial4 NOT NULL,
	delivery_ts timestamp NULL,
	CONSTRAINT dm_delivery_ts_pkey PRIMARY KEY (id),
	CONSTRAINT dm_delivery_ts_unique UNIQUE (delivery_ts)
);


-- dds.dm_restaurants определение

-- Drop table

-- DROP TABLE dds.dm_restaurants;

CREATE TABLE if NOT EXISTS dds.dm_restaurants (
	id int4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp DEFAULT now() NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT pk_dm_restaurants PRIMARY KEY (id)
);


-- dds.dm_timestamps определение

-- Drop table

-- DROP TABLE dds.dm_timestamps;

CREATE TABLE if NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);


-- dds.dm_users определение

-- Drop table

-- DROP TABLE dds.dm_users;

CREATE TABLE if NOT EXISTS dds.dm_users (
	id int4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT pk_dm_users PRIMARY KEY (id)
);


-- dds.srv_wf_settings определение

-- Drop table

-- DROP TABLE dds.srv_wf_settings;

CREATE TABLE if NOT EXISTS  dds.srv_wf_settings (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);


-- dds.dm_deliveries определение

-- Drop table

-- DROP TABLE dds.dm_deliveries;

CREATE TABLE if NOT EXISTS  dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_key varchar(50) NULL,
	delivery_ts_id int4 NULL,
	courier_id int4 NULL,
	address_id int4 NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_unique UNIQUE (delivery_key),
	CONSTRAINT dm_deliveries_dm_courier_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_courier(id),
	CONSTRAINT dm_deliveries_dm_delivery_address_fk FOREIGN KEY (address_id) REFERENCES dds.dm_delivery_address(id),
	CONSTRAINT dm_deliveries_dm_delivery_ts_fk FOREIGN KEY (delivery_ts_id) REFERENCES dds.dm_delivery_ts(id)
);


-- dds.dm_orders определение

-- Drop table

-- DROP TABLE dds.dm_orders;

CREATE TABLE if NOT EXISTS      dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	delivery_id int4 NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_unique UNIQUE (order_key),
	CONSTRAINT dm_orders_dm_deliveries_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id),
	CONSTRAINT dm_orders_dm_restaurants_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_orders_dm_timestamps_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
	CONSTRAINT dm_orders_dm_users_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);


-- dds.dm_products определение

-- Drop table

-- DROP TABLE dds.dm_products;

CREATE TABLE if NOT EXISTS          dds.dm_products (
	id int4 DEFAULT nextval('dds.dm_products_sequence'::regclass) NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) DEFAULT 0 NOT NULL,
	active_from timestamp DEFAULT now() NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_check CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT dm_products_unique UNIQUE (restaurant_id, product_id),
	CONSTRAINT pk_dm_products PRIMARY KEY (id),
	CONSTRAINT dm_products_dm_restaurants_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);


-- dds.fct_delivery определение

-- Drop table

-- DROP TABLE dds.fct_delivery;

CREATE TABLE if NOT EXISTS          dds.fct_delivery (
	id serial4 NOT NULL,
	delivery_id int4 NOT NULL,
	sum int4 NOT NULL,
	rate int4 NOT NULL,
	tip_sum int4 NOT NULL,
	CONSTRAINT fct_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT fct_delivery_unique UNIQUE (delivery_id),
	CONSTRAINT fct_delivery_dm_deliveries_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id)
);


-- dds.fct_product_sales определение

-- Drop table

-- DROP TABLE dds.fct_product_sales;

CREATE TABLE if NOT EXISTS  dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 DEFAULT 0 NOT NULL,
	price numeric(14, 2) DEFAULT 0 NOT NULL,
	total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_payment numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_grant numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
	CONSTRAINT fct_product_sales_unique UNIQUE (product_id, order_id),
	CONSTRAINT fct_product_sales_dm_orders_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);