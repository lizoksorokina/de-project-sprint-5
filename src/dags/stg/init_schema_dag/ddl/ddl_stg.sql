-- DROP SCHEMA stg;

CREATE SCHEMA if NOT EXISTS stg ;

-- DROP SEQUENCE stg.bonussystem_users_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.bonussystem_users_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.couriers_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.couriers_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.deliveries_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.deliveries_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.ordersystem_orders_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.ordersystem_orders_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.ordersystem_restaurants_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.ordersystem_restaurants_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.ordersystem_users_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.ordersystem_users_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.srv_wf_settings_id_seq;

CREATE SEQUENCE if NOT EXISTS stg.srv_wf_settings_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- stg.bonussystem_events определение

-- Drop table

-- DROP TABLE stg.bonussystem_events;

CREATE TABLE if NOT EXISTS stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
CREATE INDEX if not exists idx_outbox__event_ts ON stg.bonussystem_events USING btree (event_ts);


-- stg.bonussystem_ranks определение

-- Drop table

-- DROP TABLE stg.bonussystem_ranks;

CREATE TABLE if NOT EXISTS stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold numeric(19, 5) DEFAULT 0 NOT NULL,
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_pkey PRIMARY KEY (id)
);


-- stg.bonussystem_users определение

-- Drop table

-- DROP TABLE stg.bonussystem_users;

CREATE TABLE if NOT EXISTS stg.bonussystem_users (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);


-- stg.couriers определение

-- Drop table

-- DROP TABLE stg.couriers;

CREATE TABLE if NOT EXISTS stg.couriers (
	id serial4 NOT NULL,
	"data" jsonb NOT NULL,
	load_ts timestamp DEFAULT now() NOT NULL,
	CONSTRAINT couriers_pkey PRIMARY KEY (id),
	CONSTRAINT couriers_unique UNIQUE (data)
);


-- stg.deliveries определение

-- Drop table

-- DROP TABLE stg.deliveries;

CREATE TABLE if NOT EXISTS stg.deliveries (
	id serial4 NOT NULL,
	"data" jsonb NOT NULL,
	load_ts timestamp DEFAULT now() NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliveries_unique UNIQUE (data)
);


-- stg.ordersystem_orders определение

-- Drop table

-- DROP TABLE stg.ordersystem_orders;

CREATE TABLE if NOT EXISTS stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);


-- stg.ordersystem_restaurants определение

-- Drop table

-- DROP TABLE stg.ordersystem_restaurants;

CREATE TABLE if NOT EXISTS stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);


-- stg.ordersystem_users определение

-- Drop table

-- DROP TABLE stg.ordersystem_users;

CREATE TABLE if NOT EXISTS stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);


-- stg.srv_wf_settings определение

-- Drop table

-- DROP TABLE stg.srv_wf_settings;

CREATE TABLE if NOT EXISTS stg.srv_wf_settings (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);