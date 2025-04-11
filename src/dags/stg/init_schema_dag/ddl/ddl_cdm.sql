-- DROP SCHEMA cdm;

CREATE SCHEMA if NOT EXISTS cdm;

-- DROP SEQUENCE cdm.courier_payouts_id_seq;

CREATE SEQUENCE if NOT EXISTS cdm.courier_payouts_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE cdm.dm_settlement_report_id_seq;

CREATE SEQUENCE if NOT EXISTS cdm.dm_settlement_report_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- cdm.courier_payouts определение

-- Drop table

-- DROP TABLE cdm.courier_payouts;

CREATE TABLE if NOT EXISTS cdm.courier_payouts (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name text NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(10, 2) NOT NULL,
	rate_avg numeric(3, 2) NOT NULL,
	order_processing_fee numeric(10, 2) NULL,
	courier_order_sum numeric(10, 2) NULL,
	courier_tips_sum numeric(10, 2) NOT NULL,
	courier_reward_sum numeric(10, 2) NULL,
	CONSTRAINT courier_payouts_pkey PRIMARY KEY (id),
	CONSTRAINT courier_payouts_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT courier_payouts_unique UNIQUE (courier_id, settlement_year, settlement_month)
);


-- cdm.dm_settlement_report определение

-- Drop table

-- DROP TABLE cdm.dm_settlement_report;

CREATE TABLE if NOT EXISTS cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 NOT NULL,
	restaurant_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_settlement_report_check CHECK (((orders_count >= 0) AND (orders_total_sum >= (0)::numeric) AND (orders_bonus_payment_sum >= (0)::numeric) AND (orders_bonus_granted_sum >= (0)::numeric) AND (order_processing_fee >= (0)::numeric) AND (restaurant_reward_sum >= (0)::numeric))),
	CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT dm_settlement_report_unique UNIQUE (restaurant_id, settlement_date)
);