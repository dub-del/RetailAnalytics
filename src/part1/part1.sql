-- Database: RetailAnalitycs v1.0
-- DROP DATABASE IF EXISTS "RetailAnalitycs v1.0";
/*CREATE DATABASE "RetailAnalitycs v1.0" WITH OWNER = postgres ENCODING = 'UTF8' LC_COLLATE = 'ru_RU.UTF-8' LC_CTYPE = 'ru_RU.UTF-8' TABLESPACE = pg_default CONNECTION
 LIMIT = -1 IS_TEMPLATE = False;*/
SET datestyle = dmy;
--DROP TABLE IF EXISTS personal_data CASCADE;
CREATE TABLE IF NOT EXISTS personal_data (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(20) NOT NULL CHECK (
        customer_name ~ '^([А-Я][- а-я]+)|([A-Z][- a-z]+)$'
    ),
    customer_surname VARCHAR(30) NOT NULL CHECK (
        customer_surname ~ '^([А-Я][- а-я]+)|([A-Z][- a-z]+)$'
    ),
    customer_primary_email VARCHAR(100) NOT NULL CHECK (
        customer_primary_email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    ),
    customer_primary_phone VARCHAR(12) NOT NULL CHECK (customer_primary_phone ~ '^\+7[0-9]{10}$')
);
CREATE TABLE IF NOT EXISTS cards (
    customer_card_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES personal_data (customer_id)
);
--DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_card_id INT REFERENCES cards NOT NULL,
    transaction_summ NUMERIC,
    transaction_datetime TIMESTAMP WITHOUT TIME ZONE,
    transaction_store_id INT NOT NULL
);
--DROP TABLE IF EXISTS groups_sku CASCADE;
CREATE TABLE IF NOT EXISTS groups_sku (
    group_id SERIAL NOT NULL PRIMARY KEY,
    group_name VARCHAR(30) NOT NULL CHECK (
        group_name ~ '^[\wа-яА-ЯёЁ0-9\s\-.,;:()\\/<>!?@#$%^&*+=]+$'
    )
);
--DROP TABLE IF EXISTS sku CASCADE;
CREATE TABLE IF NOT EXISTS sku (
    sku_id SERIAL NOT NULL PRIMARY KEY,
    sku_name VARCHAR(50) NOT NULL CHECK (
        sku_name ~ '^[\wа-яА-ЯёЁ0-9\s\-.,;:()\\/<>!?@#$%^&*+=]+$'
    ),
    group_id INT REFERENCES groups_sku (group_id) NOT NULL
);
--DROP TABLE IF EXISTS checks CASCADE;
CREATE TABLE IF NOT EXISTS checks (
    transaction_id INT REFERENCES transactions (transaction_id) NOT NULL,
    sku_id INT REFERENCES sku (sku_id) NOT NULL,
    sku_amount NUMERIC NOT NULL,
    sku_summ NUMERIC NOT NULL,
    sku_summ_paid NUMERIC NOT NULL,
    sku_discount NUMERIC NOT NULL
);
--DROP TABLE IF EXISTS stores CASCADE;
CREATE TABLE IF NOT EXISTS stores (
    transaction_store_id SERIAL REFERENCES transactions NOT NULL,
    sku_id INT REFERENCES sku (sku_id) NOT NULL,
    sku_purchase_price NUMERIC NOT NULL,
    sku_retail_price NUMERIC NOT NULL
);
--DROP TABLE IF EXISTS  date_of_analysis_formation CASCADE;
CREATE TABLE IF NOT EXISTS date_of_analysis_formation (
    analysis_formation TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
-- TRUNCATE personal_data CASCADE;
-- TRUNCATE cards CASCADE;
-- TRUNCATE transactions CASCADE;
-- TRUNCATE checks CASCADE;
-- TRUNCATE sku CASCADE;
-- TRUNCATE stores CASCADE;
-- TRUNCATE groups_sku CASCADE;
-- TRUNCATE date_of_analysis_formation CASCADE;
SET my_file.path = '/home/morfinpo/sql/SQL3_RetailAnalitycs_v1.0-2/datasets/';
-- DROP PROCEDURE IF EXISTS import(VARCHAR, TEXT, CHAR);
CREATE OR REPLACE PROCEDURE import(
        IN table_name VARCHAR,
        IN filename TEXT,
        IN delim CHAR
    ) AS $$ BEGIN IF (delim = '\t') THEN EXECUTE format(
        'COPY %I FROM %L DELIMITER E''%L'';',
        table_name,
        (current_setting('my_file.path') || filename),
        delim
    );
ELSE EXECUTE format(
    'COPY %I FROM %L DELIMITER %L;',
    table_name,
    (current_setting('my_file.path') || filename),
    delim
);
END IF;
END;
$$ LANGUAGE plpgsql;
-- CALL import('personal_data', 'Personal_Data.tsv', E'\t');
-- CALL import('cards', 'Cards.tsv', E'\t');
-- CALL import('transactions', 'Transactions.tsv', E'\t');
-- CALL import('groups_sku', 'Groups_SKU.tsv', E'\t');
-- CALL import('sku', 'SKU.tsv', E'\t');
-- CALL import('checks', 'Checks.tsv', E'\t');
-- CALL import('stores', 'Stores.tsv', E'\t');
-- CALL import(
--         'date_of_analysis_formation',
--         'Date_Of_Analysis_Formation.tsv',
--         E'\t'
--     );
CALL import('personal_data', 'Personal_Data_Mini.tsv', E'\t');
CALL import('cards', 'Cards_Mini.tsv', E'\t');
CALL import('transactions', 'Transactions_Mini.tsv', E'\t');
CALL import('groups_sku', 'Groups_SKU_Mini.tsv', E'\t');
CALL import('sku', 'SKU_Mini.tsv', E'\t');
CALL import('checks', 'Checks_Mini.tsv', E'\t');
CALL import('stores', 'Stores_Mini.tsv', E'\t');
CALL import(
    'date_of_analysis_formation',
    'Date_Of_Analysis_Formation.tsv',
    E'\t'
);
--SELECT * FROM personal_data;
--SELECT * FROM cards;
--SELECT * FROM transactions;
--SELECT * FROM checks;
--SELECT * FROM sku;
--SELECT * FROM stores;
--SELECT * FROM groups_sku;
--SELECT * FROM date_of_analysis_formation;
SET file.path = '/home/morfinpo/sql/SQL3_RetailAnalitycs_v1.0-2/datasets/';
-- DROP PROCEDURE IF EXISTS export(VARCHAR, TEXT, CHAR);
CREATE OR REPLACE PROCEDURE export(
        IN table_name VARCHAR,
        IN filename TEXT,
        IN delim CHAR
    ) AS $$ BEGIN IF (delim = '\t') THEN EXECUTE format(
        'COPY %I TO %L DELIMITER E''%L'';',
        table_name,
        (current_setting('file.path') || filename),
        delim
    );
ELSE EXECUTE format(
    'COPY %I TO %L DELIMITER %L;',
    table_name,
    (current_setting('file.path') || filename),
    delim
);
END IF;
END;
$$ LANGUAGE plpgsql;
-- INSERT INTO personal_data(customer_id, customer_name, customer_surname, customer_primary_email, customer_primary_phone)
-- VALUES (21, 'John', 'Johnov', 'John666@gmail.com', '+76666666666');
-- CALL export('personal_data', 'Personal_Data.csv', ';');
-- CALL export('cards', 'Cards.csv', E'\t');
-- CALL export('transactions', 'Transactions.csv', ';');
-- CALL export('checks', 'Checks.csv', ';');
-- CALL export('sku', 'SKU.csv', ';');
-- CALL export('stores', 'Stores.csv', ';');
-- CALL export('groups_sku', 'Groups_SKU.csv', ';');
-- CALL export(
--         'date_of_analysis_formation',
--         'Date_Of_Analysis_Formation.csv',
--         ';'
--     );