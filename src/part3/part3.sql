CREATE ROLE admin SUPERUSER CREATEDB CREATEROLE;
GRANT ALL PRIVILEGES ON DATABASE "RetailAnalitycs v1.0" TO admin;
GRANT pg_signal_backend,
    pg_execute_server_program TO admin;
/*pg_signal_backend - встроенная функция в PostgreSQL, 
 которая позволяет отправлять сигналы процессу PostgreSQL, идентифицированному по pid.
 pg_execute_server_program - встроенная функция в PostgreSQL, 
 которая позволяет выполнять внешние программы и скрипты */
--REASSIGN OWNED BY admin TO "RetailAnalitycs_v1.0";
--DROP OWNED BY admin;
--DROP ROLE admin;
CREATE ROLE comer LOGIN;
GRANT CONNECT ON DATABASE "RetailAnalitycs v1.0" TO comer;
--позволяет пользователю видеть все объекты в этой схеме, но без возможности изменения или удаления данных
GRANT SELECT ON ALL TABLES IN SCHEMA public TO comer;
--позволяет пользователю только просматривать данные в таблицах.
-- проверка
--SELECT * FROM pg_roles where left(rolname, 2) IN ('ad', 'co');