DROP TABLE IF EXISTS delta.smoke.orders;
DROP TABLE IF EXISTS delta.smoke.customers;
DROP TABLE IF EXISTS delta.smoke.complex_types;
DROP SCHEMA IF EXISTS delta.smoke;

CREATE SCHEMA delta.smoke;

CREATE TABLE delta.smoke.customers
WITH (location = 's3://remote-delta-smoke/warehouse/smoke/customers') AS
SELECT *
FROM (
    VALUES
        (BIGINT '1', 'alice', 'AMERICA'),
        (BIGINT '2', 'bob', 'EUROPE'),
        (BIGINT '3', 'carol', 'ASIA')
) AS t(custkey, name, region);

CREATE TABLE delta.smoke.orders
WITH (
    location = 's3://remote-delta-smoke/warehouse/smoke/orders',
    partitioned_by = ARRAY['ds']
) AS
SELECT *
FROM (
    VALUES
        (BIGINT '10', BIGINT '1', CAST(DECIMAL '101.25' AS DECIMAL(18, 2)), DATE '2026-01-01', 'OPEN', '2026-01-01'),
        (BIGINT '11', BIGINT '1', CAST(DECIMAL '30.50' AS DECIMAL(18, 2)), DATE '2026-01-01', 'CLOSED', '2026-01-01'),
        (BIGINT '12', BIGINT '2', CAST(DECIMAL '200.00' AS DECIMAL(18, 2)), DATE '2026-01-02', 'OPEN', '2026-01-02'),
        (BIGINT '13', BIGINT '3', CAST(DECIMAL '5.75' AS DECIMAL(18, 2)), DATE '2026-01-03', 'OPEN', '2026-01-03')
) AS t(orderkey, custkey, totalprice, orderdate, status, ds);

CREATE TABLE delta.smoke.complex_types
WITH (location = 's3://remote-delta-smoke/warehouse/smoke/complex_types') AS
SELECT
    BIGINT '1' AS id,
    ARRAY['red', 'blue'] AS tags,
    MAP(ARRAY['tier', 'source'], ARRAY['gold', 'delta']) AS attrs,
    CAST(ROW('widget', BIGINT '7') AS ROW(name VARCHAR, qty BIGINT)) AS detail,
    TIMESTAMP '2026-01-01 10:11:12.123456' AS created_at,
    DECIMAL '12345678901234567890.1234' AS large_amount;
