/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.trino;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Sets up two in-process Trino instances for testing the trino2trino connector:
 * <ul>
 *     <li><b>Remote</b>: Plain Trino with tpch + memory catalogs (acts as the remote data source)</li>
 *     <li><b>Local</b>: Trino with trino2trino plugin connecting to Remote</li>
 * </ul>
 */
public final class TrinoQueryRunner
{
    private TrinoQueryRunner() {}

    public static Builder builder(DistributedQueryRunner remoteRunner)
    {
        return new Builder(remoteRunner);
    }

    /**
     * Creates a standard two-instance query runner with tpch data and complex-type test data.
     * Connects to the memory catalog on the remote and populates it with tpch data
     * plus complex type test tables. Uses explicit catalog in the JDBC URL to avoid
     * ambiguity when the remote has multiple catalogs with overlapping schema names.
     */
    public static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteRunner = createRemoteQueryRunner();
        populateTpchData(remoteRunner);
        return builder(remoteRunner)
                .setRemoteCatalog("memory")
                .setDefaultSchema("default")
                .withComplexTypeTestData()
                .build();
    }

    /**
     * Creates a query runner suitable for BaseJdbcConnectorTest.
     * Connects to the memory catalog on the remote for writable table support.
     */
    public static QueryRunner createQueryRunnerForConnectorTest()
            throws Exception
    {
        DistributedQueryRunner remoteRunner = createRemoteQueryRunner();
        return builder(remoteRunner)
                .setRemoteCatalog("memory")
                .setDefaultSchema("default")
                // Skip staging tables — remote Trino catalogs handle their own atomicity
                .addConnectorProperty("insert.non-transactional-insert.enabled", "true")
                .build();
    }

    /**
     * Creates the remote Trino instance with tpch and memory catalogs.
     */
    public static DistributedQueryRunner createRemoteQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("tpch").setSchema("tiny").build())
                .setWorkerCount(0)
                .build();
        remoteRunner.installPlugin(new TpchPlugin());
        remoteRunner.createCatalog("tpch", "tpch");
        remoteRunner.installPlugin(new MemoryPlugin());
        remoteRunner.createCatalog("memory", "memory");
        return remoteRunner;
    }

    /**
     * Returns a session targeting the 'remote' catalog (trino2trino connector).
     */
    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("remote")
                .setSchema("default")
                .build();
    }

    /**
     * Populates the remote memory catalog with tpch tiny data so that
     * integration tests can access tpch tables through a single catalog connection.
     */
    private static void populateTpchData(DistributedQueryRunner remoteRunner)
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();
        for (String table : List.of("nation", "region", "orders", "customer", "lineitem", "part", "partsupp", "supplier")) {
            remoteRunner.execute(memorySession,
                    "CREATE TABLE " + table + " AS SELECT * FROM tpch.tiny." + table);
        }
    }

    public static final class Builder
    {
        private final DistributedQueryRunner remoteRunner;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private String remoteCatalog = "";
        private String defaultSchema = "tiny";
        private boolean withComplexTypeTestData;

        private Builder(DistributedQueryRunner remoteRunner)
        {
            this.remoteRunner = remoteRunner;
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        public Builder setRemoteCatalog(String remoteCatalog)
        {
            this.remoteCatalog = remoteCatalog;
            return this;
        }

        public Builder setDefaultSchema(String defaultSchema)
        {
            this.defaultSchema = defaultSchema;
            return this;
        }

        public Builder withComplexTypeTestData()
        {
            this.withComplexTypeTestData = true;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            int remotePort = remoteRunner.getCoordinator().getBaseUrl().getPort();

            Session defaultSession = testSessionBuilder()
                    .setCatalog("remote")
                    .setSchema(defaultSchema)
                    .build();

            DistributedQueryRunner localRunner = DistributedQueryRunner.builder(defaultSession)
                    .setWorkerCount(0)
                    .addExtraProperty("retry-policy", "NONE")
                    .build();

            localRunner.installPlugin(new TpchPlugin());
            localRunner.createCatalog("tpch", "tpch");

            String connectionUrl = "jdbc:trino://localhost:" + remotePort;
            if (!remoteCatalog.isEmpty()) {
                connectionUrl += "/" + remoteCatalog;
            }

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", connectionUrl)
                    .put("connection-user", "test")
                    .putAll(connectorProperties)
                    .buildOrThrow();

            localRunner.installPlugin(new TrinoPlugin());
            localRunner.createCatalog("remote", "trino", properties);

            if (withComplexTypeTestData) {
                createTestData(remoteRunner);
            }

            // Attach remote runner as a closeable so it shuts down with local
            localRunner.registerResource(() -> {
                try {
                    remoteRunner.close();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            return localRunner;
        }
    }

    /**
     * Creates test tables with various types on the remote Trino's memory catalog.
     */
    static void createTestData(DistributedQueryRunner remoteRunner)
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();

        remoteRunner.execute(memorySession, "CREATE SCHEMA alt");
        remoteRunner.execute(memorySession,
                "CREATE TABLE alt.access_log AS SELECT * FROM (VALUES BIGINT '1', BIGINT '2') AS t(id)");

        // --- ARRAY types ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_varchar AS SELECT x FROM (VALUES ARRAY['a','b','c'], ARRAY['d'], ARRAY[]) AS t(x)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_int AS SELECT x FROM (VALUES ARRAY[1,2,3], ARRAY[4,5]) AS t(x)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_nulls AS SELECT x FROM (VALUES ARRAY['a', NULL, 'c'], CAST(NULL AS ARRAY(VARCHAR))) AS t(x)");

        // --- MAP types ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_map_vv AS SELECT x FROM (VALUES MAP(ARRAY['k1'], ARRAY['v1']), MAP(ARRAY['a','b'], ARRAY['1','2'])) AS t(x)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_map_nulls AS SELECT x FROM (VALUES MAP(ARRAY['a','b'], ARRAY['1', NULL])) AS t(x)");

        // --- ROW types ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_row AS SELECT CAST(ROW('Alice', 30) AS ROW(name VARCHAR, age INTEGER)) AS x UNION ALL SELECT CAST(ROW('Bob', 25) AS ROW(name VARCHAR, age INTEGER))");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_row_null AS SELECT CAST(ROW('Alice', NULL) AS ROW(name VARCHAR, age INTEGER)) AS x UNION ALL SELECT CAST(NULL AS ROW(name VARCHAR, age INTEGER))");

        // --- Nested: array(map(varchar, varchar)) ---
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_array_of_map AS
                SELECT ARRAY[
                    MAP(ARRAY['type','post_id','nation_key'], ARRAY['pageview','p1','2']),
                    MAP(ARRAY['type','post_id','nation_key'], ARRAY['click','p2','3'])
                ] AS evts
                """);

        // --- Deeply nested: row(svc varchar, evts array(map(varchar, varchar))) ---
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_nested_row AS
                SELECT CAST(ROW('article', ARRAY[
                    MAP(ARRAY['type'], ARRAY['pageview']),
                    MAP(ARRAY['type'], ARRAY['click'])
                ]) AS ROW(svc VARCHAR, evts ARRAY(MAP(VARCHAR, VARCHAR)))) AS d
                """);

        // --- Boolean ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_boolean AS SELECT x FROM (VALUES true, false) AS t(x)");

        // --- Decimal ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_decimal AS SELECT x FROM (VALUES DECIMAL '123.45', DECIMAL '999.99') AS t(x)");

        // --- NULL varchar ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_null_varchar AS SELECT x FROM (VALUES 'hello', CAST(NULL AS VARCHAR)) AS t(x)");

        // --- Date ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_date AS SELECT x FROM (VALUES DATE '2024-01-15', DATE '1999-12-31') AS t(x)");

        // --- Timestamp with precision ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_timestamp6 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456' AS TIMESTAMP(6)) AS x");

        // --- UUID ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_uuid AS SELECT UUID '12345678-1234-1234-1234-123456789abc' AS x");
        remoteRunner.execute(memorySession, """
                CREATE TABLE test_uuid_null AS
                SELECT * FROM (
                    VALUES
                        (1, UUID '12345678-1234-1234-1234-123456789abc'),
                        (2, CAST(NULL AS UUID))
                ) AS t(id, x)
                """);

        // --- Array of dates (complex type with temporal inner type) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_date AS SELECT ARRAY[DATE '2024-01-01', DATE '2024-06-15'] AS x");

        // --- Map with decimal values ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_map_decimal AS SELECT MAP(ARRAY['price','tax'], ARRAY[DECIMAL '99.99', DECIMAL '7.50']) AS x");

        // --- Array of timestamps (temporal inside complex) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_timestamp AS SELECT ARRAY[TIMESTAMP '2024-01-15 10:30:45.123456', TIMESTAMP '2024-06-15 23:59:59.000000'] AS x");

        // --- Array of timestamp with time zone ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_tstz AS SELECT ARRAY[TIMESTAMP '2024-01-15 10:30:45.123 UTC', TIMESTAMP '2024-06-15 12:00:00.000 +09:00'] AS x");

        // --- Map with UUID values ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_map_uuid AS SELECT MAP(ARRAY['id1','id2'], ARRAY[UUID '12345678-1234-1234-1234-123456789abc', UUID 'abcdefab-cdef-abcd-efab-cdefabcdefab']) AS x");

        // --- Nested row inside row ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_row_in_row AS SELECT CAST(ROW(ROW(1, 'hello'), 'outer') AS ROW(inner_val ROW(x INTEGER, y VARCHAR), label VARCHAR)) AS d");

        // --- Quoted row field names ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_quoted_row_field AS SELECT CAST(ROW('value', 42) AS ROW(\"my field\" VARCHAR, \"my count\" INTEGER)) AS d");

        // --- TIME with various precisions ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_time AS SELECT x FROM (VALUES TIME '10:30:45', TIME '23:59:59') AS t(x)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_time6 AS SELECT CAST(TIME '10:30:45.123456' AS TIME(6)) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_time12 AS SELECT CAST(TIME '10:30:45.123456789012' AS TIME(12)) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_timetz3 AS SELECT CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE) AS x");

        // --- CHAR type ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_char AS SELECT CAST('abc' AS CHAR(10)) AS x");

        // --- VARBINARY type ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_varbinary AS SELECT x FROM (VALUES X'48656C6C6F', X'') AS t(x)");

        // --- TIMESTAMP WITH TIME ZONE (top-level, not nested) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz AS SELECT TIMESTAMP '2024-01-15 10:30:45.123 UTC' AS x");

        // --- Long DECIMAL (precision > 18, uses Decimals.encodeScaledValue path) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_long_decimal AS SELECT CAST(12345678901234567890.12345 AS DECIMAL(38, 5)) AS x");

        // --- Long TIMESTAMP (precision > 6, uses LongTimestamp path) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_timestamp9 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456789' AS TIMESTAMP(9)) AS x");

        // --- Long TIMESTAMP WITH TIME ZONE (precision > 3) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz6 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456 UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS x");

        // --- JSON type (native type mapping) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_json AS SELECT JSON '{\"key\": \"value\", \"num\": 42}' AS x");
        remoteRunner.execute(memorySession, """
                CREATE TABLE test_json_null AS
                SELECT * FROM (
                    VALUES
                        (1, JSON '{\"key\": \"value\"}'),
                        (2, CAST(NULL AS JSON))
                ) AS t(id, x)
                """);

        // --- IPADDRESS type (native type mapping) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_ipaddress AS SELECT IPADDRESS '192.168.1.1' AS x");
        remoteRunner.execute(memorySession, """
                CREATE TABLE test_ipaddress_null AS
                SELECT * FROM (
                    VALUES
                        (1, IPADDRESS '192.168.1.1'),
                        (2, CAST(NULL AS IPADDRESS))
                ) AS t(id, x)
                """);

        // --- TSTZ with various precisions (regression: ensure all 0-9 work) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz0 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45 UTC' AS TIMESTAMP(0) WITH TIME ZONE) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz1 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.1 UTC' AS TIMESTAMP(1) WITH TIME ZONE) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz2 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.12 UTC' AS TIMESTAMP(2) WITH TIME ZONE) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz9 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456789 UTC' AS TIMESTAMP(9) WITH TIME ZONE) AS x");

        // --- Nested JSON and IPADDRESS inside complex types ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_array_json AS SELECT ARRAY[JSON '{\"a\":1}', JSON '{\"b\":2}'] AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_map_ipaddress AS SELECT MAP(ARRAY['server1','server2'], ARRAY[IPADDRESS '192.168.1.1', IPADDRESS '10.0.0.1']) AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_row_json AS SELECT CAST(ROW('test', JSON '{\"key\":\"val\"}') AS ROW(label VARCHAR, data JSON)) AS x");

        // --- TSTZ precision > 9: unsupported (Java ZonedDateTime caps at nanosecond) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tstz12 AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE) AS unsupported_col");

        // --- Plain TIMESTAMP precision > 9: unsupported (JDBC caps at nanosecond) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_ts12 AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)) AS unsupported_col");

        // --- Top-level interval transport fallback ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_interval_day_to_second AS SELECT INTERVAL '2 03:04:05.678' DAY TO SECOND AS x");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_interval_year_to_month AS SELECT INTERVAL '1-6' YEAR TO MONTH AS x");

        // --- Top-level sketch transport fallback via VARBINARY ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_hyperloglog AS SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_qdigest AS SELECT qdigest_agg(v) AS x FROM (VALUES BIGINT '1', BIGINT '2', BIGINT '3', BIGINT '4', BIGINT '5') t(v)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_tdigest AS SELECT tdigest_agg(v) AS x FROM (VALUES DOUBLE '1.0', DOUBLE '2.0', DOUBLE '3.0', DOUBLE '4.0', DOUBLE '5.0') t(v)");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_setdigest AS SELECT make_set_digest(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)");

        // --- Nested unsupported complex types ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_nested_unsupported_array AS SELECT CAST('id1' AS VARCHAR) AS id, ARRAY[CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE)] AS unsupported_col");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_nested_unsupported_map AS SELECT CAST('id1' AS VARCHAR) AS id, MAP(ARRAY['duration'], ARRAY[INTERVAL '1' DAY]) AS unsupported_col");
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_nested_unsupported_map_int_key AS SELECT CAST('id1' AS VARCHAR) AS id, MAP(ARRAY[1, 2], ARRAY[INTERVAL '1' DAY, INTERVAL '2' DAY]) AS unsupported_col");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_nested_unsupported_map_row_key AS
                SELECT
                    CAST('id1' AS VARCHAR) AS id,
                    MAP(
                        ARRAY[
                            CAST(ROW(1) AS ROW(x INTEGER)),
                            CAST(ROW(2) AS ROW(x INTEGER))
                        ],
                        ARRAY[INTERVAL '1' DAY, INTERVAL '2' DAY]
                    ) AS unsupported_col
                """);
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_nested_unsupported_row AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(ROW(INTERVAL '1' DAY) AS ROW(x INTERVAL DAY TO SECOND)) AS unsupported_col");

        // --- Opaque unsupported type without a transport rule (for unsupported-type-handling fallback) ---
        remoteRunner.execute(memorySession,
                "CREATE TABLE test_unsupported_color AS SELECT rgb(255, 0, 0) AS x");
    }
}
