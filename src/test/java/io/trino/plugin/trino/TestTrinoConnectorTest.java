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

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * BaseJdbcConnectorTest suite for the trino2trino connector configured as READ-ONLY.
 * <p>
 * All write/DDL behaviors are declared false so the framework auto-skips write tests.
 * Only architecturally-justified overrides remain (federation SQL differences, JDBC
 * driver bugs, single-node runner constraints).
 */
class TestTrinoConnectorTest
        extends BaseJdbcConnectorTest
{
    private DistributedQueryRunner remoteRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remoteRunner = TrinoQueryRunner.createRemoteQueryRunner();

        // Pre-populate remote memory catalog with tpch tables as read-only test fixtures.
        // BaseConnectorTest expects nation/region/orders/customer/lineitem/part/partsupp/supplier
        // to exist and be queryable through the connector's default catalog+schema.
        Session remoteSession = remoteMemorySession();
        remoteRunner.execute(remoteSession, "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        remoteRunner.execute(remoteSession, "CREATE TABLE nation_lowercase AS SELECT nationkey, lower(name) AS name, regionkey FROM tpch.tiny.nation");
        remoteRunner.execute(remoteSession, "CREATE TABLE region AS SELECT * FROM tpch.tiny.region");
        remoteRunner.execute(remoteSession, "CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders");
        remoteRunner.execute(remoteSession, "CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer");
        remoteRunner.execute(remoteSession, "CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
        remoteRunner.execute(remoteSession, "CREATE TABLE part AS SELECT * FROM tpch.tiny.part");
        remoteRunner.execute(remoteSession, "CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp");
        remoteRunner.execute(remoteSession, "CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier");
        remoteRunner.execute(remoteSession,
                "CREATE TABLE simple_table AS SELECT * FROM (VALUES BIGINT '1', BIGINT '2') AS t(col)");
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_cs_agg_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('A', CAST('A' AS CHAR(1)), BIGINT '1'),
                        ('B', CAST('B' AS CHAR(1)), BIGINT '1'),
                        ('a', CAST('a' AS CHAR(1)), BIGINT '3'),
                        ('b', CAST('b' AS CHAR(1)), BIGINT '4')
                ) AS t(a_string, a_char, a_bigint)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_case_sensitive_topn_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('A', CAST('A' AS CHAR(10)), BIGINT '1'),
                        ('B', CAST('B' AS CHAR(10)), BIGINT '2'),
                        ('a', CAST('a' AS CHAR(10)), BIGINT '3'),
                        ('b', CAST('b' AS CHAR(10)), BIGINT '4')
                ) AS t(a_string, a_char, a_bigint)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_null_sensitive_topn_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('small', BIGINT '42'),
                        ('big', BIGINT '134134'),
                        ('negative', BIGINT '-15'),
                        ('null', CAST(NULL AS BIGINT))
                ) AS t(name, a)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE native_query_unsupported AS
                SELECT
                    CAST(1 AS BIGINT) AS one,
                    CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)) AS two,
                    CAST('ok' AS VARCHAR) AS three
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_decimal_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('low', CAST(10.125 AS DECIMAL(10, 3))),
                        ('high', CAST(123.456 AS DECIMAL(10, 3)))
                ) AS t(id, amount)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_timestamp12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011' AS TIMESTAMP(12))),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)))
                ) AS t(id, ts_col)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_timestamptz12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011 UTC' AS TIMESTAMP(12) WITH TIME ZONE)),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE))
                ) AS t(id, ts_tz_col)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_interval_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('short', INTERVAL '1' DAY),
                        ('long', INTERVAL '2' DAY)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_interval_ym_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('one_year', INTERVAL '12' MONTH),
                        ('fourteen_months', INTERVAL '14' MONTH)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession, """
                CREATE TABLE test_timetz_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('early', CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE)),
                        ('late', CAST(TIME '10:30:45.124 +09:00' AS TIME(3) WITH TIME ZONE))
                ) AS t(id, time_tz_col)
                """);

        return TrinoQueryRunner.builder(remoteRunner)
                .setRemoteCatalog("memory")
                .setDefaultSchema("default")
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        Session remoteSession = remoteMemorySession();
        return sql -> remoteRunner.execute(remoteSession, sql);
    }

    private Session remoteMemorySession()
    {
        return testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();
    }

    private Session eagerJoinPushdownSession(boolean complexJoinPushdownEnabled)
    {
        Session session = joinPushdownEnabled(getSession());
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, "join_pushdown_strategy", "EAGER")
                .setCatalogSessionProperty(catalog, "complex_join_pushdown_enabled", Boolean.toString(complexJoinPushdownEnabled))
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();
    }

    // =========================================================================
    // Connector behavior declarations -- read-only connector
    // =========================================================================

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Read support for complex types
            case SUPPORTS_ARRAY,
                 SUPPORTS_ROW_TYPE -> true;

            // Pushdown: both sides are Trino with identical SQL syntax
            case SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT -> true;
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY -> true;
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM -> false;
            case SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY -> false;
            case SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN -> true;
            // Advanced statistical aggregation functions not yet implemented
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION -> false;

            // Read-only connector: all write, DDL, and schema operations
            // are blocked in TrinoClient with NOT_SUPPORTED errors
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_INSERT,
                 SUPPORTS_MULTI_STATEMENT_WRITES -> false;
            case SUPPORTS_DELETE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_TRUNCATE -> false;
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS -> false;
            case SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_DROP_SCHEMA_CASCADE -> false;
            case SUPPORTS_NOT_NULL_CONSTRAINT -> false;
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW -> false;
            case SUPPORTS_NEGATIVE_DATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    // =========================================================================
    // Architectural overrides -- federation SQL/format differences
    // =========================================================================

    @Test
    @Override
    public void testShowCreateTable()
    {
        String showCreate = computeActual("SHOW CREATE TABLE nation").getOnlyValue().toString();
        assertThat(showCreate)
                .contains("CREATE TABLE")
                .contains("nationkey")
                .contains("name")
                .contains("regionkey");
    }

    // =========================================================================
    // Architectural overrides -- query passthrough SQL resolution
    //
    // The system.query() table function resolves SQL on the remote Trino,
    // which requires catalog-qualified table names. The framework tests use
    // unqualified names that fail through federation.
    // Passthrough is verified in TestTrinoConnectorIntegration.testQueryPassthrough.
    // =========================================================================

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        // Override: use catalog-qualified SQL for the remote Trino
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT 1 AS col'))"))
                .matches("VALUES 1");
    }

    @Test
    @Override
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();

        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'name', 'memory.default.nation WHERE nationkey = 0'", "VALUES 'ALGERIA'");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromNation()
    {
        // Override: use catalog-qualified table name on the remote
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT name FROM memory.default.nation WHERE nationkey = 0'))"))
                .matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        assertQuery(
                "SELECT * FROM TABLE(system.query(query => 'SELECT * FROM memory.default.simple_table'))",
                "VALUES 1, 2");
    }

    @Test
    @Override
    public void testNativeQueryColumnAlias()
    {
        // Override: use catalog-qualified table name on the remote
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT name AS nation_name FROM memory.default.nation WHERE nationkey = 0'))"))
                .matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }

    @Test
    @Override
    public void testNativeQueryColumnAliasNotFound()
    {
        assertQueryFails(
                "SELECT name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM memory.default.region'))",
                ".* Column 'name' cannot be resolved");
        assertQueryFails(
                "SELECT column_not_found FROM TABLE(system.query(query => 'SELECT name AS region_name FROM memory.default.region'))",
                ".* Column 'column_not_found' cannot be resolved");
    }

    @Test
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        assertQuery(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'native_query_unsupported'",
                "VALUES 'one', 'two', 'three'");

        assertQuery(
                "SELECT typeof(two) FROM TABLE(system.query(query => 'SELECT one, two, three FROM memory.default.native_query_unsupported'))",
                "VALUES 'timestamp(12)'");
        assertQuery(
                "SELECT one, CAST(two AS VARCHAR), three FROM TABLE(system.query(query => 'SELECT one, two, three FROM memory.default.native_query_unsupported'))",
                "VALUES (1, '2024-01-15 10:30:45.123456789012', 'ok')");
    }

    @Test
    void testNativeQuerySelectSketchType()
    {
        assertQuery(
                "SELECT typeof(x) FROM TABLE(system.query(query => 'SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)'))",
                "VALUES 'HyperLogLog'");
        assertQuery(
                "SELECT cardinality(CAST(CAST(x AS VARBINARY) AS HyperLogLog)) FROM TABLE(system.query(query => 'SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)'))",
                "VALUES 5");
    }

    @Test
    void testDecimalPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_decimal_filter_pushdown WHERE amount > DECIMAL '50.000'"))
                .isFullyPushedDown()
                .matches("VALUES 'high'");
    }

    @Test
    void testTimestamp12PredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timestamp12_filter_pushdown WHERE ts_col > TIMESTAMP '2024-01-15 10:30:45.123456789011'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('after' AS VARCHAR(6))");
    }

    @Test
    void testTimestampWithTimeZone12PredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timestamptz12_filter_pushdown WHERE ts_tz_col > TIMESTAMP '2024-01-15 10:30:45.123456789011 UTC'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('after' AS VARCHAR(6))");
    }

    @Test
    void testIntervalPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_interval_filter_pushdown WHERE duration > INTERVAL '1' DAY"))
                .isFullyPushedDown()
                .matches("VALUES CAST('long' AS VARCHAR(5))");
    }

    @Test
    void testIntervalYearToMonthPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_interval_ym_filter_pushdown WHERE duration > INTERVAL '12' MONTH"))
                .isFullyPushedDown()
                .matches("VALUES CAST('fourteen_months' AS VARCHAR(15))");
    }

    @Test
    void testTimeWithTimeZonePredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timetz_filter_pushdown WHERE time_tz_col > TIME '10:30:45.123 +09:00'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('late' AS VARCHAR(5))");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        abort("DDL passthrough is outside the supported row-returning read contract");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        abort("DML passthrough is outside the supported row-returning read contract");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        abort("DML passthrough is outside the supported row-returning read contract");
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // Through federation, syntax errors are caught by the remote before
        // reaching the passthrough handler, producing a TrinoException.
        assertThatThrownBy(() -> computeActual(
                "SELECT * FROM TABLE(system.query(query => 'SOME INCORRECT SYNTAX'))"))
                .hasMessageContaining("mismatched input");
    }

    // =========================================================================
    // Architectural overrides -- type compatibility through federation
    // =========================================================================

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        // Data mapping smoke test creates tables to verify type round-trips;
        // not feasible with a read-only connector.
        abort("Data mapping smoke test requires write support to create test tables");
    }

    // =========================================================================
    // Architectural overrides -- planner-level pushdown verification
    //
    // Base limit/topN pushdown tests are inherited as-is. These overrides
    // restore planner-level assertions for aggregation and validate join
    // pushdown in a read-only federation fixture setup.
    // =========================================================================

    @Test
    @Override
    public void testAggregationPushdown()
    {
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT max(regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation GROUP BY regionkey")).isFullyPushedDown();
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        assertThat(query("SELECT sum(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
    }

    @Test
    @Override
    public void testCaseSensitiveAggregationPushdown()
    {
        assertCaseSensitiveAggregationLocal(
                "SELECT max(a_string), min(a_string), max(a_char), min(a_char) FROM test_cs_agg_pushdown",
                "VALUES ('b', 'A', 'b', 'A')");
        assertCaseSensitiveAggregationPushedDown(
                "SELECT DISTINCT a_string FROM test_cs_agg_pushdown",
                "VALUES 'A', 'B', 'a', 'b'");
        assertCaseSensitiveAggregationPushedDown(
                "SELECT DISTINCT a_char FROM test_cs_agg_pushdown",
                "VALUES 'A', 'B', 'a', 'b'");

        assertThat(query("SELECT count(a_string), count(a_char) FROM test_cs_agg_pushdown"))
                .isFullyPushedDown();
        assertThat(query("SELECT count(a_string), count(a_char) FROM test_cs_agg_pushdown GROUP BY a_bigint"))
                .isFullyPushedDown();
    }

    @Test
    @Override
    public void testComplexJoinPushdown()
    {
        String query = """
                SELECT n.name, o.orderstatus
                FROM nation n
                JOIN orders o ON n.regionkey = o.orderkey
                    AND n.nationkey + o.custkey - 3 = 0
                """;

        assertThat(query(eagerJoinPushdownSession(false), query))
                .joinIsNotFullyPushedDown();

        assertThat(query(eagerJoinPushdownSession(true), query))
                .isFullyPushedDown();
    }

    @Test
    @Override
    public void testJoinPushdown()
    {
        Session session = eagerJoinPushdownSession(false);

        assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                .isFullyPushedDown();
        assertThat(query(session, "SELECT r.name, n.name FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey"))
                .isFullyPushedDown();
        assertThat(query(session, "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.regionkey = r.regionkey"))
                .isFullyPushedDown();
        assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r USING (regionkey)"))
                .isFullyPushedDown();
        assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey IS NOT DISTINCT FROM r.regionkey"))
                .joinIsNotFullyPushedDown();
        assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey <> r.regionkey"))
                .joinIsNotFullyPushedDown();
        assertThat(query(session, "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name"))
                .isFullyPushedDown();
        assertThat(query(session, "SELECT n.name, nl.regionkey FROM nation n JOIN nation_lowercase nl ON n.name > nl.name"))
                .joinIsNotFullyPushedDown();
    }

    @Test
    @Override
    public void testArithmeticPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE nationkey + 1 > 24"))
                .matches("VALUES BIGINT '24'");
    }

    @Test
    @Override
    public void testCaseSensitiveTopNPushdown()
    {
        boolean expectPushdown = hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR);

        assertCaseSensitiveTopN(
                "SELECT a_bigint FROM test_case_sensitive_topn_pushdown ORDER BY a_string ASC LIMIT 2",
                expectPushdown,
                "VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)");
        assertCaseSensitiveTopN(
                "SELECT a_bigint FROM test_case_sensitive_topn_pushdown ORDER BY a_string DESC LIMIT 2",
                expectPushdown,
                "VALUES CAST(4 AS BIGINT), CAST(3 AS BIGINT)");
        assertCaseSensitiveTopN(
                "SELECT a_bigint FROM test_case_sensitive_topn_pushdown ORDER BY a_char ASC LIMIT 2",
                expectPushdown,
                "VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)");
        assertCaseSensitiveTopN(
                "SELECT a_bigint FROM test_case_sensitive_topn_pushdown ORDER BY a_char DESC LIMIT 2",
                expectPushdown,
                "VALUES CAST(4 AS BIGINT), CAST(3 AS BIGINT)");
    }

    @Test
    @Override
    public void testNullSensitiveTopNPushdown()
    {
        assertThat(query("SELECT name FROM test_null_sensitive_topn_pushdown ORDER BY a ASC NULLS FIRST LIMIT 5"))
                .ordered()
                .isFullyPushedDown()
                .matches("VALUES 'null', 'negative', 'small', 'big'");
        assertThat(query("SELECT name FROM test_null_sensitive_topn_pushdown ORDER BY a ASC NULLS LAST LIMIT 5"))
                .ordered()
                .isFullyPushedDown()
                .matches("VALUES 'negative', 'small', 'big', 'null'");
        assertThat(query("SELECT name FROM test_null_sensitive_topn_pushdown ORDER BY a DESC NULLS FIRST LIMIT 5"))
                .ordered()
                .isFullyPushedDown()
                .matches("VALUES 'null', 'big', 'small', 'negative'");
        assertThat(query("SELECT name FROM test_null_sensitive_topn_pushdown ORDER BY a DESC NULLS LAST LIMIT 5"))
                .ordered()
                .isFullyPushedDown()
                .matches("VALUES 'big', 'small', 'negative', 'null'");
    }

    @Test
    public void testLimitPushdownWithDistinctAndJoin()
    {
        MaterializedResult result = computeActual("""
                SELECT DISTINCT n.name
                FROM nation n
                JOIN region r ON n.regionkey = r.regionkey
                LIMIT 5""");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    // =========================================================================
    // Architectural overrides -- procedure and runner constraints
    // =========================================================================

    @Test
    @Override
    public void testExecuteProcedure()
    {
        abort("No procedure support through federation");
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        abort("No procedure support through federation");
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        abort("No procedure support through federation");
    }

    @Test
    @Override
    public void ensureDistributedQueryRunner()
    {
        abort("Single-node test runner -- distributed runner check not applicable");
    }

    private void assertCaseSensitiveAggregationLocal(String sql, String expected)
    {
        var assertion = assertThat(query(sql)).skippingTypesCheck();
        assertion.isNotFullyPushedDown(AggregationNode.class);
        assertion.matches(expected);
    }

    private void assertCaseSensitiveAggregationPushedDown(String sql, String expected)
    {
        var assertion = assertThat(query(sql)).skippingTypesCheck();
        assertion.isFullyPushedDown();
        assertion.matches(expected);
    }

    private void assertCaseSensitiveTopN(String sql, boolean expectPushdown, String expected)
    {
        var assertion = assertThat(query(sql)).ordered();
        if (expectPushdown) {
            assertion.isFullyPushedDown();
        }
        else {
            assertion.isNotFullyPushedDown(TopNNode.class);
        }
        assertion.matches(expected);
    }
}
