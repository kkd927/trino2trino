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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BaseJdbcConnectorTest suite for the trino2trino connector configured as READ-ONLY.
 * <p>
 * All write/DDL behaviors are declared false so the framework auto-skips write tests.
 * Only architecturally-justified overrides remain (federation SQL differences, JDBC
 * driver bugs, single-node runner constraints).
 */
class TestTrinoConnectorTest
        extends TestTrinoConnectorIntegration
{
    private DistributedQueryRunner remoteRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remoteRunner = TrinoQueryRunner.createRemoteQueryRunner();
        try {
            createRemoteFixtures();
            return TrinoQueryRunner.builder(remoteRunner)
                    .setRemoteCatalog("memory")
                    .setDefaultSchema("default")
                    .withComplexTypeTestData()
                    .withRemoteTpchCatalog()
                    .withStatisticsDisabledCatalog()
                    .build();
        }
        catch (Exception | Error failure) {
            TrinoQueryRunner.closeOnFailure(remoteRunner, failure);
            throw failure;
        }
    }

    private void createRemoteFixtures()
    {
        // Pre-populate remote memory catalog with tpch tables as read-only test fixtures.
        // BaseConnectorTest expects nation/region/orders/customer/lineitem/part/partsupp/supplier
        // to exist and be queryable through the connector's default catalog+schema.
        TrinoQueryRunner.populateTpchData(remoteRunner);
        Session remoteSession = remoteMemorySession();
        remoteRunner.execute(
                remoteSession,
                "CREATE TABLE nation_lowercase AS SELECT nationkey, lower(name) AS name, regionkey FROM tpch.tiny.nation");
        remoteRunner.execute(
                remoteSession,
                "CREATE TABLE simple_table AS SELECT * FROM (VALUES BIGINT '1', BIGINT '2') AS t(col)");
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE native_query_unsupported AS
                SELECT
                    CAST(1 AS BIGINT) AS one,
                    CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)) AS two,
                    CAST('ok' AS VARCHAR) AS three
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_decimal_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('low', CAST(10.125 AS DECIMAL(10, 3))),
                        ('high', CAST(123.456 AS DECIMAL(10, 3)))
                ) AS t(id, amount)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timestamp12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011' AS TIMESTAMP(12))),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)))
                ) AS t(id, ts_col)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timestamptz12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011 UTC' AS TIMESTAMP(12) WITH TIME ZONE)),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE))
                ) AS t(id, ts_tz_col)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_interval_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('short', INTERVAL '1' DAY),
                        ('long', INTERVAL '2' DAY)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_interval_ym_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('one_year', INTERVAL '12' MONTH),
                        ('fourteen_months', INTERVAL '14' MONTH)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timetz_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('early', CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE)),
                        ('late', CAST(TIME '10:30:45.124 +09:00' AS TIME(3) WITH TIME ZONE))
                ) AS t(id, time_tz_col)
                """);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        Session remoteSession = remoteMemorySession();
        return sql -> remoteRunner.execute(remoteSession, sql);
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, String tableDefinition, List<String> rowsToInsert)
    {
        // The connector is read-only: fixtures that base tests would create through
        // the connector are created directly on the remote instead. The connector's
        // default schema maps to the same remote schema, so the tables stay visible.
        return new TestTable(onRemoteDatabase(), namePrefix, tableDefinition, rowsToInsert);
    }

    private Session eagerJoinPushdownSession(boolean complexJoinPushdownEnabled)
    {
        return eagerJoinPushdownSession(getSession(), complexJoinPushdownEnabled);
    }

    private Session eagerJoinPushdownSession(Session session, boolean complexJoinPushdownEnabled)
    {
        Session joinPushdownSession = super.joinPushdownEnabled(session);
        String catalog = joinPushdownSession.getCatalog().orElseThrow();
        return Session.builder(joinPushdownSession)
                .setCatalogSessionProperty(catalog, "join_pushdown_strategy", "EAGER")
                .setCatalogSessionProperty(catalog, "complex_join_pushdown_enabled", Boolean.toString(complexJoinPushdownEnabled))
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return eagerJoinPushdownSession(session, false);
    }

    private Session remoteMemorySession()
    {
        return testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();
    }

    @Override
    protected void assertQueryFails(String sql, String expectedMessageRegex)
    {
        super.assertQueryFails(sql, readOnlyFailurePattern(expectedMessageRegex));
    }

    @Override
    protected void assertQueryFails(Session session, String sql, String expectedMessageRegex)
    {
        super.assertQueryFails(session, sql, readOnlyFailurePattern(expectedMessageRegex));
    }

    private static String readOnlyFailurePattern(String expectedMessageRegex)
    {
        if (expectedMessageRegex.startsWith("This connector does not support")) {
            return "(?s)(?:" + expectedMessageRegex + "|Access Denied:.*)";
        }
        return expectedMessageRegex;
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
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT -> true;
            // Remote TopN is still applied, but transport projection can wrap it in
            // an outer query, so local ordering verification must remain in the plan.
            case SUPPORTS_TOPN_PUSHDOWN -> false;
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY -> true;
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY -> false;
            case SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE -> true;
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
    void testNativeQueryAnonymousOutputColumn()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT count(*) FROM memory.default.nation'))"))
                .matches("VALUES BIGINT '25'");
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
        assertThat(query("SELECT id FROM test_interval_filter_pushdown WHERE duration > INTERVAL '-1' DAY"))
                .isFullyPushedDown()
                .matches("VALUES CAST('short' AS VARCHAR(5)), CAST('long' AS VARCHAR(5))");
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
        assertPassthroughStatementRejected("CREATE TABLE memory.default.native_query_create AS SELECT 1 AS value");
        assertThat(computeRemoteActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain("native_query_create");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertPassthroughStatementRejected("INSERT INTO memory.default.native_query_missing VALUES (1)");
        assertThat(computeRemoteActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain("native_query_missing");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        assertPassthroughStatementRejected("INSERT INTO memory.default.nation VALUES (99, 'TEST', 0, 'test')");
        assertThat(computeRemoteActual("SELECT count(*) FROM nation WHERE nationkey = 99").getOnlyValue())
                .isEqualTo(0L);
    }

    @Test
    void testNativeQueryDeleteStatement()
    {
        assertPassthroughStatementRejected("DELETE FROM memory.default.nation WHERE nationkey = 0");
        assertThat(computeRemoteActual("SELECT count(*) FROM nation WHERE nationkey = 0").getOnlyValue())
                .isEqualTo(1L);
    }

    @Test
    void testNativeQueryUpdateStatement()
    {
        assertPassthroughStatementRejected("UPDATE memory.default.nation SET name = 'X' WHERE nationkey = 0");
        assertThat(computeRemoteActual("SELECT name FROM nation WHERE nationkey = 0").getOnlyValue())
                .isEqualTo("ALGERIA");
    }

    @Test
    void testNativeQueryCallStatement()
    {
        // Only the local rejection is asserted: there is no observable remote
        // procedure, so a remote-state invariant would be vacuous here
        assertPassthroughStatementRejected("CALL system.runtime.kill_query('query-id', 'reason')");
    }

    private MaterializedResult computeRemoteActual(String sql)
    {
        return remoteRunner.execute(remoteMemorySession(), sql);
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // The passthrough validator parses the statement locally before any
        // remote contact, so the syntax error comes from the local parser,
        // not the remote cluster.
        assertThatThrownBy(() -> computeActual(
                "SELECT * FROM TABLE(system.query(query => 'SOME INCORRECT SYNTAX'))"))
                .hasMessageContaining("mismatched input");
    }

    // =========================================================================
    // Architectural overrides -- type compatibility through federation
    // =========================================================================

    private void assertPassthroughStatementRejected(String sql)
    {
        assertThatThrownBy(() -> computeActual("SELECT * FROM TABLE(system.query(query => '" + sql.replace("'", "''") + "'))"))
                .hasMessageContaining("system.query only supports row-returning read queries");
    }

    // =========================================================================
    // Architectural overrides -- procedure and runner constraints
    // =========================================================================

    // The procedure is inherited from base-jdbc, but this connector exposes a
    // read-only surface and denies it before the remote SQL is executed.
    @Test
    @Override
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = "memory.default." + tableName;

        assertExecuteProcedureDenied("CALL system.execute('CREATE TABLE " + schemaTableName + " (a int)')");
        assertThat(computeRemoteActual("SHOW TABLES FROM memory.default LIKE '" + tableName + "'").getRowCount()).isEqualTo(0);
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = "memory.default." + tableName;

        assertExecuteProcedureDenied("CALL system.execute(query => 'CREATE TABLE " + schemaTableName + " (a int)')");
        assertThat(computeRemoteActual("SHOW TABLES FROM memory.default LIKE '" + tableName + "'").getRowCount()).isEqualTo(0);
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertExecuteProcedureDenied("CALL system.execute('some incorrect syntax')");
    }

    @Test
    void testFlushMetadataCacheProcedure()
    {
        assertUpdate("CALL system.flush_metadata_cache()");
    }

    @Test
    void testDropNotNullConstraintDoesNotMutateRemoteTable()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_not_null", "(value bigint NOT NULL)")) {
            assertThatThrownBy(() -> computeActual("ALTER TABLE " + table.getName() + " ALTER COLUMN value DROP NOT NULL"))
                    .hasMessageContaining("Access Denied");
            assertThat(computeRemoteActual("SELECT is_nullable FROM information_schema.columns " +
                    "WHERE table_schema = 'default' AND table_name = '" + table.getName() + "' AND column_name = 'value'").getOnlyValue())
                    .isEqualTo("NO");
        }
    }

    private void assertExecuteProcedureDenied(String sql)
    {
        assertThatThrownBy(() -> computeActual(sql))
                .hasMessageContaining("Access Denied")
                .hasMessageContaining("Cannot execute procedure system.execute");
    }
}
