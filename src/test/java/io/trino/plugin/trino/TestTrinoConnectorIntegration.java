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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive integration tests for the trino2trino connector.
 * <p>
 * Spins up two in-process Trino instances (local + remote) and verifies
 * all type mappings, complex types, cross-catalog joins, and edge cases.
 * <p>
 * "remote" catalog = trino2trino connector → remote Trino (tpch + memory)
 * "tpch" catalog = local tpch (for cross-catalog join comparison)
 */
public class TestTrinoConnectorIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createQueryRunner();
    }

    // =========================================================================
    // 1. Basic connectivity
    // =========================================================================

    @Test
    void testShowCatalogs()
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        assertThat(result.getOnlyColumnAsSet()).contains("remote", "tpch", "system");
    }

    @Test
    void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM remote");
        assertThat(result.getOnlyColumnAsSet()).contains("default", "alt", "information_schema");
    }

    @Test
    void testShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM remote.default");
        assertThat(result.getOnlyColumnAsSet()).contains("nation", "region", "orders");
    }

    @Test
    void testShowTablesInAdditionalSchema()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM remote.alt");
        assertThat(result.getOnlyColumnAsSet()).contains("access_log");
    }

    @Test
    void testConfiguredCatalogScopeDoesNotExposeOtherRemoteCatalogAsSchema()
    {
        assertThatThrownBy(() -> computeActual("SHOW TABLES FROM remote.tiny"))
                .hasMessageContaining("Schema 'tiny' does not exist");
    }

    // =========================================================================
    // 2. Scalar type read tests (via remote tpch)
    // =========================================================================

    @Test
    void testBigint()
    {
        // remote memory.default.orders.orderkey is BIGINT (copied from tpch.tiny)
        MaterializedResult result = computeActual("SELECT orderkey FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isInstanceOf(Long.class);
    }

    @Test
    void testVarchar()
    {
        MaterializedResult remote = computeActual("SELECT name FROM remote.default.nation ORDER BY name LIMIT 5");
        MaterializedResult local = computeActual("SELECT name FROM tpch.tiny.nation ORDER BY name LIMIT 5");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testDouble()
    {
        MaterializedResult result = computeActual("SELECT totalprice FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isInstanceOf(Double.class);
    }

    @Test
    void testInteger()
    {
        MaterializedResult remote = computeActual("SELECT regionkey FROM remote.default.nation ORDER BY nationkey LIMIT 5");
        MaterializedResult local = computeActual("SELECT regionkey FROM tpch.tiny.nation ORDER BY nationkey LIMIT 5");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testDate()
    {
        MaterializedResult result = computeActual("SELECT orderdate FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        // Date values should not be null
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    // =========================================================================
    // 3. Aggregation & filtering
    // =========================================================================

    @Test
    void testCount()
    {
        MaterializedResult remote = computeActual("SELECT COUNT(*) FROM remote.default.nation");
        MaterializedResult local = computeActual("SELECT COUNT(*) FROM tpch.tiny.nation");
        assertThat(remote.getOnlyValue()).isEqualTo(local.getOnlyValue());
    }

    @Test
    void testFilterPushdown()
    {
        MaterializedResult remote = computeActual("SELECT name FROM remote.default.nation WHERE regionkey = 1 ORDER BY name");
        MaterializedResult local = computeActual("SELECT name FROM tpch.tiny.nation WHERE regionkey = 1 ORDER BY name");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testGroupBy()
    {
        MaterializedResult remote = computeActual("SELECT regionkey, COUNT(*) FROM remote.default.nation GROUP BY regionkey ORDER BY regionkey");
        MaterializedResult local = computeActual("SELECT regionkey, COUNT(*) FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testOrderByLimit()
    {
        MaterializedResult remote = computeActual("SELECT nationkey, name FROM remote.default.nation ORDER BY nationkey DESC LIMIT 3");
        MaterializedResult local = computeActual("SELECT nationkey, name FROM tpch.tiny.nation ORDER BY nationkey DESC LIMIT 3");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    // =========================================================================
    // 4. Cross-catalog JOIN (the core use case)
    // =========================================================================

    @Test
    void testCrossCatalogJoin()
    {
        MaterializedResult result = computeActual("""
                SELECT r.name AS remote_name, l.name AS local_name
                FROM remote.default.region r
                JOIN tpch.tiny.region l ON r.regionkey = l.regionkey
                ORDER BY r.regionkey
                """);
        assertThat(result.getRowCount()).isEqualTo(5);
        // Names should be identical since both are tpch
        for (var row : result.getMaterializedRows()) {
            assertThat(row.getField(0)).isEqualTo(row.getField(1));
        }
    }

    @Test
    void testCrossCatalogJoinWithFilter()
    {
        MaterializedResult result = computeActual("""
                SELECT n.name
                FROM remote.default.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        assertThat(result.getRowCount()).isEqualTo(5); // 5 European nations in tpch
    }

    // =========================================================================
    // 5. Complex types: ARRAY
    // =========================================================================

    @Test
    void testArrayVarchar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_varchar");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testArrayInteger()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_int");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testArrayWithNulls()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_nulls");
        assertThat(result.getRowCount()).isEqualTo(2);
        // Second row should be NULL (entire array is null)
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testArrayUnnest()
    {
        MaterializedResult result = computeActual(
                "SELECT e FROM remote.default.test_array_varchar CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        // ['a','b','c'] + ['d'] + [] = 4 elements
        assertThat(result.getRowCount()).isEqualTo(4);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("a");
    }

    // =========================================================================
    // 6. Complex types: MAP
    // =========================================================================

    @Test
    void testMapVarcharVarchar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_map_vv");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testMapSubscript()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'k1') FROM remote.default.test_map_vv WHERE element_at(x, 'k1') IS NOT NULL");
        assertThat(result.getOnlyValue()).isEqualTo("v1");
    }

    @Test
    void testMapWithNullValues()
    {
        MaterializedResult result = computeActual("SELECT x['b'] FROM remote.default.test_map_nulls");
        assertThat(result.getOnlyValue()).isNull();
    }

    // =========================================================================
    // 7. Complex types: ROW
    // =========================================================================

    @Test
    void testRow()
    {
        MaterializedResult result = computeActual(
                "SELECT x.name, x.age FROM remote.default.test_row ORDER BY x.name");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
    }

    @Test
    void testRowWithNull()
    {
        // First row: ROW with NULL field inside, Second row: entire ROW is NULL
        MaterializedResult result = computeActual("SELECT x.name FROM remote.default.test_row_null ORDER BY x.name NULLS LAST");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        // Second: NULL row → NULL name
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    // =========================================================================
    // 8. Deeply nested complex types
    // =========================================================================

    @Test
    void testArrayOfMap()
    {
        // array(map(varchar, varchar)) — common event log pattern
        MaterializedResult result = computeActual(
                "SELECT element_at(evt, 'type') FROM remote.default.test_array_of_map CROSS JOIN UNNEST(evts) AS t(evt) ORDER BY 1");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("click");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("pageview");
    }

    @Test
    void testRowContainingArrayOfMap()
    {
        // row(svc varchar, evts array(map(varchar, varchar))) — nested complex type
        MaterializedResult result = computeActual("""
                SELECT
                    d.svc,
                    element_at(evt, 'type') AS evt_type
                FROM remote.default.test_nested_row
                CROSS JOIN UNNEST(d.evts) AS t(evt)
                ORDER BY evt_type
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("article");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("click");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("pageview");
    }

    @Test
    void testCrossJoinWithNestedUnnest()
    {
        // Full scenario: UNNEST complex type from remote + JOIN with local tpch
        MaterializedResult result = computeActual("""
                SELECT
                    element_at(evt, 'post_id') AS post_id,
                    n.name AS nation_name
                FROM remote.default.test_array_of_map
                CROSS JOIN UNNEST(evts) AS t(evt)
                JOIN tpch.tiny.nation n ON CAST(element_at(evt, 'nation_key') AS BIGINT) = n.nationkey
                ORDER BY post_id
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("p1");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("BRAZIL");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("p2");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("CANADA");
    }

    // =========================================================================
    // 9. Boolean, Decimal
    // =========================================================================

    @Test
    void testBoolean()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_boolean ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(true);
    }

    @Test
    void testDecimal()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_decimal ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    // =========================================================================
    // 10. NULL handling
    // =========================================================================

    @Test
    void testNullScalar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_null_varchar ORDER BY x NULLS LAST");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("hello");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    // =========================================================================
    // 11. Query passthrough
    // =========================================================================

    @Test
    void testQueryPassthrough()
    {
        MaterializedResult result = computeActual("""
                SELECT * FROM TABLE(remote.system.query(
                    query => 'SELECT nationkey, name FROM memory.default.nation ORDER BY nationkey LIMIT 3'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testQueryPassthroughWithAnonymousOutput()
    {
        MaterializedResult result = computeActual("""
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT count(*) FROM memory.default.nation'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo(25L);
    }

    @Test
    void testQueryPassthroughCanUseFullyQualifiedRemoteSql()
    {
        MaterializedResult result = computeActual("""
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name FROM memory.default.nation WHERE nationkey = 0'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("ALGERIA");
    }

    @Test
    void testQueryPassthroughRejectsOtherRemoteCatalog()
    {
        assertThatThrownBy(() -> computeActual("""
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name FROM tpch.tiny.nation WHERE nationkey = 0'
                ))
                """))
                .hasMessageContaining("configured remote catalog 'memory'");
    }

    @Test
    void testQueryPassthroughWithCte()
    {
        MaterializedResult result = computeActual("""
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'WITH ranked AS (
                        SELECT nationkey, name
                        FROM memory.default.nation
                        WHERE regionkey = 1
                    )
                    SELECT nationkey, name FROM ranked ORDER BY nationkey LIMIT 2'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("ARGENTINA");
    }

    @Test
    void testQueryPassthroughWithWindowFunction()
    {
        MaterializedResult result = computeActual("""
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name, row_number() OVER (ORDER BY nationkey) AS rn
                        FROM memory.default.nation
                        ORDER BY nationkey
                        LIMIT 3'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(1L);
    }

    @Test
    void testQueryPassthroughUnsupportedStructuralResult()
    {
        MaterializedResult result = computeActual("""
                SELECT CAST(element_at(m, 2) AS VARCHAR)
                FROM TABLE(remote.system.query(
                    query => 'SELECT MAP(ARRAY[1, 2], ARRAY[INTERVAL ''1'' DAY, INTERVAL ''2'' DAY]) AS m'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    // =========================================================================
    // 12. DESCRIBE
    // =========================================================================

    @Test
    void testDescribeTable()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.nation");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    void testDescribeComplexTable()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_nested_row");
        assertThat(result.getRowCount()).isGreaterThan(0);
        // Should show 'd' column with row(...) type
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("row");
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("map");
    }

    @Test
    void testShowStats()
    {
        MaterializedResult result = computeActual("SHOW STATS FOR remote.default.nation");
        assertThat(result.getRowCount()).isGreaterThan(0);
        assertThat(result.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst())
                .isPresent();
    }

    // =========================================================================
    // 13. Pushdown verification
    //
    // These are smoke tests for remote SQL generation and end-to-end correctness.
    // Planner-level pushdown assertions live in TestTrinoConnectorTest.
    // =========================================================================

    @Test
    void testLimitPushdown()
    {
        MaterializedResult result = computeActual("SELECT nationkey FROM remote.default.nation LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testTopNPushdown()
    {
        // Verify ORDER BY + LIMIT produces correct, ordered results matching local tpch
        MaterializedResult remote = computeActual(
                "SELECT nationkey FROM remote.default.nation ORDER BY nationkey LIMIT 3");
        MaterializedResult local = computeActual(
                "SELECT nationkey FROM tpch.tiny.nation ORDER BY nationkey LIMIT 3");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testAggregationPushdown()
    {
        // Verify aggregation results match local tpch exactly
        MaterializedResult remoteCount = computeActual(
                "SELECT COUNT(*) FROM remote.default.nation");
        MaterializedResult localCount = computeActual(
                "SELECT COUNT(*) FROM tpch.tiny.nation");
        assertThat(remoteCount.getOnlyValue()).isEqualTo(localCount.getOnlyValue());

        MaterializedResult remoteSum = computeActual(
                "SELECT SUM(regionkey) FROM remote.default.nation");
        MaterializedResult localSum = computeActual(
                "SELECT SUM(regionkey) FROM tpch.tiny.nation");
        assertThat(remoteSum.getOnlyValue()).isEqualTo(localSum.getOnlyValue());

        // GROUP BY + COUNT: compare full result sets
        MaterializedResult remoteGrouped = computeActual(
                "SELECT regionkey, COUNT(*) FROM remote.default.nation GROUP BY regionkey ORDER BY regionkey");
        MaterializedResult localGrouped = computeActual(
                "SELECT regionkey, COUNT(*) FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey");
        assertThat(remoteGrouped.getMaterializedRows()).isEqualTo(localGrouped.getMaterializedRows());

        // MIN/MAX
        MaterializedResult remoteMinMax = computeActual(
                "SELECT MIN(nationkey), MAX(nationkey) FROM remote.default.nation");
        MaterializedResult localMinMax = computeActual(
                "SELECT MIN(nationkey), MAX(nationkey) FROM tpch.tiny.nation");
        assertThat(remoteMinMax.getMaterializedRows()).isEqualTo(localMinMax.getMaterializedRows());

        // COUNT(DISTINCT)
        MaterializedResult remoteDistinct = computeActual(
                "SELECT COUNT(DISTINCT regionkey) FROM remote.default.nation");
        MaterializedResult localDistinct = computeActual(
                "SELECT COUNT(DISTINCT regionkey) FROM tpch.tiny.nation");
        assertThat(remoteDistinct.getOnlyValue()).isEqualTo(localDistinct.getOnlyValue());
    }

    @Test
    void testPredicatePushdown()
    {
        // Verify predicate filtering returns correct results matching local tpch
        MaterializedResult remote = computeActual(
                "SELECT name FROM remote.default.nation WHERE nationkey = 1");
        MaterializedResult local = computeActual(
                "SELECT name FROM tpch.tiny.nation WHERE nationkey = 1");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());

        // Verify multiple predicates
        MaterializedResult remoteMulti = computeActual(
                "SELECT name FROM remote.default.nation WHERE regionkey = 1 AND nationkey > 5 ORDER BY name");
        MaterializedResult localMulti = computeActual(
                "SELECT name FROM tpch.tiny.nation WHERE regionkey = 1 AND nationkey > 5 ORDER BY name");
        assertThat(remoteMulti.getMaterializedRows()).isEqualTo(localMulti.getMaterializedRows());
    }

    @Test
    void testJoinPushdown()
    {
        // Join between two tables on the same remote catalog — verify results match local
        MaterializedResult remote = computeActual("""
                SELECT n.name, r.name AS region_name
                FROM remote.default.nation n
                JOIN remote.default.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        MaterializedResult local = computeActual("""
                SELECT n.name, r.name AS region_name
                FROM tpch.tiny.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testComplexFunctionRemoteDelegation()
    {
        String sql = """
                SELECT regexp_extract(path, '/post/([0-9]+)', 1)
                FROM remote.default.test_delegation_log
                WHERE date_trunc('day', CAST(log_timestamp AS timestamp)) = TIMESTAMP '2024-01-15 00:00:00'
                    AND regexp_like(path, '^/post/')
                    AND CAST(from_iso8601_timestamp(iso_timestamp) AT TIME ZONE 'Asia/Seoul' AS DATE) = DATE '2024-01-15'
                ORDER BY 1
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getOnlyColumnAsSet()).containsExactly("100", "200");

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("RemoteTrinoQuery[catalog=memory, delegated=true]");
        assertThat(explain).doesNotContain("ScanFilterProject");
    }

    @Test
    void testRemoteSubtreeDelegatedForLocalJoin()
    {
        String sql = """
                SELECT r.name, pageviews.views
                FROM tpch.tiny.region r
                JOIN (
                    SELECT regionkey, count(*) AS views
                    FROM remote.default.test_delegation_log
                    WHERE regexp_like(path, '^/post/')
                    GROUP BY regionkey
                ) pageviews
                    ON pageviews.regionkey = r.regionkey
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getMaterializedRows()).hasSize(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("AMERICA");
        assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo(2L);

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("RemoteTrinoQuery[catalog=memory, delegated=true]");
    }

    // =========================================================================
    // Read-only enforcement — verify INSERT/DELETE/UPDATE are blocked
    // =========================================================================

    @Test
    void testInsertBlocked()
    {
        assertThatThrownBy(() -> computeActual("INSERT INTO remote.default.nation VALUES (99, 'TEST', 0, 'test')"))
                .hasMessageContaining("does not support inserts");
    }

    @Test
    void testCreateTableBlocked()
    {
        assertThatThrownBy(() -> computeActual("CREATE TABLE remote.default.should_not_exist (id INTEGER)"))
                .hasMessageContaining("does not support creating tables");
    }

    @Test
    void testDeleteBlocked()
    {
        assertThatThrownBy(() -> computeActual("DELETE FROM remote.default.nation WHERE nationkey = 0"))
                .hasMessageContaining("does not support deletes");
    }

    @Test
    void testUpdateBlocked()
    {
        assertThatThrownBy(() -> computeActual("UPDATE remote.default.nation SET name = 'X' WHERE nationkey = 0"))
                .hasMessageContaining("does not support updates");
    }

    @Test
    void testDropTableBlocked()
    {
        assertThatThrownBy(() -> computeActual("DROP TABLE remote.default.nation"))
                .hasMessageContaining("does not support dropping tables");
    }
}
