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

class TestTrinoUnsupportedTypeHandling
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.builder(TrinoQueryRunner.createRemoteQueryRunner())
                .setRemoteCatalog("memory")
                .setDefaultSchema("default")
                .withComplexTypeTestData()
                .build();
    }

    @Test
    void testOpaqueUnsupportedTypeConvertsToVarchar()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_unsupported_color");
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("x");
            assertThat(row.getField(1)).isEqualTo("varchar");
        });
    }

    @Test
    void testTransportBackedTypesDoNotFallBackToVarchar()
    {
        assertDescribeColumnType("test_hyperloglog", "x", "HyperLogLog");
        assertDescribeColumnType("test_qdigest", "x", "qdigest(bigint)");
        assertDescribeColumnType("test_tdigest", "x", "tdigest");
        assertDescribeColumnType("test_setdigest", "x", "setdigest");
        assertDescribeColumnType("test_nested_unsupported_array", "unsupported_col", "array(time(3) with time zone)");
        assertDescribeColumnType("test_nested_unsupported_map", "unsupported_col", "map(varchar(8), interval day to second)");
        assertDescribeColumnType("test_nested_unsupported_row", "unsupported_col", "row(x interval day to second)");
    }

    private void assertDescribeColumnType(String tableName, String columnName, String expectedType)
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default." + tableName);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo(columnName);
            assertThat(row.getField(1).toString()).isEqualToIgnoringCase(expectedType);
        });
    }
}
