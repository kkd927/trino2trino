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

/**
 * Tests type mapping reads through the trino2trino connector.
 * Verifies that pre-created remote tables with various types are
 * correctly read through the JDBC federation layer.
 */
class TestTrinoTypeMapping
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createQueryRunner();
    }

    // =========================================================================
    // Scalar types via remote memory catalog
    // =========================================================================

    @Test
    void testDateRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_date ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("1999-12-31");
        assertThat(result.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("2024-01-15");
    }

    @Test
    void testTimestamp6Precision()
    {
        // Iceberg default: TIMESTAMP(6)
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_timestamp6");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        // Verify microsecond precision is preserved (source: 2024-01-15 10:30:45.123456)
        assertThat(value).contains("2024-01-15");
        assertThat(value).contains("10:30:45");
        assertThat(value).contains(".123456");
    }

    @Test
    void testUuid()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_uuid");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString())
                .isEqualToIgnoringCase("12345678-1234-1234-1234-123456789abc");
    }

    // =========================================================================
    // Complex types with temporal/decimal inner types
    // =========================================================================

    @Test
    void testArrayOfDate()
    {
        // array(date) — temporal type inside complex type
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_date");
        assertThat(result.getRowCount()).isEqualTo(1);
        // Unnest and verify individual dates
        MaterializedResult unnested = computeActual(
                "SELECT e FROM remote.default.test_array_date CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        assertThat(unnested.getRowCount()).isEqualTo(2);
        assertThat(unnested.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("2024-01-01");
        assertThat(unnested.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("2024-06-15");
    }

    @Test
    void testMapWithDecimalValues()
    {
        // map(varchar, decimal) — decimal type inside complex type
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'price') FROM remote.default.test_map_decimal");
        assertThat(result.getRowCount()).isEqualTo(1);
        // The value should be 99.99
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("99.99");
    }

    // =========================================================================
    // Type identity verification via DESCRIBE
    // =========================================================================

    @Test
    void testDescribeScalarTypes()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_date");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("date");
    }

    @Test
    void testDescribeUuid()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_uuid");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("uuid");
    }

    @Test
    void testDescribeTimestamp6()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_timestamp6");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        // Should reflect the precision
        assertThat(typeStr).contains("timestamp");
    }

    @Test
    void testDescribeArrayOfDate()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_array_date");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("date");
    }

    // =========================================================================
    // Existing complex type round-trip verification
    // =========================================================================

    @Test
    void testArrayVarcharRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT e FROM remote.default.test_array_varchar CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        assertThat(result.getRowCount()).isEqualTo(4);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("a");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("b");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("c");
        assertThat(result.getMaterializedRows().get(3).getField(0)).isEqualTo("d");
    }

    @Test
    void testMapRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'k1') FROM remote.default.test_map_vv WHERE element_at(x, 'k1') IS NOT NULL");
        assertThat(result.getOnlyValue()).isEqualTo("v1");
    }

    @Test
    void testRowRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT x.name, x.age FROM remote.default.test_row ORDER BY x.name");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Bob");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(25);
    }

    @Test
    void testNestedComplexTypeRoundTrip()
    {
        // row(svc varchar, evts array(map(varchar, varchar)))
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
    }

    // =========================================================================
    // Nested complex types with temporal/UUID inner types
    // =========================================================================

    @Test
    void testArrayOfTimestamp()
    {
        MaterializedResult unnested = computeActual("""
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_array_timestamp
                CROSS JOIN UNNEST(x) AS t(e)
                ORDER BY 1
                """);
        assertThat(unnested.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "2024-01-15 10:30:45.123456",
                        "2024-06-15 23:59:59.000000");
    }

    @Test
    void testArrayOfTimestampWithTimeZone()
    {
        MaterializedResult unnested = computeActual("""
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_array_tstz
                CROSS JOIN UNNEST(x) AS t(e)
                ORDER BY 1
                """);
        assertThat(unnested.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "2024-01-15 10:30:45.123 UTC",
                        "2024-06-15 12:00:00.000 +09:00");
    }

    @Test
    void testMapWithUuidValues()
    {
        // map(varchar, uuid)
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'id1') FROM remote.default.test_map_uuid");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString())
                .isEqualToIgnoringCase("12345678-1234-1234-1234-123456789abc");
    }

    @Test
    void testUuidNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_uuid_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0))
                .isEqualTo("12345678-1234-1234-1234-123456789abc");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testRowInsideRow()
    {
        // row(inner_val row(x integer, y varchar), label varchar)
        MaterializedResult result = computeActual(
                "SELECT d.inner_val.x, d.inner_val.y, d.label FROM remote.default.test_row_in_row");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("hello");
        assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo("outer");
    }

    @Test
    void testQuotedRowFieldNames()
    {
        // row("my field" varchar, "my count" integer) — quoted field names
        // The JDBC driver may strip quotes from type metadata. The parser
        // uses right-to-left type suffix matching to recover field names.
        MaterializedResult result = computeActual(
                "SELECT d FROM remote.default.test_quoted_row_field");
        assertThat(result.getRowCount()).isEqualTo(1);
        String rowStr = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(rowStr).contains("value");
        assertThat(rowStr).contains("42");
    }

    // =========================================================================
    // DESCRIBE verification for new complex types
    // =========================================================================

    @Test
    void testDescribeArrayOfTimestamp()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_array_timestamp");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("timestamp");
    }

    @Test
    void testDescribeRowInRow()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_row_in_row");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("row");
    }

    @Test
    void testDecimalRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_decimal ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("123.45");
        assertThat(result.getMaterializedRows().get(1).getField(0).toString()).contains("999.99");
    }

    @Test
    void testBooleanRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_boolean ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(true);
    }

    // =========================================================================
    // Additional type coverage: TIME, CHAR, VARBINARY, Long Decimal/Timestamp
    // =========================================================================

    @Test
    void testTimeRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_time ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45");
        assertThat(result.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("23:59:59");
    }

    @Test
    void testTimePrecision6()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_time6");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45.123456");
    }

    @Test
    void testTimeWithTimeZoneTransport()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_timetz3");
        assertThat(result.getOnlyValue()).isEqualTo("10:30:45.123+09:00");
    }

    @Test
    void testCharType()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_char");
        assertThat(result.getRowCount()).isEqualTo(1);
        // CHAR is space-padded
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).startsWith("abc");
    }

    @Test
    void testVarbinaryRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_varbinary ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testTimestampWithTimeZoneTopLevel()
    {
        // Source: TIMESTAMP '2024-01-15 10:30:45.123 UTC'
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(value).contains("2024-01-15");
        assertThat(value).contains("10:30:45");
        assertThat(value).contains(".123");
    }

    @Test
    void testLongDecimal()
    {
        // DECIMAL(38, 5) — uses Decimals.encodeScaledValue (long decimal path)
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_long_decimal");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString())
                .contains("12345678901234567890.12345");
    }

    @Test
    void testLongTimestamp()
    {
        // TIMESTAMP(9) — uses LongTimestamp path (source: 2024-01-15 10:30:45.123456789)
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_timestamp9");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(value).contains("2024-01-15");
        assertThat(value).contains("10:30:45");
        assertThat(value).contains(".123456789");
    }

    @Test
    void testLongTimestampWithTimeZone()
    {
        // TIMESTAMP(6) WITH TIME ZONE — uses LongTimestampWithTimeZone path
        // Source: 2024-01-15 10:30:45.123456 UTC
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz6");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(value).contains("2024-01-15");
        assertThat(value).contains("10:30:45");
        assertThat(value).contains(".123456");
    }

    @Test
    void testTimestampWithTimeZonePrecision0()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz0");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("2024-01-15");
    }

    @Test
    void testTimestampWithTimeZonePrecision1()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("2024-01-15");
    }

    @Test
    void testTimestampWithTimeZonePrecision2()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz2");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("2024-01-15");
    }

    @Test
    void testTimestampWithTimeZonePrecision9()
    {
        // Long TSTZ path (precision > 3)
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_tstz9");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("2024-01-15");
    }

    @Test
    void testDescribeTime()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_time6");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("time");
    }

    @Test
    void testDescribeVarbinary()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_varbinary");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("varbinary");
    }

    @Test
    void testDescribeLongDecimal()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_long_decimal");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("decimal(38,5)");
    }

    // =========================================================================
    // VARCHAR transport fallback
    // =========================================================================

    @Test
    void testTimestampWithTimeZonePrecision12Transport()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_tstz12");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("timestamp(12) with time zone");
        });
        assertThat(computeActual("SELECT CAST(unsupported_col AS VARCHAR) FROM remote.default.test_tstz12").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789012 UTC");
    }

    @Test
    void testPlainTimestampPrecision12Transport()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_ts12");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("timestamp(12)");
        });
        assertThat(computeActual("SELECT CAST(unsupported_col AS VARCHAR) FROM remote.default.test_ts12").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789012");
    }

    @Test
    void testIntervalDayToSecondTransport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_interval_day_to_second").getOnlyValue())
                .isEqualTo("2 03:04:05.678");
    }

    @Test
    void testIntervalYearToMonthTransport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_interval_year_to_month").getOnlyValue())
                .isEqualTo("1-6");
    }

    @Test
    void testTimePrecision12Transport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_time12").getOnlyValue())
                .isEqualTo("10:30:45.123456789012");
    }

    @Test
    void testHyperLogLogVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_hyperloglog").getOnlyValue())
                .isEqualTo("HyperLogLog");
        assertThat(computeActual("SELECT cardinality(CAST(CAST(x AS VARBINARY) AS HyperLogLog)) FROM remote.default.test_hyperloglog").getOnlyValue())
                .isEqualTo(5L);
    }

    @Test
    void testQDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_qdigest").getOnlyValue())
                .isEqualTo("qdigest(bigint)");
        assertThat(computeActual("SELECT value_at_quantile(CAST(CAST(x AS VARBINARY) AS qdigest(bigint)), 0.5) FROM remote.default.test_qdigest").getOnlyValue())
                .isEqualTo(3L);
    }

    @Test
    void testTDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_tdigest").getOnlyValue())
                .isEqualTo("tdigest");
        assertThat(computeActual("SELECT value_at_quantile(CAST(CAST(x AS VARBINARY) AS tdigest), 0.5) FROM remote.default.test_tdigest").getOnlyValue())
                .isEqualTo(3.0);
    }

    @Test
    void testSetDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_setdigest").getOnlyValue().toString())
                .isEqualToIgnoringCase("setdigest");
        assertThat(computeActual("SELECT cardinality(CAST(CAST(x AS VARBINARY) AS setdigest)) FROM remote.default.test_setdigest").getOnlyValue())
                .isEqualTo(5L);
    }

    @Test
    void testNestedArrayWithUnsupportedElementTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_array");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("array(time(3) with time zone)");
        });
        assertThat(computeActual("""
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_nested_unsupported_array
                CROSS JOIN UNNEST(unsupported_col) AS t(e)
                """).getOnlyValue()).isEqualTo("10:30:45.123+09:00");
    }

    @Test
    void testNestedMapWithUnsupportedValueTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_map");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).contains("map");
            assertThat(row.getField(1).toString()).contains("interval day to second");
        });
        assertThat(computeActual("""
                SELECT CAST(element_at(unsupported_col, 'duration') AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map
                """).getOnlyValue()).isEqualTo("1 00:00:00.000");
    }

    @Test
    void testNestedMapWithUnsupportedIntegerKeyTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_map_int_key");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).contains("map");
            assertThat(row.getField(1).toString()).contains("integer");
            assertThat(row.getField(1).toString()).contains("interval day to second");
        });
        assertThat(computeActual("""
                SELECT CAST(element_at(unsupported_col, 2) AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map_int_key
                """).getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    @Test
    void testNestedMapWithUnsupportedRowKeyTransport()
    {
        assertThat(computeActual("""
                SELECT CAST(v AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map_row_key
                CROSS JOIN UNNEST(unsupported_col) AS t(k, v)
                WHERE k.x = 2
                """).getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    @Test
    void testNestedRowWithUnsupportedFieldTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_row");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("row(x interval day to second)");
        });
        assertThat(computeActual("""
                SELECT CAST(unsupported_col.x AS VARCHAR)
                FROM remote.default.test_nested_unsupported_row
                """).getOnlyValue()).isEqualTo("1 00:00:00.000");
    }

    // =========================================================================
    // JSON and IPADDRESS native type mapping
    // =========================================================================

    @Test
    void testJsonNativeType()
    {
        // JSON should be mapped to native json type, not varchar
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_json");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(value).contains("key");
        assertThat(value).contains("value");
    }

    @Test
    void testDescribeJson()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_json");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("json");
    }

    @Test
    void testJsonNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT json_format(x) FROM remote.default.test_json_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("\"key\"");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testIpAddressNativeType()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_ipaddress");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("192.168.1.1");
    }

    @Test
    void testDescribeIpAddress()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_ipaddress");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("ipaddress");
    }

    @Test
    void testIpAddressNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_ipaddress_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("192.168.1.1");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    // =========================================================================
    // Nested JSON and IPADDRESS inside complex types
    // =========================================================================

    @Test
    void testArrayOfJson()
    {
        // JSON is not orderable, so no ORDER BY — verify both elements are present
        MaterializedResult unnested = computeActual(
                "SELECT e FROM remote.default.test_array_json CROSS JOIN UNNEST(x) AS t(e)");
        assertThat(unnested.getRowCount()).isEqualTo(2);
        String all = unnested.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(java.util.stream.Collectors.joining(","));
        assertThat(all).contains("\"a\"");
        assertThat(all).contains("\"b\"");
    }

    @Test
    void testMapOfIpAddress()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'server1') FROM remote.default.test_map_ipaddress");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("192.168.1.1");
    }

    @Test
    void testRowWithJson()
    {
        MaterializedResult result = computeActual(
                "SELECT x.label, x.data FROM remote.default.test_row_json");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("test");
        assertThat(result.getMaterializedRows().get(0).getField(1).toString()).contains("key");
    }
}
