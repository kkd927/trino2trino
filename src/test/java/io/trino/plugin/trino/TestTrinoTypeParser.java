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

import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for TrinoClient's type name parser.
 * Verifies that Trino type strings from JDBC metadata are correctly parsed
 * into Trino Type objects, including recursive complex types.
 */
class TestTrinoTypeParser
{
    private static final TypeManager TYPE_MANAGER = new TestingFunctionResolution().getPlannerContext().getTypeManager();

    private Type parse(String name)
    {
        return TrinoTypeNameParser.parseTypeName(name, TYPE_MANAGER);
    }

    @Test
    void testScalarTypes()
    {
        assertThat(parse("boolean")).isEqualTo(BOOLEAN);
        assertThat(parse("tinyint")).isEqualTo(TINYINT);
        assertThat(parse("smallint")).isEqualTo(SMALLINT);
        assertThat(parse("integer")).isEqualTo(INTEGER);
        assertThat(parse("bigint")).isEqualTo(BIGINT);
        assertThat(parse("real")).isInstanceOf(Type.class);
        assertThat(parse("double")).isEqualTo(DOUBLE);
        assertThat(parse("number")).isEqualTo(NUMBER);
    }

    @Test
    void testIntervalQualifiersAndPrecision()
    {
        for (String typeName : List.of("interval year(4)", "interval month(5)", "interval year(4) to month", "interval day(4)", "interval hour(6) to second(6)", "interval second(10, 6)")) {
            Type type = parse(typeName);
            assertThat(type).isNotNull();
            assertThat(TrinoTypeClassifier.requiresVarcharTransport(type)).isTrue();
            assertThat(TrinoParameterBindingFactory.createWriteMapping(type).orElseThrow().getDataType()).isEqualTo(type.getDisplayName());
        }

        Type picosecondInterval = parse("interval second(10, 12)");
        assertThat(picosecondInterval).isNotNull();
        assertThat(TrinoTypeClassifier.requiresVarcharTransport(picosecondInterval)).isFalse();
        assertThat(TrinoTypeClassifier.requiresJsonTransport(new ArrayType(picosecondInterval))).isFalse();
        assertThat(TrinoParameterBindingFactory.createWriteMapping(picosecondInterval)).isEmpty();
    }

    @Test
    void testLegacyIntervalMetadataUsesPersistedTypeDefaults()
    {
        // Same as the engine reading stored pre-484 metadata: millisecond fractional
        // precision and the maximum leading precision of the legacy types
        assertThat(parse("interval day to second")).isEqualTo(parse("interval day(9) to second(3)"));
        assertThat(parse("interval year to month")).isEqualTo(parse("interval year(9) to month"));
        assertThat(parse("array(interval day to second)"))
                .isEqualTo(parse("array(interval day(9) to second(3))"));
        assertThat(parse("row(ds interval day to second, ym interval year to month)"))
                .isEqualTo(parse("row(ds interval day(9) to second(3), ym interval year(9) to month)"));
        // Explicit qualifiers must retain their precision.
        assertThat(parse("interval day(2) to second(3)").getDisplayName()).isEqualTo("interval day(2) to second(3)");
    }

    @Test
    void testLegacyIntervalTypeNameDetection()
    {
        for (String typeName : List.of("interval day to second", "INTERVAL YEAR TO MONTH", "array(interval day to second)", "row(ds interval year to month)", "map(varchar, interval day to second)")) {
            assertThat(TrinoTypeNameParser.containsLegacyIntervalTypeName(typeName)).as(typeName).isTrue();
        }
        // Trino 484 and later report precision even for columns declared without one
        for (String typeName : List.of("interval day(2) to second(6)", "interval year(2) to month", "interval day(9) to second(3)", "interval second(2, 6)", "interval hour(2) to minute", "array(interval day(2) to second(6))", "varchar")) {
            assertThat(TrinoTypeNameParser.containsLegacyIntervalTypeName(typeName)).as(typeName).isFalse();
        }
    }

    @Test
    void testIntervalDetectionIncludesLongAndNestedIntervals()
    {
        for (String typeName : List.of("interval year(9) to month", "interval second(13, 12)", "array(interval day to second)", "map(varchar, row(ds interval second(13, 9)))")) {
            assertThat(TrinoTypeClassifier.containsIntervalType(parse(typeName))).as(typeName).isTrue();
        }
        assertThat(TrinoTypeClassifier.containsIntervalType(parse("row(ts timestamp(12), values array(bigint))"))).isFalse();
    }

    @Test
    void testIntervalTransportParsing()
    {
        assertIntervalValue("interval year(9)", "178956970", 2_147_483_640L);
        assertIntervalValue("interval month(10)", "-2147483648", Integer.MIN_VALUE);
        assertIntervalValue("interval year(9) to month", "-178956970-8", Integer.MIN_VALUE);
        assertIntervalValue("interval day(4)", "1234", 106_617_600_000_000L);
        assertIntervalValue("interval day(2) to hour", "2 03", 183_600_000_000L);
        assertIntervalValue("interval hour(6) to second(6)", "27:03:04.123456", 97_384_123_456L);
        assertIntervalValue("interval minute(6) to second(6)", "150:01.000001", 9_001_000_001L);
        assertIntervalValue("interval second(13, 6)", "0.000001", 1);
        assertIntervalValue("interval second(13, 6)", "-0.000001", -1);
        assertIntervalValue("interval day(9) to second(6)", "106751991 04:00:54.775807", Long.MAX_VALUE);
        assertIntervalValue("interval day(9) to second(6)", "-106751991 04:00:54.775808", Long.MIN_VALUE);
    }

    private void assertIntervalValue(String typeName, String value, long expected)
    {
        assertThat(TemporalTransportCodec.parseIntervalValue(value, parse(typeName))).isEqualTo(expected);
    }

    @Test
    void testVarcharTypes()
    {
        assertThat(parse("varchar")).isEqualTo(VARCHAR);
        assertThat(parse("varchar(255)")).isEqualTo(createVarcharType(255));
    }

    @Test
    void testBinaryAndSpecialTypes()
    {
        assertThat(parse("varbinary")).isEqualTo(VARBINARY);
        assertThat(parse("date")).isNotNull();
        assertThat(parse("uuid")).isNotNull();
    }

    @Test
    void testJsonAndIpAddressTypes()
    {
        // json and ipaddress are SPI types, parsed natively via TypeManager
        Type jsonType = parse("json");
        assertThat(jsonType).isNotNull();
        assertThat(jsonType.getDisplayName()).isEqualTo("json");

        Type ipAddressType = parse("ipaddress");
        assertThat(ipAddressType).isNotNull();
        assertThat(ipAddressType.getDisplayName()).isEqualTo("ipaddress");
    }

    @Test
    void testTimestampTypes()
    {
        assertThat(parse("timestamp")).isNotNull();
        assertThat(parse("timestamp(6)")).isNotNull();
        assertThat(parse("timestamp with time zone")).isNotNull();
        assertThat(parse("timestamp(3) with time zone")).isNotNull();
    }

    @Test
    void testDecimalType()
    {
        Type decimal = parse("decimal(10, 2)");
        assertThat(decimal).isNotNull();
        assertThat(decimal.getDisplayName()).isEqualTo("decimal(10,2)");
    }

    @Test
    void testSimpleArray()
    {
        Type type = parse("array(varchar)");
        assertThat(type).isInstanceOf(ArrayType.class);
        assertThat(((ArrayType) type).getElementType()).isEqualTo(VARCHAR);
    }

    @Test
    void testSimpleMap()
    {
        Type type = parse("map(varchar, varchar)");
        assertThat(type).isInstanceOf(MapType.class);
        MapType mapType = (MapType) type;
        assertThat(mapType.getKeyType()).isEqualTo(VARCHAR);
        assertThat(mapType.getValueType()).isEqualTo(VARCHAR);
    }

    @Test
    void testSimpleRow()
    {
        Type type = parse("row(name varchar, age integer)");
        assertThat(type).isInstanceOf(RowType.class);
        RowType rowType = (RowType) type;
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).getName()).hasValue("name");
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(VARCHAR);
        assertThat(rowType.getFields().get(1).getName()).hasValue("age");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(INTEGER);
    }

    @Test
    void testNestedArrayOfMap()
    {
        Type type = parse("array(map(varchar, varchar))");
        assertThat(type).isInstanceOf(ArrayType.class);
        ArrayType arrayType = (ArrayType) type;
        assertThat(arrayType.getElementType()).isInstanceOf(MapType.class);
        MapType mapType = (MapType) arrayType.getElementType();
        assertThat(mapType.getKeyType()).isEqualTo(VARCHAR);
        assertThat(mapType.getValueType()).isEqualTo(VARCHAR);
    }

    @Test
    void testDeeplyNestedRow()
    {
        String typeName = "row(svc varchar, evts array(map(varchar, varchar)), env map(varchar, varchar))";
        Type type = parse(typeName);
        assertThat(type).isInstanceOf(RowType.class);
        RowType rowType = (RowType) type;
        assertThat(rowType.getFields()).hasSize(3);

        // svc: varchar
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(VARCHAR);

        // evts: array(map(varchar, varchar))
        Type evtsType = rowType.getFields().get(1).getType();
        assertThat(evtsType).isInstanceOf(ArrayType.class);
        assertThat(((ArrayType) evtsType).getElementType()).isInstanceOf(MapType.class);

        // env: map(varchar, varchar)
        assertThat(rowType.getFields().get(2).getType()).isInstanceOf(MapType.class);
    }

    @Test
    void testSplitTopLevel()
    {
        List<String> parts = TrinoTypeNameParser.splitTopLevel("name varchar, data row(x integer, y integer), tags array(varchar)");
        assertThat(parts).hasSize(3);
        assertThat(parts.get(0).trim()).isEqualTo("name varchar");
        assertThat(parts.get(1).trim()).startsWith("data row(");
        assertThat(parts.get(2).trim()).startsWith("tags array(");
    }

    @Test
    void testSplitTopLevelWithQuotedFieldNames()
    {
        List<String> parts = TrinoTypeNameParser.splitTopLevel("\"a,b\" varchar, \"a) b\" integer, payload row(\"x,y\" varchar)");
        assertThat(parts).hasSize(3);
        assertThat(parts.get(0).trim()).isEqualTo("\"a,b\" varchar");
        assertThat(parts.get(1).trim()).isEqualTo("\"a) b\" integer");
        assertThat(parts.get(2).trim()).isEqualTo("payload row(\"x,y\" varchar)");
    }

    @Test
    void testFindTopLevelComma()
    {
        assertThat(TrinoTypeNameParser.findTopLevelComma("varchar, varchar")).isEqualTo(7);
        assertThat(TrinoTypeNameParser.findTopLevelComma("map(varchar, varchar), integer")).isEqualTo(21);
        String complexType = "row(\"a,b\" varchar), integer";
        assertThat(TrinoTypeNameParser.findTopLevelComma(complexType)).isEqualTo(complexType.indexOf(", integer"));
        assertThat(TrinoTypeNameParser.findTopLevelComma("varchar")).isEqualTo(-1);
    }

    @Test
    void testUnknownTypeReturnsNull()
    {
        assertThat(parse("not_a_real_type")).isNull();
    }

    @Test
    void testGeospatialTypesWithoutRegisteredPluginReturnNull()
    {
        assertThat(parse("Geometry")).isNull();
        assertThat(parse("SphericalGeography")).isNull();
    }

    @Test
    void testRowWithUnquotedSpacedFieldNames()
    {
        // Simulates JDBC driver stripping quotes: row("my field" varchar, "my count" integer)
        // becomes row(my field varchar, my count integer)
        Type type = parse("row(my field varchar, my count integer)");
        assertThat(type).isInstanceOf(RowType.class);
        RowType rowType = (RowType) type;
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).getName()).hasValue("my field");
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(VARCHAR);
        assertThat(rowType.getFields().get(1).getName()).hasValue("my count");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(INTEGER);
    }

    @Test
    void testRowWithUnquotedSpacedFieldAndComplexType()
    {
        Type type = parse("row(my tags array(varchar), my count integer)");
        assertThat(type).isInstanceOf(RowType.class);
        RowType rowType = (RowType) type;
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).getName()).hasValue("my tags");
        assertThat(rowType.getFields().get(0).getType()).isInstanceOf(ArrayType.class);
        assertThat(rowType.getFields().get(1).getName()).hasValue("my count");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(INTEGER);
    }

    @Test
    void testRowWithQuotedSpecialCharacterFieldNames()
    {
        Type type = parse("row(\"a,b\" varchar, \"a) b\" integer, \"a\"\"b\" bigint)");
        assertThat(type).isInstanceOf(RowType.class);
        RowType rowType = (RowType) type;
        assertThat(rowType.getFields()).hasSize(3);
        assertThat(rowType.getFields().get(0).getName()).hasValue("a,b");
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(VARCHAR);
        assertThat(rowType.getFields().get(1).getName()).hasValue("a) b");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(INTEGER);
        assertThat(rowType.getFields().get(2).getName()).hasValue("a\"b");
        assertThat(rowType.getFields().get(2).getType()).isEqualTo(BIGINT);
    }

    @Test
    void testSupportedComplexReadTypes()
    {
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(json)"))).isTrue();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("map(varchar, ipaddress)"))).isTrue();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(number)"))).isTrue();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("row(x number)"))).isTrue();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(time(0))"))).isTrue();
    }

    @Test
    void testUnsupportedComplexReadTypes()
    {
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(time with time zone)"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("map(varchar, interval day to second)"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("row(x interval day to second)"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(timestamp(6))"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(timestamp(12))"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("row(id uuid, ts timestamp(6) with time zone)"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(date)"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(time(1))"))).isFalse();
        assertThat(TrinoTypeClassifier.supportsComplexReadType(parse("array(time(9))"))).isFalse();

        assertThat(TrinoTypeClassifier.transportKind(parse("array(date)"))).isEqualTo(TrinoTypeClassifier.TransportKind.JSON_CAST);
        assertThat(TrinoTypeClassifier.transportKind(parse("array(time(3))"))).isEqualTo(TrinoTypeClassifier.TransportKind.JSON_CAST);
    }

    @Test
    void testJsonRowTransportPreservesNullRow()
    {
        JsonTransportHelper helper = new JsonTransportHelper(identifier -> "\"" + identifier + "\"");

        assertThat(helper.buildJsonTransportExpression("\"row_value\"", parse("row(ts timestamp(3), name varchar)")))
                .isEqualTo(
                        "CASE WHEN \"row_value\" IS NULL THEN NULL ELSE ARRAY[" +
                                "CAST((\"row_value\").\"ts\" AS VARCHAR), " +
                                "CAST((\"row_value\").\"name\" AS VARCHAR)] END");
    }

    @Test
    void testNumberJdbcTypeHandle()
    {
        assertThat(TrinoJdbcTypeHandleResolver.resolve(TYPE_MANAGER, "number").jdbcType()).isEqualTo(Types.OTHER);
        assertThat(TrinoRemoteSqlRenderer.jdbcTypeHandleFor(NUMBER).jdbcType()).isEqualTo(Types.OTHER);
    }
}
