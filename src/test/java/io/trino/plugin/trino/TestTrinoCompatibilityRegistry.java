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
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoCompatibilityRegistry
{
    private static final TypeManager TYPE_MANAGER = new TestingFunctionResolution().getPlannerContext().getTypeManager();

    private final TrinoCompatibilityRegistry registry = new TrinoCompatibilityRegistry();
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of("regexp_like", "count"));

    @Test
    void testAllowedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), capabilities)).isTrue();
    }

    @Test
    void testDeniedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("remote_only_custom_function"), capabilities)).isFalse();
    }

    @Test
    void testAllowlistedFunctionRequiresRemoteMetadata()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), TrinoRemoteCapabilities.forTesting(Set.of()))).isFalse();
    }

    @Test
    void testSessionSensitiveFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("current_timestamp"), capabilities)).isFalse();
    }

    @Test
    void testLocaleSensitiveFunctionsDenied()
    {
        TrinoRemoteCapabilities localeFunctions = capabilitiesWithRemoteTimeZone(
                "UTC",
                "date_format",
                "date_parse",
                "format_datetime");

        assertThat(registry.isFunctionSupported(SESSION, call("date_format"), localeFunctions)).isFalse();
        assertThat(registry.isFunctionSupported(SESSION, call("date_parse"), localeFunctions)).isFalse();
        assertThat(registry.isFunctionSupported(SESSION, call("format_datetime"), localeFunctions)).isFalse();
    }

    @Test
    void testQualifiedFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, new FunctionName(Optional.of(new CatalogSchemaName("memory", "default")), "regexp_like"), List.of()),
                capabilities)).isFalse();
    }

    @Test
    void testStandardOperatorDoesNotRequireRemoteFunctionMetadata()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of()),
                TrinoRemoteCapabilities.forTesting(Set.of()))).isTrue();
    }

    @Test
    void testCastAddingSessionTimeZoneDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, TIMESTAMP_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, DATE),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, TIME_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
    }

    @Test
    void testVarcharCastAddingSessionTimeZoneDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, VARCHAR),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, VARCHAR),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
    }

    @Test
    void testCastUsingValueTimeZoneAllowedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(DATE, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, TIMESTAMP_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, TIME_TZ_MILLIS),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isTrue();
    }

    @Test
    void testSessionSensitiveCastAllowedWhenRemoteTimeZoneMatches()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, TIMESTAMP_MILLIS),
                capabilitiesWithRemoteTimeZone("UTC"))).isTrue();
    }

    @Test
    void testCastDependingOnSessionStartAlwaysDenied()
    {
        TrinoRemoteCapabilities sameTimeZone = capabilitiesWithRemoteTimeZone("UTC");

        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_TZ_MILLIS, TIME_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_MILLIS, TIME_TZ_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIMESTAMP_TZ_MILLIS, TIME_TZ_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, TIMESTAMP_MILLIS), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(utcSession(), cast(TIME_TZ_MILLIS, VARCHAR), sameTimeZone)).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_TZ_MILLIS, CharType.createCharType(20)),
                sameTimeZone)).isFalse();
    }

    @Test
    void testCharToTimestampWithTimeZoneRequiresMatchingTimeZone()
    {
        Type charTimestamp = CharType.createCharType(19);

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, charTimestamp),
                capabilitiesWithRemoteTimeZone("Asia/Seoul"))).isFalse();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIMESTAMP_TZ_MILLIS, charTimestamp),
                capabilitiesWithRemoteTimeZone("UTC"))).isTrue();
    }

    @Test
    void testTryCastUsesSameSessionDependencyRules()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                tryCast(TIMESTAMP_MILLIS, TIME_MILLIS),
                capabilitiesWithRemoteTimeZone("UTC"))).isFalse();
    }

    @Test
    void testUnknownTemporalCastDenied()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(TIME_MILLIS, DATE),
                capabilitiesWithRemoteTimeZone("UTC"))).isFalse();
    }

    @Test
    void testSessionSensitiveContainerCastDeniedRecursively()
    {
        TrinoRemoteCapabilities sameTimeZone = capabilitiesWithRemoteTimeZone("UTC");
        TrinoRemoteCapabilities differentTimeZone = capabilitiesWithRemoteTimeZone("Asia/Seoul");

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(new ArrayType(TIMESTAMP_MILLIS), new ArrayType(TIME_MILLIS)),
                sameTimeZone)).isFalse();

        TypeOperators typeOperators = new TypeOperators();
        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(
                        new MapType(VARCHAR, TIMESTAMP_TZ_MILLIS, typeOperators),
                        new MapType(VARCHAR, TIMESTAMP_MILLIS, typeOperators)),
                differentTimeZone)).isFalse();

        assertThat(registry.isFunctionSupported(
                utcSession(),
                cast(
                        RowType.anonymous(List.of(VARCHAR, TIMESTAMP_TZ_MILLIS)),
                        RowType.anonymous(List.of(VARCHAR, TIMESTAMP_MILLIS))),
                sameTimeZone)).isTrue();
    }

    @Test
    void testFromIso8601TimestampDeniedWhenRemoteTimeZoneDiffers()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                fromIso8601Timestamp(),
                capabilitiesWithRemoteTimeZone("Asia/Seoul", "from_iso8601_timestamp"))).isFalse();
    }

    @Test
    void testFromIso8601TimestampAllowedWhenRemoteTimeZoneMatches()
    {
        assertThat(registry.isFunctionSupported(
                utcSession(),
                fromIso8601Timestamp(),
                capabilitiesWithRemoteTimeZone("UTC", "from_iso8601_timestamp"))).isTrue();
    }

    private static Call call(String functionName)
    {
        return new Call(BOOLEAN, new FunctionName(functionName), List.of());
    }

    private static Call cast(Type targetType, Type sourceType)
    {
        return new Call(targetType, StandardFunctions.CAST_FUNCTION_NAME, List.of(new Constant(constantValue(sourceType), sourceType)));
    }

    private static Call tryCast(Type targetType, Type sourceType)
    {
        return new Call(targetType, StandardFunctions.TRY_CAST_FUNCTION_NAME, List.of(new Constant(constantValue(sourceType), sourceType)));
    }

    private static Call fromIso8601Timestamp()
    {
        return new Call(TIMESTAMP_TZ_MILLIS, new FunctionName("from_iso8601_timestamp"), List.of(new Constant(utf8Slice("2024-01-15T00:00:00"), VARCHAR)));
    }

    private static Object constantValue(Type type)
    {
        if (type instanceof VarcharType || type instanceof CharType) {
            return utf8Slice("2024-01-15T00:00:00");
        }
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return null;
        }
        return 0L;
    }

    private static ConnectorSession utcSession()
    {
        return TestingConnectorSession.builder()
                .setTimeZoneKey(getTimeZoneKey("UTC"))
                .build();
    }

    private static TrinoRemoteCapabilities capabilitiesWithRemoteTimeZone(String timeZone, String... functions)
    {
        return new TrinoRemoteCapabilities(
                Optional.of("484"),
                true,
                List.of(),
                Optional.of(Set.of(functions)),
                Optional.of(timeZone));
    }

    @Test
    void testOtherVersionRemoteReceivesOnlyBasicCalls()
    {
        Set<String> functions = Set.of("lower", "length");
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(functions);
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.forTestingOtherVersion(functions);

        List<Call> fullListOnly = List.of(
                function(VARCHAR, "lower", variable(VARCHAR)),
                cast(BIGINT, variable(INTEGER)),
                tryCast(BIGINT, variable(VARCHAR)),
                operator(BIGINT, StandardFunctions.ADD_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BIGINT, StandardFunctions.SUBTRACT_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BIGINT, StandardFunctions.MULTIPLY_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BIGINT, StandardFunctions.DIVIDE_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BIGINT, StandardFunctions.MODULUS_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BIGINT, StandardFunctions.NEGATE_FUNCTION_NAME, variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.LIKE_FUNCTION_NAME, variable(VARCHAR), variable(VARCHAR)),
                operator(BIGINT, StandardFunctions.NULLIF_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)));
        for (Call call : fullListOnly) {
            assertThat(registry.isFunctionSupported(SESSION, call, sameVersion)).as("same version: %s", call).isTrue();
            assertThat(registry.isFunctionSupported(SESSION, call, otherVersion)).as("other version: %s", call).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, sameVersion)).as("same version: %s", call).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, otherVersion)).as("other version: %s", call).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, TrinoRemoteCapabilities.unavailable())).as("unavailable: %s", call).isFalse();
        }

        List<Call> basic = List.of(
                operator(BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, variable(BOOLEAN), variable(BOOLEAN)),
                operator(BOOLEAN, StandardFunctions.OR_FUNCTION_NAME, variable(BOOLEAN), variable(BOOLEAN)),
                operator(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, variable(BOOLEAN)),
                operator(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, variable(VARCHAR)),
                operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                operator(BOOLEAN, StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT)),
                in(variable(BIGINT), variable(BIGINT), new Constant(1L, BIGINT)));
        for (Call call : basic) {
            assertThat(registry.isFunctionSupported(SESSION, call, sameVersion)).as("same version: %s", call).isTrue();
            assertThat(registry.isFunctionSupported(SESSION, call, otherVersion)).as("other version: %s", call).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, otherVersion)).as("other version: %s", call).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, TrinoRemoteCapabilities.unavailable())).as("unavailable: %s", call).isTrue();
        }
    }

    @Test
    void testBasicExpressionChecksNestedCalls()
    {
        // The engine also pushes down the arguments of an expression on their own, so the
        // check cannot depend on the shape of the expression
        Call lengthComparison = operator(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                function(BIGINT, "length", variable(VARCHAR)),
                variable(BIGINT));
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(lengthComparison)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, lengthComparison))).isFalse();

        Call comparison = operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(VARCHAR), new Constant(utf8Slice("x"), VARCHAR));
        Call isNull = operator(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, variable(DATE));
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(
                BOOLEAN,
                StandardFunctions.NOT_FUNCTION_NAME,
                operator(BOOLEAN, StandardFunctions.OR_FUNCTION_NAME, comparison, isNull)))).isTrue();

        Type json = TYPE_MANAGER.fromSqlType("json");
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(new FieldDereference(VARCHAR, variable(RowType.anonymous(List.of(INTEGER, VARCHAR))), 1))).isTrue();
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(new FieldDereference(VARCHAR, variable(RowType.anonymous(List.of(json, VARCHAR))), 1))).isFalse();
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, variable(json)))).isFalse();
    }

    @Test
    void testBasicTypes()
    {
        List<Type> scalarTypes = List.of(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(18, 2),
                createDecimalType(38, 10),
                VARCHAR,
                createVarcharType(10),
                CharType.createCharType(3),
                VARBINARY,
                DATE,
                TIME_MILLIS,
                createTimeType(12),
                TIME_TZ_MILLIS,
                TIMESTAMP_MILLIS,
                createTimestampType(12),
                TIMESTAMP_TZ_MILLIS,
                createTimestampWithTimeZoneType(12),
                UUID,
                TYPE_MANAGER.fromSqlType("ipaddress"));
        for (Type type : scalarTypes) {
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(type)).as("equality: %s", type).isTrue();
            assertThat(TrinoCompatibilityRegistry.isBasicOrderingType(type)).as("ordering: %s", type).isTrue();
        }

        // Servers before 482 compare number values by their bytes, and servers before 484
        // have other interval types and normalize JSON text
        List<Type> excludedTypes = List.of(
                NUMBER,
                TYPE_MANAGER.fromSqlType("json"),
                TYPE_MANAGER.fromSqlType("interval day(2) to second(6)"),
                TYPE_MANAGER.fromSqlType("interval year(2) to month"));
        for (Type type : excludedTypes) {
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(type)).as("equality: %s", type).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicOrderingType(type)).as("ordering: %s", type).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(new ArrayType(type))).as("equality: array(%s)", type).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(RowType.anonymous(List.of(BIGINT, type)))).as("equality: row(bigint, %s)", type).isFalse();
        }

        // Servers before 482 fail to order arrays and rows when a null element decides the order
        List<Type> containerTypes = List.of(
                new ArrayType(BIGINT),
                RowType.anonymous(List.of(INTEGER, VARCHAR)),
                new MapType(VARCHAR, BIGINT, new TypeOperators()),
                new ArrayType(RowType.anonymous(List.of(DATE, new ArrayType(DOUBLE)))));
        for (Type type : containerTypes) {
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(type)).as("equality: %s", type).isTrue();
            assertThat(TrinoCompatibilityRegistry.isBasicOrderingType(type)).as("ordering: %s", type).isFalse();
        }
    }

    @Test
    void testBasicComparisonsFollowTheirArgumentTypes()
    {
        Type array = new ArrayType(BIGINT);
        Type row = RowType.anonymous(List.of(INTEGER, VARCHAR));
        for (FunctionName equality : List.of(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, equality, variable(VARCHAR), variable(VARCHAR)))).as("%s on varchar", equality).isTrue();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, equality, variable(NUMBER), variable(NUMBER)))).as("%s on number", equality).isFalse();
        }
        // An equality join on arrays and rows makes servers before 482 order the values for
        // a dynamic filter, which fails for a null element
        for (FunctionName equal : List.of(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, equal, variable(array), variable(array)))).as("%s on arrays", equal).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, equal, variable(row), variable(row)))).as("%s on rows", equal).isFalse();
        }
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, variable(array), variable(array)))).isTrue();
        assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, variable(row), variable(row)))).isTrue();
        for (FunctionName ordering : List.of(
                StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, ordering, variable(TIMESTAMP_TZ_MILLIS), variable(TIMESTAMP_TZ_MILLIS)))).as("%s on timestamps", ordering).isTrue();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, ordering, variable(array), variable(array)))).as("%s on arrays", ordering).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, ordering, variable(row), variable(row)))).as("%s on rows", ordering).isFalse();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, ordering, variable(NUMBER), variable(NUMBER)))).as("%s on number", ordering).isFalse();
        }
    }

    @Test
    void testBasicEqualLeavesOutRawComparedTypes()
    {
        // Servers before 484 compare a list of 2 to 7 such values by their raw form, also in
        // the dynamic filter of an equality join. A join on a real column then misses the
        // rows whose zero has the other sign.
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of());
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.forTestingOtherVersion(Set.of());
        for (Type type : List.of(REAL, TIME_TZ_MILLIS, TIMESTAMP_TZ_MILLIS, createTimestampWithTimeZoneType(12))) {
            for (FunctionName equal : List.of(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME)) {
                Call call = operator(BOOLEAN, equal, variable(type), variable(type));
                assertThat(TrinoCompatibilityRegistry.isBasicExpression(call)).as("%s on %s", equal, type).isFalse();
                assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, sameVersion)).as("%s on %s", equal, type).isTrue();
            }
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, variable(type), variable(type))))
                    .as("<> on %s", type)
                    .isTrue();
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(operator(BOOLEAN, StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, variable(type), variable(type))))
                    .as("< on %s", type)
                    .isTrue();
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, otherVersion)).as("join on %s", type).isFalse();
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, sameVersion)).as("join on %s", type).isTrue();
            // GROUP BY and DISTINCT do not build such a list
            assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(type, otherVersion)).as("grouping on %s", type).isTrue();
        }
        for (Type type : List.of(DOUBLE, BIGINT, VARCHAR, TIMESTAMP_MILLIS, UUID)) {
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, otherVersion)).as("join on %s", type).isTrue();
        }
        for (Type type : List.of(new ArrayType(BIGINT), RowType.anonymous(List.of(INTEGER, VARCHAR)), new MapType(VARCHAR, BIGINT, new TypeOperators()))) {
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, otherVersion)).as("join on %s", type).isFalse();
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, sameVersion)).as("join on %s", type).isTrue();
            assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(type, otherVersion)).as("grouping on %s", type).isTrue();
        }
    }

    @Test
    void testBasicInLeavesOutRawComparedTypes()
    {
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, DATE, TIMESTAMP_MILLIS, createDecimalType(38, 10))) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(in(variable(type), variable(type), variable(type)))).as("IN on %s", type).isTrue();
        }
        // Servers before 484 compare a short IN list by the raw values, which differ for
        // equal values with another time zone or sign of zero
        for (Type type : List.of(REAL, TIME_TZ_MILLIS, TIMESTAMP_TZ_MILLIS)) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(in(variable(type), variable(type), variable(type)))).as("IN on %s", type).isFalse();
        }
        // The connector cannot send uuid and ipaddress constants in an expression
        for (Type type : List.of(UUID, TYPE_MANAGER.fromSqlType("ipaddress"))) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(in(variable(type), variable(type), variable(type)))).as("IN on %s", type).isFalse();
        }
        // Trino 448 checks too few elements of a dictionary of arrays for null
        for (Type type : List.of(new ArrayType(BIGINT), RowType.anonymous(List.of(INTEGER, VARCHAR)), new MapType(VARCHAR, BIGINT, new TypeOperators()), NUMBER)) {
            assertThat(TrinoCompatibilityRegistry.isBasicExpression(in(variable(type), variable(type), variable(type)))).as("IN on %s", type).isFalse();
        }
    }

    @Test
    void testBasicAggregates()
    {
        Type row = RowType.anonymous(List.of(INTEGER, VARCHAR));
        Type longDecimal = createDecimalType(38, 2);
        Type shortDecimal = createDecimalType(18, 2);
        Call comparison = operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT));
        Call sumOfColumns = operator(BIGINT, StandardFunctions.ADD_FUNCTION_NAME, variable(BIGINT), variable(BIGINT));
        Call functionFilter = operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, function(BIGINT, "length", variable(VARCHAR)), variable(BIGINT));

        List<AggregateFunction> basic = List.of(
                aggregate("count", BIGINT),
                aggregate("count", BIGINT, variable(VARCHAR)),
                new AggregateFunction("count", BIGINT, List.of(variable(VARCHAR)), List.of(), true, Optional.empty()),
                aggregate("count", BIGINT, variable(row)),
                aggregate("count_if", BIGINT, comparison),
                aggregate("min", VARCHAR, variable(VARCHAR)),
                aggregate("max", TIMESTAMP_TZ_MILLIS, variable(TIMESTAMP_TZ_MILLIS)),
                aggregate("sum", BIGINT, variable(BIGINT)),
                aggregate("sum", longDecimal, variable(longDecimal)),
                aggregate("avg", DOUBLE, variable(BIGINT)),
                aggregate("avg", shortDecimal, variable(shortDecimal)),
                aggregate("checksum", VARBINARY, variable(VARCHAR)),
                new AggregateFunction("count", BIGINT, List.of(variable(VARCHAR)), List.of(), false, Optional.of(comparison)));
        List<AggregateFunction> fullListOnly = List.of(
                // Servers before 484 return a wrong average when the sum overflows 128 bits
                aggregate("avg", longDecimal, variable(longDecimal)),
                // Servers before 482 fail to order arrays and rows with a deciding null element
                aggregate("max", row, variable(row)),
                aggregate("min", new ArrayType(BIGINT), variable(new ArrayType(BIGINT))),
                aggregate("sum", NUMBER, variable(NUMBER)),
                // The engine casts smaller integer types to bigint, and casts are not basic
                aggregate("sum", BIGINT, variable(INTEGER)),
                aggregate("avg", DOUBLE, variable(SMALLINT)),
                aggregate("count", BIGINT, variable(TYPE_MANAGER.fromSqlType("json"))),
                aggregate("sum", BIGINT, sumOfColumns),
                new AggregateFunction("count", BIGINT, List.of(variable(VARCHAR)), List.of(), false, Optional.of(functionFilter)),
                aggregate("array_agg", new ArrayType(BIGINT), variable(BIGINT)));

        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of("count", "count_if", "min", "max", "sum", "avg", "checksum"));
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.forTestingOtherVersion(Set.of("count", "count_if", "min", "max", "sum", "avg", "checksum"));
        for (AggregateFunction aggregate : basic) {
            assertThat(TrinoCompatibilityRegistry.isBasicAggregate(aggregate)).as(aggregate.toString()).isTrue();
            assertThat(registry.isAggregationSupported(aggregate, otherVersion)).as(aggregate.toString()).isTrue();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(aggregate, TrinoRemoteCapabilities.unavailable())).as(aggregate.toString()).isTrue();
        }
        for (AggregateFunction aggregate : fullListOnly) {
            assertThat(TrinoCompatibilityRegistry.isBasicAggregate(aggregate)).as(aggregate.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(aggregate, otherVersion)).as(aggregate.toString()).isFalse();
            assertThat(registry.isAggregationSupported(aggregate, otherVersion)).as(aggregate.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(aggregate, sameVersion)).as(aggregate.toString()).isTrue();
        }
        // The allowlist still applies to a remote server of the local version
        assertThat(registry.isAggregationSupported(aggregate("avg", longDecimal, variable(longDecimal)), sameVersion)).isTrue();
        assertThat(registry.isAggregationSupported(aggregate("array_agg", new ArrayType(BIGINT), variable(BIGINT)), sameVersion)).isFalse();
    }

    @Test
    void testBasicAggregationGrouping()
    {
        AggregateFunction distinctCountA = new AggregateFunction("count", BIGINT, List.of(new Variable("a", BIGINT)), List.of(), true, Optional.empty());
        AggregateFunction distinctCountB = new AggregateFunction("count", BIGINT, List.of(new Variable("b", BIGINT)), List.of(), true, Optional.empty());
        AggregateFunction distinctSumA = new AggregateFunction("sum", BIGINT, List.of(new Variable("a", BIGINT)), List.of(), true, Optional.empty());
        AggregateFunction countB = new AggregateFunction("count", BIGINT, List.of(new Variable("b", BIGINT)), List.of(), false, Optional.empty());
        List<List<Type>> grouped = List.of(List.of(VARCHAR));
        List<List<Type>> global = List.of(List.of());
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of());
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.forTestingOtherVersion(Set.of());

        // Before 484, a grouped aggregation with DISTINCT aggregates over different
        // arguments could drop the group of NULL keys
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(distinctCountA, distinctCountB), grouped, otherVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(distinctCountA, distinctCountB), grouped, sameVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(distinctCountA, distinctCountB), global, otherVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(distinctCountA, distinctSumA), grouped, otherVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(distinctCountA, countB), grouped, otherVersion)).isTrue();

        // Before 482, GROUP BY on number values could split equal values into groups
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(countB), List.of(List.of(NUMBER)), otherVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(countB), List.of(List.of(NUMBER)), sameVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(countB), List.of(List.of(VARCHAR, new ArrayType(BIGINT))), otherVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(countB), List.of(List.of(VARCHAR), List.of(TYPE_MANAGER.fromSqlType("json"))), otherVersion)).isFalse();
    }

    @Test
    void testOrderingAndEqualityDelegation()
    {
        Type array = new ArrayType(BIGINT);
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of());
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.forTestingOtherVersion(Set.of());

        assertThat(TrinoCompatibilityRegistry.isOrderingDelegable(BIGINT, otherVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isOrderingDelegable(array, otherVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isOrderingDelegable(array, sameVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(array, otherVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(NUMBER, otherVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(NUMBER, sameVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(NUMBER, TrinoRemoteCapabilities.unavailable())).isFalse();
    }

    @Test
    void testRemoteWithOtherSettingsReceivesOnlyTheBasicList()
    {
        Call lower = function(VARCHAR, "lower", variable(VARCHAR));
        Call equal = operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(BIGINT), variable(BIGINT));
        AggregateFunction longDecimalAverage = aggregate("avg", createDecimalType(38, 2), variable(createDecimalType(38, 2)));
        for (String setting : TrinoRemoteCapabilities.EVALUATION_SETTINGS) {
            TrinoRemoteCapabilities otherSettings = TrinoRemoteCapabilities.forTestingOtherSettings(Set.of("lower", "avg"), List.of(setting));

            assertThat(registry.isFunctionSupported(SESSION, lower, otherSettings)).as(setting).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(lower, otherSettings)).as(setting).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(equal, otherSettings)).as(setting).isTrue();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(longDecimalAverage, otherSettings)).as(setting).isFalse();
            assertThat(TrinoCompatibilityRegistry.isOrderingDelegable(new ArrayType(BIGINT), otherSettings)).as(setting).isFalse();
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(REAL, otherSettings)).as(setting).isFalse();
            assertThat(TrinoCompatibilityRegistry.isJoinDelegable(JoinType.FULL_OUTER, otherSettings)).as(setting).isFalse();
        }
    }

    @Test
    void testFullJoinRequiresRemoteThatEvaluatesAsLocal()
    {
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of());
        // Servers before 484 drop the unmatched row of an input with exactly one row when the
        // join condition refers only to the other input
        for (TrinoRemoteCapabilities other : List.of(TrinoRemoteCapabilities.forTestingOtherVersion(Set.of()), TrinoRemoteCapabilities.unavailable())) {
            assertThat(TrinoCompatibilityRegistry.isJoinDelegable(JoinType.FULL_OUTER, other)).isFalse();
            for (JoinType joinType : List.of(JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)) {
                assertThat(TrinoCompatibilityRegistry.isJoinDelegable(joinType, other)).as(joinType.toString()).isTrue();
            }
        }
        for (JoinType joinType : JoinType.values()) {
            assertThat(TrinoCompatibilityRegistry.isJoinDelegable(joinType, sameVersion)).as(joinType.toString()).isTrue();
        }
    }

    @Test
    void testDecimalArithmeticRequiresTheDefaultResultType()
    {
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of("sum"));

        record Arithmetic(FunctionName operator, Type left, Type right, Type defaultResult, Type legacyResult) {}
        // The legacy result is the type that a local server with
        // deprecated.legacy-arithmetic-decimal-operators derives

        List<Arithmetic> differing = List.of(
                new Arithmetic(StandardFunctions.DIVIDE_FUNCTION_NAME, createDecimalType(3, 2), createDecimalType(3, 2), createDecimalType(9, 6), createDecimalType(5, 2)),
                new Arithmetic(StandardFunctions.DIVIDE_FUNCTION_NAME, createDecimalType(38, 10), createDecimalType(18, 4), createDecimalType(38, 6), createDecimalType(38, 10)),
                new Arithmetic(StandardFunctions.MULTIPLY_FUNCTION_NAME, createDecimalType(38, 10), createDecimalType(38, 10), createDecimalType(38, 6), createDecimalType(38, 20)),
                new Arithmetic(StandardFunctions.ADD_FUNCTION_NAME, createDecimalType(38, 30), createDecimalType(38, 0), createDecimalType(38, 0), createDecimalType(38, 30)),
                new Arithmetic(StandardFunctions.SUBTRACT_FUNCTION_NAME, createDecimalType(38, 30), createDecimalType(38, 0), createDecimalType(38, 0), createDecimalType(38, 30)));
        for (Arithmetic arithmetic : differing) {
            Call defaultCall = operator(arithmetic.defaultResult(), arithmetic.operator(), variable(arithmetic.left()), variable(arithmetic.right()));
            Call legacyCall = operator(arithmetic.legacyResult(), arithmetic.operator(), variable(arithmetic.left()), variable(arithmetic.right()));

            assertThat(registry.isFunctionSupported(SESSION, defaultCall, sameVersion)).as(defaultCall.toString()).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(defaultCall, sameVersion)).as(defaultCall.toString()).isTrue();
            assertThat(registry.isFunctionSupported(SESSION, legacyCall, sameVersion)).as(legacyCall.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(legacyCall, sameVersion)).as(legacyCall.toString()).isFalse();
            // The baseline JDBC rewriter and aggregations pass the same check for nested calls
            Call comparison = operator(BOOLEAN, StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, legacyCall, variable(arithmetic.legacyResult()));
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(comparison, sameVersion)).as(comparison.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(aggregate("sum", createDecimalType(38, 0), legacyCall), sameVersion)).as(legacyCall.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(aggregate("sum", createDecimalType(38, 0), defaultCall), sameVersion)).as(defaultCall.toString()).isTrue();
        }

        // Both rules derive the same type, so the remote server returns the same value
        List<Call> sameType = List.of(
                operator(createDecimalType(4, 2), StandardFunctions.ADD_FUNCTION_NAME, variable(createDecimalType(3, 2)), variable(createDecimalType(3, 2))),
                operator(createDecimalType(4, 2), StandardFunctions.SUBTRACT_FUNCTION_NAME, variable(createDecimalType(3, 2)), variable(createDecimalType(3, 2))),
                operator(createDecimalType(6, 4), StandardFunctions.MULTIPLY_FUNCTION_NAME, variable(createDecimalType(3, 2)), variable(createDecimalType(3, 2))),
                // The setting does not change the modulus, and it applies to decimal values only
                operator(createDecimalType(3, 2), StandardFunctions.MODULUS_FUNCTION_NAME, variable(createDecimalType(3, 2)), variable(createDecimalType(3, 2))),
                operator(DOUBLE, StandardFunctions.DIVIDE_FUNCTION_NAME, variable(DOUBLE), variable(DOUBLE)));
        for (Call call : sameType) {
            assertThat(registry.isFunctionSupported(SESSION, call, sameVersion)).as(call.toString()).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, sameVersion)).as(call.toString()).isTrue();
        }
    }

    @Test
    void testOperationsThatCompareJsonValuesStayLocal()
    {
        Type json = TYPE_MANAGER.fromSqlType("json");
        Type jsonArray = new ArrayType(json);
        Type jsonRow = RowType.anonymous(List.of(BIGINT, json));
        Type jsonValueMap = new MapType(VARCHAR, json, new TypeOperators());
        Type jsonKeyMap = new MapType(json, VARCHAR, new TypeOperators());
        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.forTesting(Set.of(
                "contains",
                "array_position",
                "array_distinct",
                "array_join",
                "cardinality",
                "concat",
                "element_at",
                "json_array_contains",
                "json_extract",
                "json_extract_scalar",
                "json_format",
                "count",
                "checksum"));

        // The remote server sends a date or time inside a JSON value as a JSON string, so it
        // tells apart values that are equal for the local server
        List<Call> comparing = List.of(
                operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(json), variable(json)),
                operator(BOOLEAN, StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, variable(json), variable(json)),
                operator(BOOLEAN, StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME, variable(json), variable(json)),
                operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(jsonArray), variable(jsonArray)),
                operator(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, variable(jsonRow), variable(jsonRow)),
                operator(json, StandardFunctions.NULLIF_FUNCTION_NAME, variable(json), variable(json)),
                in(variable(json), variable(json), variable(json)),
                function(BOOLEAN, "contains", variable(jsonArray), variable(json)),
                function(BIGINT, "array_position", variable(jsonArray), variable(json)),
                function(jsonArray, "array_distinct", variable(jsonArray)),
                function(VARCHAR, "element_at", variable(jsonKeyMap), variable(json)),
                function(VARCHAR, "$operator$subscript", variable(jsonKeyMap), variable(json)));
        for (Call call : comparing) {
            assertThat(registry.isFunctionSupported(SESSION, call, sameVersion)).as(call.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, sameVersion)).as(call.toString()).isFalse();
            Call nested = operator(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, call);
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(nested, sameVersion)).as(nested.toString()).isFalse();
        }

        // These read a JSON value as text or by position, or only pass it on
        List<Call> reading = List.of(
                operator(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, variable(json)),
                cast(VARCHAR, variable(json)),
                tryCast(DATE, variable(json)),
                cast(json, variable(jsonArray)),
                operator(jsonArray, StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME, variable(json), variable(json)),
                function(json, "json_extract", variable(json), variable(VARCHAR)),
                function(VARCHAR, "json_extract_scalar", variable(json), variable(VARCHAR)),
                function(VARCHAR, "json_format", variable(json)),
                function(BOOLEAN, "json_array_contains", variable(json), variable(VARCHAR)),
                function(BIGINT, "cardinality", variable(jsonArray)),
                function(VARCHAR, "array_join", variable(jsonArray), variable(VARCHAR)),
                function(jsonArray, "concat", variable(jsonArray), variable(jsonArray)),
                function(json, "element_at", variable(jsonArray), variable(BIGINT)),
                function(json, "element_at", variable(jsonValueMap), variable(VARCHAR)),
                function(json, "$operator$subscript", variable(jsonArray), variable(BIGINT)),
                function(json, "$operator$subscript", variable(jsonValueMap), variable(VARCHAR)));
        for (Call call : reading) {
            assertThat(registry.isFunctionSupported(SESSION, call, sameVersion)).as(call.toString()).isTrue();
            assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(call, sameVersion)).as(call.toString()).isTrue();
        }
        assertThat(TrinoCompatibilityRegistry.isExpressionDelegable(new FieldDereference(json, variable(jsonRow), 1), sameVersion)).isTrue();

        // count checks a JSON value only for NULL. DISTINCT and checksum compare or hash it.
        assertThat(registry.isAggregationSupported(aggregate("count", BIGINT, variable(json)), sameVersion)).isTrue();
        assertThat(registry.isAggregationSupported(aggregate("count", BIGINT), sameVersion)).isTrue();
        assertThat(registry.isAggregationSupported(aggregate("checksum", VARBINARY, variable(json)), sameVersion)).isFalse();
        assertThat(registry.isAggregationSupported(aggregate("checksum", VARBINARY, variable(jsonArray)), sameVersion)).isFalse();
        AggregateFunction distinctCount = new AggregateFunction("count", BIGINT, List.of(variable(json)), List.of(), true, Optional.empty());
        assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(distinctCount, sameVersion)).isFalse();
        AggregateFunction distinctBigintCount = new AggregateFunction("count", BIGINT, List.of(variable(BIGINT)), List.of(), true, Optional.empty());
        assertThat(TrinoCompatibilityRegistry.isAggregateDelegable(distinctBigintCount, sameVersion)).isTrue();

        // GROUP BY, DISTINCT, and a join on equality compare the values of a column
        AggregateFunction count = aggregate("count", BIGINT);
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(count), List.of(List.of(VARCHAR, json)), sameVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(count), List.of(List.of(VARCHAR), List.of(jsonRow)), sameVersion)).isFalse();
        assertThat(TrinoCompatibilityRegistry.isAggregationDelegable(List.of(count), List.of(List.of(VARCHAR, new ArrayType(BIGINT))), sameVersion)).isTrue();
        for (Type type : List.of(json, jsonArray, jsonRow, jsonValueMap)) {
            assertThat(TrinoCompatibilityRegistry.isEqualDelegable(type, sameVersion)).as(type.toString()).isFalse();
            assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(type, sameVersion)).as(type.toString()).isFalse();
        }
        assertThat(TrinoCompatibilityRegistry.isEqualDelegable(REAL, sameVersion)).isTrue();
        assertThat(TrinoCompatibilityRegistry.isEqualityDelegable(new ArrayType(VARCHAR), sameVersion)).isTrue();
    }

    @Test
    void testSmokeTestCoversTheBasicList()
            throws IOException
    {
        // The basic list may hold only what the remote version smoke test compares with
        // local evaluation on each remote release that it runs
        Map<Type, Set<String>> operationsByType = new HashMap<>();
        for (String line : Files.readAllLines(Path.of("testing/remote-version-smoke/basic-pushdown-types.txt"))) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            // The values, which are the last field, can contain the concatenation operator
            String[] fields = line.split("\\|", 4);
            assertThat(fields).as(line).hasSize(4);
            Set<String> operations = operationsByType.computeIfAbsent(TYPE_MANAGER.fromSqlType(fields[1]), _ -> new HashSet<>());
            for (String operation : fields[2].split(",")) {
                if (!operation.isEmpty()) {
                    operations.add(operation);
                }
            }
        }

        Set<String> scalarTypes = new HashSet<>();
        Set<String> containerTypes = new HashSet<>();
        operationsByType.forEach((type, operations) -> {
            assertThat(TrinoCompatibilityRegistry.isBasicEqualityType(type)).as("equality: %s", type).isTrue();
            assertThat(operations.contains("equal")).as("equal: %s", type).isEqualTo(TrinoCompatibilityRegistry.isBasicEqualType(type));
            assertThat(operations.contains("order")).as("ordering: %s", type).isEqualTo(TrinoCompatibilityRegistry.isBasicOrderingType(type));
            assertThat(operations.contains("in")).as("IN: %s", type).isEqualTo(TrinoCompatibilityRegistry.isBasicInType(type));
            assertThat(operations.contains("sum")).as("sum: %s", type).isEqualTo(TrinoCompatibilityRegistry.isBasicSumType(type));
            assertThat(operations.contains("avg")).as("avg: %s", type).isEqualTo(TrinoCompatibilityRegistry.isBasicAverageType(type));
            if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
                containerTypes.add(type.getBaseName());
            }
            else {
                scalarTypes.add(type.getBaseName());
            }
        });
        assertThat(scalarTypes).isEqualTo(TrinoCompatibilityRegistry.BASIC_SCALAR_TYPES);
        assertThat(containerTypes).containsExactlyInAnyOrder("array", "map", "row");
    }

    private static AggregateFunction aggregate(String name, Type outputType, ConnectorExpression... arguments)
    {
        return new AggregateFunction(name, outputType, List.of(arguments), List.of(), false, Optional.empty());
    }

    private static Call in(ConnectorExpression value, ConnectorExpression... values)
    {
        return new Call(
                BOOLEAN,
                StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
                List.of(value, new Call(new ArrayType(value.getType()), StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME, List.of(values))));
    }

    private static Variable variable(Type type)
    {
        return new Variable("v", type);
    }

    private static Call function(Type returnType, String name, ConnectorExpression... arguments)
    {
        return new Call(returnType, new FunctionName(name), List.of(arguments));
    }

    private static Call operator(Type returnType, FunctionName name, ConnectorExpression... arguments)
    {
        return new Call(returnType, name, List.of(arguments));
    }

    private static Call cast(Type targetType, ConnectorExpression source)
    {
        return new Call(targetType, StandardFunctions.CAST_FUNCTION_NAME, List.of(source));
    }

    private static Call tryCast(Type targetType, ConnectorExpression source)
    {
        return new Call(targetType, StandardFunctions.TRY_CAST_FUNCTION_NAME, List.of(source));
    }
}
