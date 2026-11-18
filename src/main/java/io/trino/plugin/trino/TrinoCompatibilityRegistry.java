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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.AggregateFunction;
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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Decides which expressions and aggregations the remote server may evaluate.
 * <p>
 * A remote server that reports the version of the local server, and that the settings
 * probe of {@link TrinoRemoteCapabilities} shows to have default settings, evaluates
 * everything as a local server with default settings does. It receives the full list: the
 * standard operators, the functions and aggregations of the allowlists, casts, and
 * arithmetic, except where the result depends on the session.
 * <p>
 * The full list leaves out two kinds of operations, for which the remote server can
 * return another result than the local server:
 * <ul>
 * <li>Operations that compare or hash JSON values: {@code =}, {@code <>},
 * {@code IS DISTINCT FROM}, {@code IN}, {@code NULLIF}, {@code contains},
 * {@code array_position}, {@code array_distinct}, {@code checksum}, {@code GROUP BY}, and
 * DISTINCT. A JSON value of Trino 484 can hold a date or time, which the remote server
 * sends as a JSON string. The remote server tells the two apart, and the local server
 * cannot. Operations that read a JSON value as text or by position stay in the list.
 * <li>Decimal {@code +}, {@code -}, {@code *}, and {@code /} whose result type the local
 * server did not derive with the default rules of Trino 484. A local server with
 * {@code deprecated.legacy-arithmetic-decimal-operators} uses the earlier rules, and the
 * connector cannot read that setting.
 * </ul>
 * Every other remote server receives only the basic list: operations on types for which
 * the remote version smoke test compares the pushed down result with local evaluation on
 * the releases that it runs. The basic list has no condition on a remote release. When a
 * release turns out to evaluate an entry differently, remove the entry, so that the
 * local server evaluates it. The connector does not work around a bug of a remote
 * release with a probe or with a rewrite of the SQL that it sends.
 * <p>
 * The basic list leaves out these operations. An entry can return when the oldest
 * release that the smoke test runs has the fix.
 * <ul>
 * <li>Functions, casts, arithmetic, LIKE, and NULLIF. Their behavior changes from release
 * to release, and Trino 482 drops the negation of a negated NULLIF over a cast.
 * <li>{@code number} values. Servers before Trino 482 compare them by their bytes.
 * <li>Ordering of arrays and rows, which {@code min}, {@code max}, and {@code ORDER BY}
 * also use. Servers before Trino 482 fail when a null element decides the order.
 * <li>{@code IN} on arrays, maps, and rows. Trino 448 checks too few elements for null.
 * <li>{@code =} and {@code IS NOT DISTINCT FROM} on arrays, maps, and rows. Servers before
 * Trino 482 fail an equality join on arrays and rows with a null element, because they
 * order the values for the dynamic filter of the join.
 * <li>{@code =}, {@code IS NOT DISTINCT FROM}, and {@code IN} on real values and on values
 * with a time zone. Servers before Trino 484 compare a list of 2 to 7 such values by
 * their raw form, in which equal values can differ by the sign of zero or by the time
 * zone. They do so for an {@code IN} list and for the dynamic filter of an equality join.
 * <li>{@code IN} on uuid and ipaddress values. The connector cannot send their constants
 * in an expression, so the smoke test has no list to compare.
 * <li>Intervals and JSON values. Servers before Trino 484 have other interval types and
 * normalize JSON text.
 * <li>{@code avg} over long decimal values. Servers before Trino 484 return a wrong
 * average when the sum overflows 128 bits.
 * <li>DISTINCT aggregates over different arguments in a grouped aggregation. Servers
 * before Trino 484 can drop the group of NULL keys.
 * <li>{@code FULL JOIN}. Servers before Trino 484 drop the unmatched row of an input with
 * exactly one row when the join condition refers only to the other input.
 * </ul>
 * A check of this list cannot depend on the shape of an expression. The engine also
 * pushes down the arguments of an expression on their own and then the rest of it, which
 * then refers to the arguments as columns.
 */
final class TrinoCompatibilityRegistry
{
    private static final Set<FunctionName> STANDARD_OPERATORS = ImmutableSet.of(
            StandardFunctions.AND_FUNCTION_NAME,
            StandardFunctions.OR_FUNCTION_NAME,
            StandardFunctions.NOT_FUNCTION_NAME,
            StandardFunctions.IS_NULL_FUNCTION_NAME,
            StandardFunctions.NULLIF_FUNCTION_NAME,
            StandardFunctions.CAST_FUNCTION_NAME,
            StandardFunctions.TRY_CAST_FUNCTION_NAME,
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.ADD_FUNCTION_NAME,
            StandardFunctions.SUBTRACT_FUNCTION_NAME,
            StandardFunctions.MULTIPLY_FUNCTION_NAME,
            StandardFunctions.DIVIDE_FUNCTION_NAME,
            StandardFunctions.MODULUS_FUNCTION_NAME,
            StandardFunctions.NEGATE_FUNCTION_NAME,
            StandardFunctions.LIKE_FUNCTION_NAME,
            StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
            StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME);

    private static final Set<String> FUNCTION_ALLOWLIST = ImmutableSet.of(
            "abs",
            "array_distinct",
            "array_join",
            "array_position",
            "at_timezone",
            "cardinality",
            "ceil",
            "ceiling",
            "concat",
            "contains",
            "date",
            "date_add",
            "date_diff",
            "date_trunc",
            "day",
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "element_at",
            "floor",
            "from_iso8601_date",
            "from_iso8601_timestamp",
            "from_unixtime",
            "greatest",
            "hour",
            "json_array_contains",
            "json_extract",
            "json_extract_scalar",
            "json_format",
            "json_parse",
            "least",
            "length",
            "lower",
            "lpad",
            "minute",
            "month",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_like",
            "regexp_replace",
            "replace",
            "round",
            "rpad",
            "second",
            "split",
            "split_part",
            "strpos",
            "substr",
            "substring",
            "timezone_hour",
            "timezone_minute",
            "to_iso8601",
            "to_unixtime",
            "trim",
            "upper",
            "week",
            "with_timezone",
            "year");

    private static final Set<String> SESSION_SENSITIVE_DENYLIST = ImmutableSet.of(
            "current_date",
            "current_time",
            "current_timestamp",
            "current_timezone",
            "localtime",
            "localtimestamp",
            "now");

    private static final Set<FunctionName> ORDERING_OPERATORS = ImmutableSet.of(
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME);

    private static final Set<FunctionName> BASIC_LOGICAL_OPERATORS = ImmutableSet.of(
            StandardFunctions.AND_FUNCTION_NAME,
            StandardFunctions.OR_FUNCTION_NAME,
            StandardFunctions.NOT_FUNCTION_NAME,
            StandardFunctions.IS_NULL_FUNCTION_NAME);

    // An equality join on these operators makes the remote server build a dynamic filter
    private static final Set<FunctionName> BASIC_EQUAL_OPERATORS = ImmutableSet.of(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME);

    // The remote version smoke test compares every operation of the basic list on each of
    // these types with local evaluation, on each remote release that it runs. Its type
    // list is testing/remote-version-smoke/basic-pushdown-types.txt. A type that is not
    // listed stays local.
    static final Set<String> BASIC_SCALAR_TYPES = ImmutableSet.of(
            StandardTypes.BOOLEAN,
            StandardTypes.TINYINT,
            StandardTypes.SMALLINT,
            StandardTypes.INTEGER,
            StandardTypes.BIGINT,
            StandardTypes.REAL,
            StandardTypes.DOUBLE,
            StandardTypes.DECIMAL,
            StandardTypes.VARCHAR,
            StandardTypes.CHAR,
            StandardTypes.VARBINARY,
            StandardTypes.DATE,
            StandardTypes.TIME,
            StandardTypes.TIME_WITH_TIME_ZONE,
            StandardTypes.TIMESTAMP,
            StandardTypes.TIMESTAMP_WITH_TIME_ZONE,
            StandardTypes.UUID,
            StandardTypes.IPADDRESS);

    // A local server with deprecated.legacy-arithmetic-decimal-operators derives another
    // result type for these operators on decimal values
    private static final Set<FunctionName> DECIMAL_ARITHMETIC_OPERATORS = ImmutableSet.of(
            StandardFunctions.ADD_FUNCTION_NAME,
            StandardFunctions.SUBTRACT_FUNCTION_NAME,
            StandardFunctions.MULTIPLY_FUNCTION_NAME,
            StandardFunctions.DIVIDE_FUNCTION_NAME);

    // Operations that read a JSON value as text or by position, or that only pass it on.
    // The remote server sends a date or time inside a JSON value as a JSON string, so the
    // local server cannot repeat an operation that compares or hashes JSON values. Every
    // operation on a JSON value that is not listed here stays local.
    private static final Set<FunctionName> JSON_READING_OPERATORS = ImmutableSet.of(
            StandardFunctions.IS_NULL_FUNCTION_NAME,
            StandardFunctions.CAST_FUNCTION_NAME,
            StandardFunctions.TRY_CAST_FUNCTION_NAME,
            StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME);

    private static final Set<String> JSON_READING_FUNCTIONS = ImmutableSet.of(
            "array_join",
            "cardinality",
            "concat",
            "element_at",
            "json_array_contains",
            "json_extract",
            "json_extract_scalar",
            "json_format");

    private static final Set<String> AGGREGATION_ALLOWLIST = ImmutableSet.of(
            "avg",
            "checksum",
            "count",
            "count_if",
            "max",
            "min",
            "sum");

    boolean isFunctionSupported(ConnectorSession session, Call call, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(session, "session is null");
        requireNonNull(call, "call is null");
        requireNonNull(capabilities, "capabilities is null");

        if (capabilities.evaluatesAsLocal() ? !isFullListCall(call) : !isBasicCall(call)) {
            return false;
        }

        FunctionName functionName = call.getFunctionName();
        if (functionName.equals(StandardFunctions.CAST_FUNCTION_NAME) || functionName.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME)) {
            CastSessionDependency dependency = castSessionDependency(call);
            if (dependency == CastSessionDependency.UNSUPPORTED ||
                    dependency == CastSessionDependency.SESSION_START ||
                    (dependency == CastSessionDependency.TIME_ZONE && !capabilities.hasSameTimeZone(session))) {
                return false;
            }
        }
        if (STANDARD_OPERATORS.contains(functionName)) {
            return true;
        }
        if (functionName.getCatalogSchema().isPresent()) {
            return false;
        }

        String name = canonicalName(functionName);
        if (SESSION_SENSITIVE_DENYLIST.contains(name)) {
            return false;
        }
        if (usesSessionTimeZone(name, call) && !capabilities.hasSameTimeZone(session)) {
            return false;
        }
        if (isSubscriptFunction(name)) {
            return true;
        }
        return FUNCTION_ALLOWLIST.contains(name) && capabilities.hasFunction(name);
    }

    boolean isAggregationSupported(AggregateFunction aggregate, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(aggregate, "aggregate is null");
        requireNonNull(capabilities, "capabilities is null");
        String name = aggregate.getFunctionName().toLowerCase(Locale.ENGLISH);
        return AGGREGATION_ALLOWLIST.contains(name) &&
                capabilities.hasFunction(name) &&
                isAggregateDelegable(aggregate, capabilities);
    }

    /**
     * Whether the remote server may evaluate the expression, including all nested calls.
     * The baseline JDBC rewriter must pass this check too.
     */
    static boolean isExpressionDelegable(ConnectorExpression expression, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() ? isFullListExpression(expression) : isBasicExpression(expression);
    }

    /**
     * Whether the remote server may evaluate the aggregate function.
     */
    static boolean isAggregateDelegable(AggregateFunction aggregate, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(aggregate, "aggregate is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() ? isFullListAggregate(aggregate) : isBasicAggregate(aggregate);
    }

    /**
     * Whether the remote server may evaluate the whole aggregation, in addition to the
     * check of each aggregate function.
     */
    static boolean isAggregationDelegable(List<AggregateFunction> aggregates, List<List<Type>> groupingSetTypes, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(aggregates, "aggregates is null");
        requireNonNull(groupingSetTypes, "groupingSetTypes is null");
        requireNonNull(capabilities, "capabilities is null");
        if (capabilities.evaluatesAsLocal()) {
            // GROUP BY compares the values of a grouping column
            return groupingSetTypes.stream().allMatch(types -> types.stream().noneMatch(TrinoCompatibilityRegistry::containsJsonType));
        }
        if (!groupingSetTypes.stream().allMatch(types -> types.stream().allMatch(TrinoCompatibilityRegistry::isBasicEqualityType))) {
            return false;
        }
        Set<List<ConnectorExpression>> distinctArguments = new HashSet<>();
        for (AggregateFunction aggregate : aggregates) {
            if (aggregate.isDistinct()) {
                distinctArguments.add(aggregate.getArguments());
            }
        }
        boolean grouped = groupingSetTypes.stream().anyMatch(types -> !types.isEmpty());
        return !grouped || distinctArguments.size() <= 1;
    }

    /**
     * Whether the remote server may tell values of the type apart, as {@code <>},
     * {@code GROUP BY}, and DISTINCT do.
     */
    static boolean isEqualityDelegable(Type type, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(type, "type is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() ? !containsJsonType(type) : isBasicEqualityType(type);
    }

    /**
     * Whether the remote server may evaluate {@code =} and {@code IS NOT DISTINCT FROM}
     * on values of the type, also as the condition of a join.
     */
    static boolean isEqualDelegable(Type type, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(type, "type is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() ? !containsJsonType(type) : isBasicEqualType(type);
    }

    /**
     * Whether the remote server may order values of the type, as ordering comparisons,
     * {@code min}, {@code max}, and {@code ORDER BY} do.
     */
    static boolean isOrderingDelegable(Type type, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(type, "type is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() || isBasicOrderingType(type);
    }

    /**
     * Whether the remote server may evaluate a join of the type. The checks of the join
     * condition apply in addition.
     */
    static boolean isJoinDelegable(JoinType joinType, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(joinType, "joinType is null");
        requireNonNull(capabilities, "capabilities is null");
        return capabilities.evaluatesAsLocal() || joinType != JoinType.FULL_OUTER;
    }

    static boolean isFullListExpression(ConnectorExpression expression)
    {
        requireNonNull(expression, "expression is null");
        if (expression instanceof Call call && !isFullListCall(call)) {
            return false;
        }
        return expression.getChildren().stream().allMatch(TrinoCompatibilityRegistry::isFullListExpression);
    }

    // This call alone, not the nested calls in its arguments
    private static boolean isFullListCall(Call call)
    {
        return hasDefaultDecimalType(call) && !comparesJsonValues(call);
    }

    static boolean isFullListAggregate(AggregateFunction aggregate)
    {
        requireNonNull(aggregate, "aggregate is null");
        List<ConnectorExpression> arguments = aggregate.getArguments();
        if (!arguments.stream().allMatch(TrinoCompatibilityRegistry::isFullListExpression) ||
                !aggregate.getFilter().map(TrinoCompatibilityRegistry::isFullListExpression).orElse(true)) {
            return false;
        }
        // count without DISTINCT only checks a JSON value for NULL. DISTINCT and checksum
        // compare or hash it.
        boolean readsJsonOnly = aggregate.getFunctionName().equalsIgnoreCase("count") && !aggregate.isDistinct();
        return readsJsonOnly || arguments.stream().noneMatch(argument -> containsJsonType(argument.getType()));
    }

    /**
     * Whether the local server derived the result type of a decimal {@code +}, {@code -},
     * {@code *}, or {@code /} with the default rules of Trino 484, which the remote server
     * uses.
     * With another result type the remote server would return another value.
     */
    private static boolean hasDefaultDecimalType(Call call)
    {
        FunctionName functionName = call.getFunctionName();
        if (!DECIMAL_ARITHMETIC_OPERATORS.contains(functionName) ||
                call.getArguments().size() != 2 ||
                !(call.getType() instanceof DecimalType result) ||
                !(call.getArguments().get(0).getType() instanceof DecimalType left) ||
                !(call.getArguments().get(1).getType() instanceof DecimalType right)) {
            return true;
        }
        int precision;
        int scale;
        if (functionName.equals(StandardFunctions.ADD_FUNCTION_NAME) || functionName.equals(StandardFunctions.SUBTRACT_FUNCTION_NAME)) {
            int integral = max(left.getPrecision() - left.getScale(), right.getPrecision() - right.getScale());
            int rawScale = max(left.getScale(), right.getScale());
            precision = min(rawScale + integral + 1, 38);
            scale = min(rawScale, precision - integral);
        }
        else {
            boolean multiply = functionName.equals(StandardFunctions.MULTIPLY_FUNCTION_NAME);
            int rawScale = multiply ? left.getScale() + right.getScale() : max(6, left.getScale() + right.getPrecision() + 1);
            int rawPrecision = multiply ? left.getPrecision() + right.getPrecision() : left.getPrecision() - left.getScale() + right.getScale() + rawScale;
            int integral = rawPrecision - rawScale;
            precision = min(rawPrecision, 38);
            scale = min(rawScale, integral > 32 ? 6 : 38 - integral);
        }
        return result.getPrecision() == precision && result.getScale() == scale;
    }

    private static boolean comparesJsonValues(Call call)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.stream().noneMatch(argument -> containsJsonType(argument.getType()))) {
            return false;
        }
        FunctionName functionName = call.getFunctionName();
        if (JSON_READING_OPERATORS.contains(functionName)) {
            return false;
        }
        if (functionName.getCatalogSchema().isPresent()) {
            return true;
        }
        String name = canonicalName(functionName);
        if (!JSON_READING_FUNCTIONS.contains(name) && !isSubscriptFunction(name)) {
            return true;
        }
        // A map with JSON keys finds an entry by comparing the keys
        return arguments.stream().anyMatch(argument -> argument.getType() instanceof MapType mapType && containsJsonType(mapType.getKeyType()));
    }

    static boolean containsJsonType(Type type)
    {
        requireNonNull(type, "type is null");
        if (type instanceof ArrayType arrayType) {
            return containsJsonType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return containsJsonType(mapType.getKeyType()) || containsJsonType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(TrinoCompatibilityRegistry::containsJsonType);
        }
        return TrinoTypeClassifier.isJsonType(type);
    }

    static boolean isBasicExpression(ConnectorExpression expression)
    {
        requireNonNull(expression, "expression is null");
        if (expression instanceof Variable || expression instanceof Constant) {
            return isBasicEqualityType(expression.getType());
        }
        if (expression instanceof FieldDereference dereference) {
            return isBasicEqualityType(dereference.getType()) && isBasicExpression(dereference.getTarget());
        }
        if (expression instanceof Call call) {
            return isBasicCall(call) && call.getArguments().stream().allMatch(TrinoCompatibilityRegistry::isBasicExpression);
        }
        return false;
    }

    // This call alone, not the nested calls in its arguments
    private static boolean isBasicCall(Call call)
    {
        FunctionName functionName = call.getFunctionName();
        List<ConnectorExpression> arguments = call.getArguments();
        if (BASIC_LOGICAL_OPERATORS.contains(functionName)) {
            return true;
        }
        if (BASIC_EQUAL_OPERATORS.contains(functionName)) {
            return arguments.stream().allMatch(argument -> isBasicEqualType(argument.getType()));
        }
        if (functionName.equals(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME) || functionName.equals(StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return arguments.stream().allMatch(argument -> isBasicEqualityType(argument.getType()));
        }
        if (ORDERING_OPERATORS.contains(functionName)) {
            return arguments.stream().allMatch(argument -> isBasicOrderingType(argument.getType()));
        }
        if (functionName.equals(StandardFunctions.IN_PREDICATE_FUNCTION_NAME)) {
            return !arguments.isEmpty() && isBasicInType(arguments.getFirst().getType());
        }
        return false;
    }

    static boolean isBasicAggregate(AggregateFunction aggregate)
    {
        requireNonNull(aggregate, "aggregate is null");
        List<ConnectorExpression> arguments = aggregate.getArguments();
        if (!aggregate.getSortItems().isEmpty() ||
                !isBasicEqualityType(aggregate.getOutputType()) ||
                !arguments.stream().allMatch(TrinoCompatibilityRegistry::isBasicExpression) ||
                !aggregate.getFilter().map(TrinoCompatibilityRegistry::isBasicExpression).orElse(true)) {
            return false;
        }
        return switch (aggregate.getFunctionName().toLowerCase(Locale.ENGLISH)) {
            case "count" -> arguments.size() <= 1;
            case "count_if", "checksum" -> arguments.size() == 1;
            case "min", "max" -> arguments.size() == 1 && isBasicOrderingType(arguments.getFirst().getType());
            case "sum" -> arguments.size() == 1 && isBasicSumType(arguments.getFirst().getType());
            case "avg" -> arguments.size() == 1 && isBasicAverageType(arguments.getFirst().getType());
            default -> false;
        };
    }

    static boolean isBasicEqualityType(Type type)
    {
        requireNonNull(type, "type is null");
        if (type instanceof ArrayType arrayType) {
            return isBasicEqualityType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return isBasicEqualityType(mapType.getKeyType()) && isBasicEqualityType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .allMatch(TrinoCompatibilityRegistry::isBasicEqualityType);
        }
        return isBasicScalarType(type);
    }

    static boolean isBasicOrderingType(Type type)
    {
        requireNonNull(type, "type is null");
        return isBasicScalarType(type);
    }

    /**
     * Whether {@code =} and {@code IS NOT DISTINCT FROM} on values of the type are in the
     * basic list. The connector cannot tell the condition of a join from a filter, and an
     * equality join makes the remote server build a dynamic filter from the values.
     * Servers before Trino 484 compare a list of 2 to 7 real values or values with a time
     * zone by their raw form, so a join on a real column misses the rows whose zero has
     * the other sign. Servers before Trino 482 order the values, which fails for arrays
     * and rows with a null element. The memory connector of the smoke test cannot apply
     * the filter on a map column, so the smoke test has no join on maps to compare.
     */
    static boolean isBasicEqualType(Type type)
    {
        requireNonNull(type, "type is null");
        return isBasicScalarType(type) &&
                !(type instanceof RealType) &&
                !(type instanceof TimeWithTimeZoneType) &&
                !(type instanceof TimestampWithTimeZoneType);
    }

    static boolean isBasicInType(Type type)
    {
        requireNonNull(type, "type is null");
        // The connector cannot send uuid and ipaddress constants in an expression, so the
        // smoke test has no IN list for them
        return isBasicEqualType(type) &&
                !(type instanceof UuidType) &&
                !TrinoTypeClassifier.isIpAddressType(type);
    }

    static boolean isBasicSumType(Type type)
    {
        requireNonNull(type, "type is null");
        return isBasicAverageType(type) || type instanceof DecimalType;
    }

    static boolean isBasicAverageType(Type type)
    {
        requireNonNull(type, "type is null");
        // The engine casts smaller integer types to bigint for sum and avg, and casts are
        // not in the basic list
        return type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                (type instanceof DecimalType decimalType && decimalType.isShort());
    }

    private static boolean isBasicScalarType(Type type)
    {
        return BASIC_SCALAR_TYPES.contains(type.getBaseName());
    }

    static String canonicalName(FunctionName functionName)
    {
        return functionName.getName().toLowerCase(Locale.ENGLISH);
    }

    static boolean isSubscriptFunction(String name)
    {
        return name.equals("subscript") || name.equals("$operator$subscript");
    }

    private static CastSessionDependency castSessionDependency(Call call)
    {
        if (call.getArguments().size() != 1) {
            return CastSessionDependency.SESSION_START;
        }
        ConnectorExpression source = call.getArguments().getFirst();
        return castSessionDependency(source.getType(), call.getType());
    }

    private static CastSessionDependency castSessionDependency(Type sourceType, Type targetType)
    {
        if (sourceType instanceof ArrayType sourceArray && targetType instanceof ArrayType targetArray) {
            return castSessionDependency(sourceArray.getElementType(), targetArray.getElementType());
        }
        if (sourceType instanceof MapType sourceMap && targetType instanceof MapType targetMap) {
            return CastSessionDependency.combine(
                    castSessionDependency(sourceMap.getKeyType(), targetMap.getKeyType()),
                    castSessionDependency(sourceMap.getValueType(), targetMap.getValueType()));
        }
        if (sourceType instanceof RowType sourceRow && targetType instanceof RowType targetRow) {
            if (sourceRow.getFields().size() != targetRow.getFields().size()) {
                return CastSessionDependency.UNSUPPORTED;
            }
            CastSessionDependency dependency = CastSessionDependency.NONE;
            for (int index = 0; index < sourceRow.getFields().size(); index++) {
                dependency = CastSessionDependency.combine(
                        dependency,
                        castSessionDependency(
                                sourceRow.getFields().get(index).getType(),
                                targetRow.getFields().get(index).getType()));
            }
            return dependency;
        }

        // These casts use the session start instant or its local date. Local and remote
        // queries do not share a start instant, so matching time zones are insufficient.
        if ((sourceType instanceof TimeType &&
                (targetType instanceof TimeWithTimeZoneType || targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType)) ||
                (sourceType instanceof TimeWithTimeZoneType &&
                        (targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType)) ||
                (sourceType instanceof TimestampType && targetType instanceof TimeWithTimeZoneType) ||
                ((sourceType instanceof CharType || sourceType instanceof VarcharType) && targetType instanceof TimeWithTimeZoneType)) {
            return CastSessionDependency.SESSION_START;
        }

        // Casts that attach a time zone are safe only when the connector and remote
        // sessions use the same zone.
        if (((sourceType instanceof DateType || sourceType instanceof TimestampType || sourceType instanceof CharType || sourceType instanceof VarcharType) &&
                targetType instanceof TimestampWithTimeZoneType)) {
            return CastSessionDependency.TIME_ZONE;
        }

        // Fail closed for temporal-to-temporal casts. Every valid pair is either
        // classified above or explicitly known to be independent of session state.
        if (isTemporalType(sourceType) && isTemporalType(targetType) && !isSessionIndependentTemporalCast(sourceType, targetType)) {
            return CastSessionDependency.UNSUPPORTED;
        }
        return CastSessionDependency.NONE;
    }

    private static boolean isTemporalType(Type type)
    {
        return type instanceof DateType ||
                type instanceof TimeType ||
                type instanceof TimeWithTimeZoneType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType;
    }

    private static boolean isSessionIndependentTemporalCast(Type sourceType, Type targetType)
    {
        return (sourceType instanceof DateType && (targetType instanceof DateType || targetType instanceof TimestampType)) ||
                (sourceType instanceof TimeType && targetType instanceof TimeType) ||
                (sourceType instanceof TimeWithTimeZoneType &&
                        (targetType instanceof TimeType || targetType instanceof TimeWithTimeZoneType)) ||
                (sourceType instanceof TimestampType &&
                        (targetType instanceof DateType || targetType instanceof TimeType || targetType instanceof TimestampType)) ||
                (sourceType instanceof TimestampWithTimeZoneType &&
                        (targetType instanceof DateType || targetType instanceof TimeType || targetType instanceof TimeWithTimeZoneType ||
                                targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType));
    }

    private enum CastSessionDependency
    {
        NONE,
        TIME_ZONE,
        SESSION_START,
        UNSUPPORTED;

        private static CastSessionDependency combine(CastSessionDependency first, CastSessionDependency second)
        {
            return first.ordinal() >= second.ordinal() ? first : second;
        }
    }

    private static boolean usesSessionTimeZone(String name, Call call)
    {
        return (name.equals("from_unixtime") && call.getArguments().size() == 1) ||
                name.equals("from_iso8601_timestamp");
    }
}
