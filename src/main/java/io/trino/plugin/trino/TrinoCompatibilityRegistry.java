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

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

final class TrinoCompatibilityRegistry
{
    private static final Set<FunctionName> STANDARD_OPERATORS = Set.of(
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

    private static final Set<String> FUNCTION_ALLOWLIST = Set.of(
            "abs",
            "array_distinct",
            "array_join",
            "array_position",
            "at_timezone",
            "cardinality",
            "ceil",
            "ceiling",
            "coalesce",
            "concat",
            "contains",
            "date",
            "date_add",
            "date_diff",
            "date_format",
            "date_parse",
            "date_trunc",
            "day",
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "element_at",
            "floor",
            "format_datetime",
            "from_iso8601_date",
            "from_iso8601_timestamp",
            "from_unixtime",
            "greatest",
            "hour",
            "if",
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
            "try",
            "upper",
            "week",
            "with_timezone",
            "year");

    private static final Set<String> SESSION_SENSITIVE_DENYLIST = Set.of(
            "current_date",
            "current_time",
            "current_timestamp",
            "current_timezone",
            "localtime",
            "localtimestamp",
            "now");

    private static final Set<String> AGGREGATION_ALLOWLIST = Set.of(
            "avg",
            "checksum",
            "count",
            "count_if",
            "max",
            "min",
            "sum");

    private static final Pattern LEADING_VERSION_NUMBER = Pattern.compile("^(\\d+)");
    private static final Map<String, Integer> MINIMUM_FUNCTION_VERSION = Map.of();

    boolean isFunctionSupported(ConnectorSession session, Call call, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(session, "session is null");
        requireNonNull(call, "call is null");
        requireNonNull(capabilities, "capabilities is null");

        FunctionName functionName = call.getFunctionName();
        if (!isTypeSupported(call.getType())) {
            return false;
        }
        if ((functionName.equals(StandardFunctions.CAST_FUNCTION_NAME) || functionName.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME)) &&
                isSessionSensitiveCast(call) &&
                !capabilities.hasSameTimeZone(session)) {
            return false;
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
        if (name.equals("from_unixtime") && call.getArguments().size() == 1 && !capabilities.hasSameTimeZone(session)) {
            return false;
        }
        if (isSubscriptFunction(name)) {
            return true;
        }
        return FUNCTION_ALLOWLIST.contains(name) &&
                isVersionAtLeast(capabilities.version(), MINIMUM_FUNCTION_VERSION.getOrDefault(name, 0)) &&
                capabilities.hasFunction(name);
    }

    boolean isAggregationSupported(AggregateFunction aggregate, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(aggregate, "aggregate is null");
        requireNonNull(capabilities, "capabilities is null");
        if (!isTypeSupported(aggregate.getOutputType())) {
            return false;
        }
        String name = aggregate.getFunctionName().toLowerCase(Locale.ENGLISH);
        return AGGREGATION_ALLOWLIST.contains(name) &&
                isVersionAtLeast(capabilities.version(), MINIMUM_FUNCTION_VERSION.getOrDefault(name, 0)) &&
                capabilities.hasFunction(name);
    }

    boolean isVersionAtLeast(Optional<String> remoteVersion, int minimumMajorVersion)
    {
        requireNonNull(remoteVersion, "remoteVersion is null");
        if (minimumMajorVersion <= 0) {
            return true;
        }
        if (remoteVersion.isEmpty()) {
            return true;
        }
        Matcher matcher = LEADING_VERSION_NUMBER.matcher(remoteVersion.orElseThrow());
        if (!matcher.find()) {
            return false;
        }
        return Integer.parseInt(matcher.group(1)) >= minimumMajorVersion;
    }

    private boolean isTypeSupported(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return isTypeSupported(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return isTypeSupported(mapType.getKeyType()) && isTypeSupported(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .allMatch(this::isTypeSupported);
        }
        return true;
    }

    static String canonicalName(FunctionName functionName)
    {
        return functionName.getName().toLowerCase(Locale.ENGLISH);
    }

    static boolean isSubscriptFunction(String name)
    {
        return name.equals("subscript") || name.equals("$operator$subscript");
    }

    private static boolean isSessionSensitiveCast(Call call)
    {
        if (call.getArguments().size() != 1) {
            return true;
        }
        ConnectorExpression source = call.getArguments().getFirst();
        Type sourceType = source.getType();
        Type targetType = call.getType();
        if (source instanceof Call sourceCall && hasExplicitTimeZone(sourceCall)) {
            return false;
        }
        return sourceType instanceof TimestampWithTimeZoneType &&
                (targetType instanceof DateType || targetType instanceof TimeType || targetType instanceof TimestampType);
    }

    private static boolean hasExplicitTimeZone(Call call)
    {
        String name = canonicalName(call.getFunctionName());
        return (name.equals("at_timezone") || name.equals("with_timezone")) &&
                call.getArguments().size() >= 2 &&
                call.getArguments().get(1) instanceof Constant;
    }
}
