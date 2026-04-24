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

import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoRemoteSqlRenderer
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new TrinoRemoteDelegationSessionProperties(new TrinoRemoteDelegationConfig()).getSessionProperties())
            .setPropertyValues(Map.of(
                    TrinoRemoteDelegationSessionProperties.REMOTE_DELEGATION_ENABLED, true,
                    TrinoRemoteDelegationSessionProperties.REMOTE_DELEGATION_MODE, TrinoRemoteDelegationMode.AUTO.name()))
            .build();

    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT,
            Optional.of("bigint"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private static final JdbcTypeHandle VARCHAR_TYPE_HANDLE = new JdbcTypeHandle(
            Types.VARCHAR,
            Optional.of("varchar"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final TrinoRemoteSqlRenderer renderer = new TrinoRemoteSqlRenderer(name -> "\"" + name + "\"", new TrinoCompatibilityRegistry());
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of(
            "date_trunc",
            "from_iso8601_timestamp",
            "regexp_like"));

    @Test
    void testVarcharToTimestampCastComparison()
    {
        ConnectorExpression cast = new Call(
                TIMESTAMP_MILLIS,
                StandardFunctions.CAST_FUNCTION_NAME,
                List.of(new Variable("log_timestamp", VARCHAR)));

        ParameterizedExpression rewritten = render(
                new Call(
                        BOOLEAN,
                        StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(cast, new Constant(0L, TIMESTAMP_MILLIS))),
                Map.of("log_timestamp", column("log_timestamp", VARCHAR_TYPE_HANDLE, VARCHAR)));

        assertThat(rewritten.expression()).isEqualTo("(CAST(\"log_timestamp\" AS timestamp(3)) >= ?)");
        assertThat(rewritten.parameters()).hasSize(1);
        assertThat(rewritten.parameters().getFirst().getType()).isEqualTo(TIMESTAMP_MILLIS);
        assertThat(rewritten.parameters().getFirst().getValue()).contains(0L);
    }

    @Test
    void testDateTruncRegexpAndIsoTimestampFunctions()
    {
        ConnectorExpression expression = new Call(
                BOOLEAN,
                StandardFunctions.AND_FUNCTION_NAME,
                List.of(
                        new Call(
                                BOOLEAN,
                                new FunctionName("regexp_like"),
                                List.of(new Variable("path", VARCHAR), varcharConstant("^/post/"))),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(
                                        new Call(
                                                TIMESTAMP_MILLIS,
                                                new FunctionName("date_trunc"),
                                                List.of(varcharConstant("day"), new Call(
                                                        TIMESTAMP_MILLIS,
                                                        StandardFunctions.CAST_FUNCTION_NAME,
                                                        List.of(new Variable("log_timestamp", VARCHAR))))),
                                        new Call(
                                                TIMESTAMP_MILLIS,
                                                StandardFunctions.CAST_FUNCTION_NAME,
                                                List.of(varcharConstant("2024-01-15 00:00:00")))))));

        ParameterizedExpression rewritten = render(
                expression,
                Map.of(
                        "path", column("path", VARCHAR_TYPE_HANDLE, VARCHAR),
                        "log_timestamp", column("log_timestamp", VARCHAR_TYPE_HANDLE, VARCHAR)));

        assertThat(rewritten.expression()).isEqualTo("(regexp_like(\"path\", ?) AND (date_trunc(?, CAST(\"log_timestamp\" AS timestamp(3))) = CAST(? AS timestamp(3))))");
        assertThat(rewritten.parameters()).hasSize(3);
    }

    @Test
    void testLikeInAndSubscript()
    {
        ConnectorExpression inPredicate = new Call(
                BOOLEAN,
                StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
                List.of(new Variable("regionkey", BIGINT), new Constant(1L, BIGINT), new Constant(2L, BIGINT)));
        ConnectorExpression likePredicate = new Call(
                BOOLEAN,
                StandardFunctions.LIKE_FUNCTION_NAME,
                List.of(new Variable("name", VARCHAR), varcharConstant("A%")));
        ConnectorExpression subscript = new Call(
                VARCHAR,
                new FunctionName("subscript"),
                List.of(new Variable("tags", new ArrayType(VARCHAR)), new Constant(1L, BIGINT)));

        assertThat(render(inPredicate, Map.of("regionkey", column("regionkey"))).expression())
                .isEqualTo("(\"regionkey\" IN (?, ?))");
        assertThat(render(likePredicate, Map.of("name", column("name", VARCHAR_TYPE_HANDLE, VARCHAR))).expression())
                .isEqualTo("(\"name\" LIKE ?)");
        assertThat(render(subscript, Map.of("tags", column("tags", VARCHAR_TYPE_HANDLE, new ArrayType(VARCHAR)))).expression())
                .isEqualTo("\"tags\"[?]");
    }

    @Test
    void testFieldDereference()
    {
        RowType rowType = RowType.from(List.of(RowType.field("name", VARCHAR), RowType.field("age", BIGINT)));
        ParameterizedExpression rewritten = render(
                new FieldDereference(VARCHAR, new Variable("person", rowType), 0),
                Map.of("person", column("person", VARCHAR_TYPE_HANDLE, rowType)));

        assertThat(rewritten.expression()).isEqualTo("\"person\".\"name\"");
        assertThat(rewritten.parameters()).isEmpty();
    }

    private ParameterizedExpression render(ConnectorExpression expression, Map<String, ? extends ColumnHandle> assignments)
    {
        return renderer.renderExpression(SESSION, expression, Map.copyOf(assignments), capabilities)
                .orElseThrow(() -> new AssertionError("Expected expression to be rendered: " + expression));
    }

    private static ConnectorExpression varcharConstant(String value)
    {
        return new Constant(Slices.utf8Slice(value), VARCHAR);
    }

    private static JdbcColumnHandle column(String name)
    {
        return new JdbcColumnHandle(name, BIGINT_TYPE_HANDLE, BIGINT);
    }

    private static JdbcColumnHandle column(String name, JdbcTypeHandle typeHandle, io.trino.spi.type.Type type)
    {
        return new JdbcColumnHandle(name, typeHandle, type);
    }
}
