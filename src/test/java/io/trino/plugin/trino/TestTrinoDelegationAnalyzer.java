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

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.trino.TrinoDelegationAnalyzer.Decision.LOCAL_FALLBACK;
import static io.trino.plugin.trino.TrinoDelegationAnalyzer.Decision.REMOTE_DELEGATE;
import static io.trino.plugin.trino.TrinoDelegationAnalyzer.Decision.UNSUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.TestingConnectorSession.builder;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoDelegationAnalyzer
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT,
            Optional.of("bigint"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final TrinoDelegationAnalyzer analyzer = new TrinoDelegationAnalyzer(
            new TrinoRemoteSqlRenderer(name -> "\"" + name + "\"", new TrinoCompatibilityRegistry()));
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of());

    @Test
    void testRemoteDelegate()
    {
        TrinoDelegationAnalyzer.ExpressionAnalysis analysis = analyzer.analyzePredicate(
                session(TrinoRemoteDelegationMode.AUTO),
                new Call(
                        BOOLEAN,
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(new Variable("nationkey", BIGINT), new Variable("regionkey", BIGINT))),
                assignments(),
                capabilities);

        assertThat(analysis.decision()).isEqualTo(REMOTE_DELEGATE);
        assertThat(analysis.expression()).isPresent();
    }

    @Test
    void testAutoFallbackForUnsupportedFunction()
    {
        TrinoDelegationAnalyzer.ExpressionAnalysis analysis = analyzer.analyzePredicate(
                session(TrinoRemoteDelegationMode.AUTO),
                new Call(BOOLEAN, new FunctionName("remote_only_custom_function"), List.of(new Variable("nationkey", BIGINT))),
                assignments(),
                capabilities);

        assertThat(analysis.decision()).isEqualTo(LOCAL_FALLBACK);
        assertThat(analysis.expression()).isEmpty();
    }

    @Test
    void testStrictClassifiesUnsupportedFunction()
    {
        TrinoDelegationAnalyzer.ExpressionAnalysis analysis = analyzer.analyzePredicate(
                session(TrinoRemoteDelegationMode.STRICT),
                new Call(BOOLEAN, new FunctionName("remote_only_custom_function"), List.of(new Variable("nationkey", BIGINT))),
                assignments(),
                capabilities);

        assertThat(analysis.decision()).isEqualTo(UNSUPPORTED);
        assertThat(analysis.reason()).hasValueSatisfying(reason -> assertThat(reason).contains("Predicate cannot be rendered"));
    }

    private static ConnectorSession session(TrinoRemoteDelegationMode mode)
    {
        return builder()
                .setPropertyMetadata(new TrinoRemoteDelegationSessionProperties(new TrinoRemoteDelegationConfig()).getSessionProperties())
                .setPropertyValues(Map.of(
                        TrinoRemoteDelegationSessionProperties.REMOTE_DELEGATION_ENABLED, true,
                        TrinoRemoteDelegationSessionProperties.REMOTE_DELEGATION_MODE, mode.name()))
                .build();
    }

    private static Map<String, ColumnHandle> assignments()
    {
        return Map.of(
                "nationkey", new JdbcColumnHandle("nationkey", BIGINT_TYPE_HANDLE, BIGINT),
                "regionkey", new JdbcColumnHandle("regionkey", BIGINT_TYPE_HANDLE, BIGINT));
    }
}
