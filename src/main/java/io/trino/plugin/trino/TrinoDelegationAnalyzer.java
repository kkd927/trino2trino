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

import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class TrinoDelegationAnalyzer
{
    enum Decision
    {
        REMOTE_DELEGATE,
        LOCAL_FALLBACK,
        UNSUPPORTED
    }

    record ExpressionAnalysis(Decision decision, Optional<ParameterizedExpression> expression, Optional<String> reason) {}

    record ProjectionAnalysis(Decision decision, Optional<JdbcExpression> expression, Optional<String> reason) {}

    private final TrinoRemoteSqlRenderer renderer;

    TrinoDelegationAnalyzer(TrinoRemoteSqlRenderer renderer)
    {
        this.renderer = requireNonNull(renderer, "renderer is null");
    }

    ExpressionAnalysis analyzePredicate(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return new ExpressionAnalysis(Decision.LOCAL_FALLBACK, Optional.empty(), Optional.of("remote delegation is disabled"));
        }
        Optional<ParameterizedExpression> rendered = renderer.renderExpression(session, expression, assignments, capabilities);
        if (rendered.isPresent()) {
            return new ExpressionAnalysis(Decision.REMOTE_DELEGATE, rendered, Optional.empty());
        }
        return unsupportedOrFallback(session, "Predicate cannot be rendered for remote Trino delegation: " + expression);
    }

    ProjectionAnalysis analyzeProjection(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return new ProjectionAnalysis(Decision.LOCAL_FALLBACK, Optional.empty(), Optional.of("remote delegation is disabled"));
        }
        Optional<JdbcExpression> rendered = renderer.renderProjection(session, expression, assignments, capabilities);
        if (rendered.isPresent()) {
            return new ProjectionAnalysis(Decision.REMOTE_DELEGATE, rendered, Optional.empty());
        }
        ExpressionAnalysis fallback = unsupportedOrFallback(session, "Projection cannot be rendered for remote Trino delegation: " + expression);
        return new ProjectionAnalysis(fallback.decision(), Optional.empty(), fallback.reason());
    }

    ProjectionAnalysis analyzeAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(capabilities, "capabilities is null");
        if (!isEnabled(session)) {
            return new ProjectionAnalysis(Decision.LOCAL_FALLBACK, Optional.empty(), Optional.of("remote delegation is disabled"));
        }
        Optional<JdbcExpression> rendered = renderer.renderAggregation(session, aggregate, assignments, capabilities);
        if (rendered.isPresent()) {
            return new ProjectionAnalysis(Decision.REMOTE_DELEGATE, rendered, Optional.empty());
        }
        ExpressionAnalysis fallback = unsupportedOrFallback(session, "Aggregation cannot be rendered for remote Trino delegation: " + aggregate);
        return new ProjectionAnalysis(fallback.decision(), Optional.empty(), fallback.reason());
    }

    private ExpressionAnalysis unsupportedOrFallback(ConnectorSession session, String reason)
    {
        if (mode(session) == TrinoRemoteDelegationMode.STRICT) {
            return new ExpressionAnalysis(Decision.UNSUPPORTED, Optional.empty(), Optional.of(reason));
        }
        return new ExpressionAnalysis(Decision.LOCAL_FALLBACK, Optional.empty(), Optional.of(reason));
    }

    private static boolean isEnabled(ConnectorSession session)
    {
        return TrinoRemoteDelegationSessionProperties.isRemoteDelegationEnabled(session) &&
                mode(session) != TrinoRemoteDelegationMode.OFF;
    }

    private static TrinoRemoteDelegationMode mode(ConnectorSession session)
    {
        return TrinoRemoteDelegationSessionProperties.getRemoteDelegationMode(session);
    }
}
