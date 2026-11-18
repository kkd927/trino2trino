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

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.spi.predicate.ValueSet;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.util.Objects.requireNonNull;

/**
 * Keeps tuple-domain filters correct on {@code real}, {@code double}, and {@code number}
 * columns, whose types have NaN.
 * <p>
 * The base JDBC query builder renders a set of ranges whose complement is a list of
 * values, such as {@code x < 1 OR x > 1}, as {@code NOT (x = ?)} or
 * {@code NOT (x IN (...))}. The ranges of a domain do not include NaN, but NaN passes that
 * predicate, so the remote server returned the rows with NaN. Servers before Trino 482
 * also compare {@code number} values by their bytes in that predicate. The engine keeps
 * {@code <>} and {@code NOT IN} on these types out of a domain for the same reason.
 * <p>
 * The controller keeps such a filter local. It selects all values but a few, so the
 * remote server would return most rows with the filter too. The decision depends only on
 * the domain values, so the coordinator and the workers render the same SQL.
 */
final class ComplementFilterPushdown
{
    private ComplementFilterPushdown() {}

    /**
     * Returns {@code mapping} with a predicate pushdown controller that keeps the filters
     * described above local and applies the mapping's controller to every other filter, or
     * {@code mapping} itself when its type has no NaN.
     */
    static ColumnMapping wrap(ColumnMapping mapping)
    {
        requireNonNull(mapping, "mapping is null");
        if (!typeHasNaN(mapping.getType())) {
            return mapping;
        }
        PredicatePushdownController delegate = mapping.getPredicatePushdownController();
        return ColumnMapping.mapping(
                mapping.getType(),
                mapping.getReadFunction(),
                mapping.getWriteFunction(),
                (session, domain) -> isComplementOfValues(domain.getValues()) ?
                        DISABLE_PUSHDOWN.apply(session, domain) :
                        delegate.apply(session, domain));
    }

    private static boolean isComplementOfValues(ValueSet values)
    {
        return !values.isNone() && !values.isAll() && !values.isDiscreteSet() && values.complement().isDiscreteSet();
    }
}
