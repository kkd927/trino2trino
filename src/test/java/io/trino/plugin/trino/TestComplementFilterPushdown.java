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
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestComplementFilterPushdown
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
            .build();

    @Test
    void testValuesOtherThanAListStayLocal()
    {
        // The query builder renders x < 1 OR x > 1 as NOT (x = ?), which NaN passes
        Domain otherThanOne = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0), Range.greaterThan(DOUBLE, 1.0)), false);
        assertLocal(doubleColumnMapping(), otherThanOne);
        // (x < 1 OR x > 1) OR x IS NULL
        assertLocal(doubleColumnMapping(), Domain.create(otherThanOne.getValues(), true));
        // The query builder renders the values other than 1 and 5 as NOT (x IN (?, ?))
        assertLocal(doubleColumnMapping(), Domain.create(ValueSet.of(DOUBLE, 1.0, 5.0).complement(), false));

        long one = floatToRawIntBits(1.0f);
        assertLocal(realColumnMapping(), Domain.create(ValueSet.ofRanges(Range.lessThan(REAL, one), Range.greaterThan(REAL, one)), false));

        TrinoNumber five = TrinoNumber.from(new BigDecimal("5"));
        assertLocal(TrinoNumberCodec.numberColumnMapping(), Domain.create(ValueSet.ofRanges(Range.lessThan(NUMBER, five), Range.greaterThan(NUMBER, five)), false));
    }

    @Test
    void testOtherFiltersFollowTheMapping()
    {
        ColumnMapping mapping = ComplementFilterPushdown.wrap(doubleColumnMapping());
        List<Domain> domains = List.of(
                Domain.singleValue(DOUBLE, 1.0),
                Domain.multipleValues(DOUBLE, List.of(1.0, 5.0)),
                Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false),
                // The complement is a range, which the query builder renders as two comparisons
                Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0), Range.greaterThan(DOUBLE, 2.0)), false),
                Domain.all(DOUBLE),
                Domain.notNull(DOUBLE),
                Domain.onlyNull(DOUBLE),
                Domain.none(DOUBLE));
        for (Domain domain : domains) {
            DomainPushdownResult result = mapping.getPredicatePushdownController().apply(SESSION, domain);
            assertThat(result.getPushedDown()).as(domain.toString()).isEqualTo(domain);
            assertThat(result.getRemainingFilter()).as(domain.toString()).isEqualTo(Domain.all(DOUBLE));
        }
    }

    @Test
    void testMappingControllerDecidesOtherFilters()
    {
        ColumnMapping disabled = ColumnMapping.mapping(DOUBLE, doubleColumnMapping().getReadFunction(), doubleColumnMapping().getWriteFunction(), DISABLE_PUSHDOWN);
        Domain domain = Domain.singleValue(DOUBLE, 1.0);

        DomainPushdownResult result = ComplementFilterPushdown.wrap(disabled).getPredicatePushdownController().apply(SESSION, domain);

        assertThat(result.getPushedDown()).isEqualTo(Domain.all(DOUBLE));
        assertThat(result.getRemainingFilter()).isEqualTo(domain);
    }

    @Test
    void testTypesWithoutNaNAreNotWrapped()
    {
        // NOT (x = ?) selects the same rows as x < 1 OR x > 1 when the type has no NaN
        ColumnMapping mapping = bigintColumnMapping();
        assertThat(ComplementFilterPushdown.wrap(mapping)).isSameAs(mapping);

        Domain otherThanOne = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false);
        assertThat(mapping.getPredicatePushdownController().apply(SESSION, otherThanOne).getPushedDown()).isEqualTo(otherThanOne);
    }

    private static void assertLocal(ColumnMapping mapping, Domain domain)
    {
        Type type = mapping.getType();
        DomainPushdownResult result = ComplementFilterPushdown.wrap(mapping).getPredicatePushdownController().apply(SESSION, domain);
        assertThat(result.getPushedDown()).as(domain.toString()).isEqualTo(Domain.all(type));
        assertThat(result.getRemainingFilter()).as(domain.toString()).isEqualTo(domain);
    }
}
