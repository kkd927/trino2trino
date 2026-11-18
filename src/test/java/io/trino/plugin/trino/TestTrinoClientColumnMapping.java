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

import io.trino.json.Json;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TrinoNumber;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoClientColumnMapping
{
    @Test
    void testIntervalPredicatePushdownDependsOnlyOnRemoteTypeName()
    {
        // Workers rebuild column mappings when they render tuple-domain filters. A worker
        // that cannot load remote capabilities must reach the coordinator's decision, or
        // the filter is removed locally but never applied remotely.
        AtomicInteger connectionAttempts = new AtomicInteger();
        TrinoClient client = createClient(connectionAttempts);

        assertThat(predicatePushdownController(client, Types.JAVA_OBJECT, "interval day(2) to second(6)")).isEqualTo(FULL_PUSHDOWN);
        assertThat(predicatePushdownController(client, Types.JAVA_OBJECT, "interval year(2) to month")).isEqualTo(FULL_PUSHDOWN);
        assertThat(predicatePushdownController(client, Types.JAVA_OBJECT, "interval day to second")).isEqualTo(DISABLE_PUSHDOWN);
        assertThat(predicatePushdownController(client, Types.JAVA_OBJECT, "interval year to month")).isEqualTo(DISABLE_PUSHDOWN);
        assertThat(connectionAttempts).hasValue(0);
    }

    @Test
    void testNumberPredicatePushdownDependsOnlyOnColumnMetadata()
    {
        // Servers before 484 report number columns as JAVA_OBJECT without a column size, and
        // servers before 482 compare number values by their bytes. Workers must reach the
        // same decision as the coordinator, so it depends only on the type handle.
        AtomicInteger connectionAttempts = new AtomicInteger();
        TrinoClient client = createClient(connectionAttempts);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
                .build();

        PredicatePushdownController legacy = predicatePushdownController(client, Types.JAVA_OBJECT, "number", Optional.empty());
        // Handles built from a type name, as for query passthrough, have no column size
        PredicatePushdownController passthrough = predicatePushdownController(client, Types.OTHER, "number", Optional.empty());
        PredicatePushdownController current = predicatePushdownController(client, Types.OTHER, "number", Optional.of(201));
        assertThat(connectionAttempts).hasValue(0);

        TrinoNumber five = TrinoNumber.from(new BigDecimal("5"));
        Domain singleValue = Domain.singleValue(NUMBER, five);
        Domain range = Domain.create(ValueSet.ofRanges(Range.greaterThan(NUMBER, five)), false);
        for (PredicatePushdownController controller : List.of(legacy, passthrough)) {
            assertThat(controller.apply(session, singleValue).getPushedDown()).isEqualTo(Domain.all(NUMBER));
            assertThat(controller.apply(session, singleValue).getRemainingFilter()).isEqualTo(singleValue);
            assertThat(controller.apply(session, range).getPushedDown()).isEqualTo(range);
            assertThat(controller.apply(session, Domain.onlyNull(NUMBER)).getPushedDown()).isEqualTo(Domain.onlyNull(NUMBER));
        }
        assertThat(current.apply(session, singleValue).getPushedDown()).isEqualTo(singleValue);
        assertThat(current.apply(session, range).getPushedDown()).isEqualTo(range);

        // The query builder renders the values other than five as NOT (n = ?), which NaN passes
        // on every version, so the filter stays local for every number column
        Domain otherValues = Domain.create(ValueSet.ofRanges(Range.lessThan(NUMBER, five), Range.greaterThan(NUMBER, five)), false);
        for (PredicatePushdownController controller : List.of(legacy, passthrough, current)) {
            assertThat(controller.apply(session, otherValues).getPushedDown()).isEqualTo(Domain.all(NUMBER));
            assertThat(controller.apply(session, otherValues).getRemainingFilter()).isEqualTo(otherValues);
        }
    }

    @Test
    void testFiltersThatNaNPassesStayLocal()
    {
        // The decision must not depend on remote capabilities, so that the coordinator and
        // the workers render the same SQL
        AtomicInteger connectionAttempts = new AtomicInteger();
        TrinoClient client = createClient(connectionAttempts);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
                .build();

        Domain doubles = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0), Range.greaterThan(DOUBLE, 1.0)), false);
        assertThat(pushedDown(client, session, Types.DOUBLE, "double", doubles)).isEqualTo(Domain.all(DOUBLE));
        long one = floatToRawIntBits(1.0f);
        Domain reals = Domain.create(ValueSet.ofRanges(Range.lessThan(REAL, one), Range.greaterThan(REAL, one)), false);
        assertThat(pushedDown(client, session, Types.REAL, "real", reals)).isEqualTo(Domain.all(REAL));
        // A type without NaN keeps the filter on the remote server
        Domain bigints = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false);
        assertThat(pushedDown(client, session, Types.BIGINT, "bigint", bigints)).isEqualTo(bigints);
        // Ranges whose complement is a range stay on the remote server
        Domain outside = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 1.0), Range.greaterThan(DOUBLE, 2.0)), false);
        assertThat(pushedDown(client, session, Types.DOUBLE, "double", outside)).isEqualTo(outside);
        assertThat(connectionAttempts).hasValue(0);
    }

    @Test
    void testJsonColumnsKeepRawTextWithoutParsing()
            throws SQLException
    {
        // Servers before 484 accepted JSON values that the 484 parser rejects, such as numbers
        // beyond double range. The remote server already validated the text, so the connector
        // keeps it as raw text, as it does for nested JSON values.
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(Types.JAVA_OBJECT, Optional.of("json"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        ColumnMapping mapping = createClient(new AtomicInteger()).toColumnMapping(SESSION, null, typeHandle).orElseThrow();
        assertThat(mapping.getPredicatePushdownController()).isEqualTo(DISABLE_PUSHDOWN);

        String text = "{\"x\":1e400}";
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (_, method, _) -> switch (method.getName()) {
                    case "getString" -> text;
                    case "wasNull" -> false;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Json value = (Json) ((ObjectReadFunction) mapping.getReadFunction()).readObject(resultSet, 1);

        assertThat(value.isRawText()).isTrue();
        assertThat(value.rawText().toStringUtf8()).isEqualTo(text);
    }

    private static TrinoClient createClient(AtomicInteger connectionAttempts)
    {
        return new TrinoClient(
                new BaseJdbcConfig(),
                new JdbcStatisticsConfig(),
                _ -> {
                    connectionAttempts.incrementAndGet();
                    throw new SQLException("remote unavailable");
                },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new TestingFunctionResolution().getPlannerContext().getTypeManager(),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE,
                new NodeVersion("484"),
                new TrinoRemoteServerConfig());
    }

    private static Domain pushedDown(TrinoClient client, ConnectorSession session, int jdbcType, String typeName, Domain domain)
    {
        return predicatePushdownController(client, jdbcType, typeName).apply(session, domain).getPushedDown();
    }

    @Test
    void testCharFilterValuesBindAsChar()
    {
        // A bare parameter is a varchar value, and servers from 482 compare a char column
        // with a varchar value without padding. 'ab' followed by a tab is less than 'ab' as
        // char(5) and greater as varchar, so the filter c < 'ab' would miss the row.
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(Types.CHAR, Optional.of("char(5)"), Optional.of(5), Optional.empty(), Optional.empty(), Optional.empty());
        ColumnMapping mapping = createClient(new AtomicInteger()).toColumnMapping(SESSION, null, typeHandle).orElseThrow();

        assertThat(mapping.getType()).isEqualTo(createCharType(5));
        assertThat(mapping.getWriteFunction().getBindExpression()).isEqualTo("CAST(? AS char(5))");
    }

    private static PredicatePushdownController predicatePushdownController(TrinoClient client, int jdbcType, String typeName)
    {
        return predicatePushdownController(client, jdbcType, typeName, Optional.empty());
    }

    private static PredicatePushdownController predicatePushdownController(TrinoClient client, int jdbcType, String typeName, Optional<Integer> columnSize)
    {
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(jdbcType, Optional.of(typeName), columnSize, Optional.empty(), Optional.empty(), Optional.empty());
        return client.toColumnMapping(SESSION, null, typeHandle).orElseThrow().getPredicatePushdownController();
    }
}
