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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcJoinPushdownUtil;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.matching.Pattern.typeOf;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

/**
 * JDBC client for connecting one Trino instance to another.
 * <p>
 * Supports all standard Trino types including complex types (ARRAY, MAP, ROW)
 * by parsing JDBC type metadata and recursively mapping to Trino's type system.
 */
public class TrinoClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(TrinoClient.class);

    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final JdbcTypeHandle VARCHAR_TYPE_HANDLE = new JdbcTypeHandle(
            Types.VARCHAR, Optional.of("varchar"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final JdbcTypeHandle VARBINARY_TYPE_HANDLE = new JdbcTypeHandle(
            Types.VARBINARY, Optional.of("varbinary"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ParameterizedExpression> aggregateFunctionRewriter;
    private final JsonTransportHelper jsonTransportHelper;
    private final TrinoReadMappingFactory readMappingFactory;
    private final PassthroughCatalogEnforcer passthroughCatalogEnforcer;
    private final PassthroughQueryMetadataHelper passthroughQueryMetadataHelper;
    private final AtomicBoolean remoteVersionLogged = new AtomicBoolean();

    @Inject
    public TrinoClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.jsonTransportHelper = new JsonTransportHelper(this::quoted);
        this.readMappingFactory = new TrinoReadMappingFactory(typeManager, typeHandle -> mapToUnboundedVarchar(typeHandle));
        this.passthroughCatalogEnforcer = new PassthroughCatalogEnforcer(extractRemoteCatalog(config.getConnectionUrl()));
        this.passthroughQueryMetadataHelper = new PassthroughQueryMetadataHelper(typeManager, this::toColumnMapping);

        // Both sides are Trino, so all standard expression rewriting rules apply.
        this.connectorExpressionRewriter = createConnectorExpressionRewriter(this::quoted);

        // Trino supports all standard aggregate functions natively, so pushdown is safe.
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                Set.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>of(
                        new ImplementCountAll(BIGINT_TYPE_HANDLE),
                        new ImplementCount(BIGINT_TYPE_HANDLE),
                        new ImplementCountDistinct(BIGINT_TYPE_HANDLE, true),
                        new ImplementMinMax(false), // Trino has no collation differences
                        new ImplementSum(TrinoClient::decimalTypeHandle),
                        new ImplementAvgFloatingPoint(),
                        new ImplementAvgDecimal()));
    }

    static ConnectorExpressionRewriter<ParameterizedExpression> createConnectorExpressionRewriter(Function<String, String> identifierQuote)
    {
        return JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(identifierQuote)
                .add(new RewriteComparison(EnumSet.allOf(ComparisonOperator.class)))
                .add(new RewriteBinaryArithmeticOperator(StandardFunctions.ADD_FUNCTION_NAME, "+"))
                .add(new RewriteBinaryArithmeticOperator(StandardFunctions.SUBTRACT_FUNCTION_NAME, "-"))
                .build();
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(
                Types.DECIMAL,
                Optional.of("decimal"),
                Optional.of(decimalType.getPrecision()),
                Optional.of(decimalType.getScale()),
                Optional.empty(),
                Optional.empty()));
    }

    private static final class RewriteBinaryArithmeticOperator
            implements ConnectorExpressionRule<Call, ParameterizedExpression>
    {
        private final FunctionName functionName;
        private final String sqlOperator;

        private RewriteBinaryArithmeticOperator(FunctionName functionName, String sqlOperator)
        {
            this.functionName = functionName;
            this.sqlOperator = sqlOperator;
        }

        @Override
        public io.trino.matching.Pattern<Call> getPattern()
        {
            return typeOf(Call.class)
                    .matching(call -> call.getArguments().size() == 2)
                    .matching(call -> call.getFunctionName().equals(functionName));
        }

        @Override
        public Optional<ParameterizedExpression> rewrite(Call expression, io.trino.matching.Captures captures, RewriteContext<ParameterizedExpression> context)
        {
            Optional<ParameterizedExpression> left = context.defaultRewrite(expression.getArguments().get(0));
            Optional<ParameterizedExpression> right = context.defaultRewrite(expression.getArguments().get(1));
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }

            ParameterizedExpression leftExpression = left.get();
            ParameterizedExpression rightExpression = right.get();
            List<io.trino.plugin.jdbc.QueryParameter> parameters = new ArrayList<>(
                    leftExpression.parameters().size() + rightExpression.parameters().size());
            parameters.addAll(leftExpression.parameters());
            parameters.addAll(rightExpression.parameters());

            return Optional.of(new ParameterizedExpression(
                    format("(%s %s %s)", leftExpression.expression(), sqlOperator, rightExpression.expression()),
                    List.copyOf(parameters)));
        }
    }

    // -------------------------------------------------------------------------
    // Pushdown support: LIMIT, TopN, Aggregation, Join
    //
    // Both sides are Trino with identical SQL syntax, so all standard
    // pushdown operations below are safe. This significantly reduces data transfer
    // through the JDBC bottleneck.
    // -------------------------------------------------------------------------

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // Both sides are Trino, so these comparison operators are SQL-compatible
        // when the planner presents them as connector join conditions.
        return switch (joinCondition.getOperator()) {
            case EQUAL,
                    NOT_EQUAL,
                    LESS_THAN,
                    LESS_THAN_OR_EQUAL,
                    GREATER_THAN,
                    GREATER_THAN_OR_EQUAL,
                    IDENTICAL -> true;
        };
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> leftAssignments,
            Map<JdbcColumnHandle, String> rightAssignments,
            JoinStatistics statistics)
    {
        return JdbcJoinPushdownUtil.implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.legacyImplementJoin(
                        session,
                        joinType,
                        leftSource,
                        rightSource,
                        joinConditions,
                        leftAssignments,
                        rightAssignments,
                        statistics));
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        return JdbcJoinPushdownUtil.implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(
                        session,
                        joinType,
                        leftSource,
                        leftProjections,
                        rightSource,
                        rightProjections,
                        joinConditions,
                        statistics));
    }

    @Override
    protected PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            Optional<JdbcSplit> split)
    {
        PreparedQuery preparedQuery = super.prepareQuery(session, connection, table, groupingSets, columns, columnExpressions, split);
        return applyTransportProjection(preparedQuery, columns);
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        passthroughCatalogEnforcer.validate(stripTrailingSemicolon(preparedQuery.query()));
        try (Connection connection = getConnection(session)) {
            List<PassthroughQueryMetadataHelper.DescribedOutputColumn> outputColumns = passthroughQueryMetadataHelper.describeOutputColumns(connection, preparedQuery);
            JdbcTableHandle tableHandle;
            try {
                tableHandle = super.getTableHandle(session, preparedQuery);
            }
            catch (TrinoException | UnsupportedOperationException e) {
                // BaseJdbcClient.getTableHandle may throw TrinoException for passthrough
                // queries that use types or syntax not representable by standard JDBC metadata.
                // Some paths surface as UnsupportedOperationException from BaseJdbcClient.
                // Fall back to building the table handle from DESCRIBE OUTPUT metadata.
                return passthroughQueryMetadataHelper.buildPassthroughTableHandle(session, connection, preparedQuery, outputColumns);
            }
            return passthroughQueryMetadataHelper.rewriteColumns(session, connection, tableHandle, outputColumns);
        }
        catch (SQLException | TrinoException e) {
            return super.getTableHandle(session, preparedQuery);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!handle.isNamedRelation()) {
            return TableStatistics.builder().build();
        }

        List<JdbcColumnHandle> columns = handle.getColumns().orElse(List.of());
        if (columns.isEmpty()) {
            return TableStatistics.builder().build();
        }

        String statsQuery = "SHOW STATS FOR " + quoted(handle.getRequiredNamedRelation().getRemoteTableName());
        try (Connection connection = getConnection(session)) {
            logRemoteVersionOnce(connection);
            try (PreparedStatement statement = connection.prepareStatement(statsQuery);
                    ResultSet resultSet = statement.executeQuery()) {
                TableStatistics.Builder tableStatistics = TableStatistics.builder();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    if (columnName == null) {
                        Double rowCount = getNullableDouble(resultSet, "row_count");
                        if (rowCount != null) {
                            tableStatistics.setRowCount(Estimate.of(rowCount));
                        }
                        continue;
                    }

                    Optional<JdbcColumnHandle> column = findColumn(columns, columnName);
                    if (column.isPresent()) {
                        tableStatistics.setColumnStatistics(column.orElseThrow(), ColumnStatistics.builder()
                                .setDataSize(toEstimate(getNullableDouble(resultSet, "data_size")))
                                .setDistinctValuesCount(toEstimate(getNullableDouble(resultSet, "distinct_values_count")))
                                .setNullsFraction(toEstimate(getNullableDouble(resultSet, "nulls_fraction")))
                                .build());
                    }
                }
                return tableStatistics.build();
            }
        }
        catch (SQLException | RuntimeException e) {
            log.warn(e, "Failed to fetch table statistics: %s", statsQuery);
            return TableStatistics.builder().build();
        }
    }

    private void logRemoteVersionOnce(Connection connection)
    {
        if (remoteVersionLogged.compareAndSet(false, true)) {
            try {
                String version = connection.getMetaData().getDatabaseProductVersion();
                log.info("Remote Trino version: %s", version);
            }
            catch (SQLException e) {
                log.warn(e, "Unable to determine remote Trino version");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read-only enforcement: block all write and DDL operations
    // -------------------------------------------------------------------------

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        // JDBC query builders use write mappings for SELECT predicate parameter binding.
        // Keep those mappings available while blocking data-changing operations separately.
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", (statement, index, value) -> statement.setBoolean(index, value));
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", (statement, index, value) -> statement.setByte(index, (byte) value));
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", (statement, index, value) -> statement.setShort(index, (short) value));
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", (statement, index, value) -> statement.setInt(index, (int) value));
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", (statement, index, value) -> statement.setLong(index, value));
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", (statement, index, value) -> statement.setFloat(index, Float.intBitsToFloat((int) value)));
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", (statement, index, value) -> statement.setDouble(index, value));
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = "decimal(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return WriteMapping.sliceMapping("varchar", (statement, index, value) -> statement.setString(index, value.toStringUtf8()));
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary", (statement, index, value) -> statement.setBytes(index, value.getBytes()));
        }
        if (type instanceof DateType) {
            return WriteMapping.longMapping("date", io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping("time(" + timeType.getPrecision() + ")", io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return WriteMapping.longMapping("timestamp(" + timestampType.getPrecision() + ")", io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction(timestampType));
            }
            if (timestampType.getPrecision() <= 9) {
                return WriteMapping.objectMapping(
                        "timestamp(" + timestampType.getPrecision() + ")",
                        io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction(timestampType, timestampType.getPrecision()));
            }
            return TemporalTransportCodec.timestampWriteMapping(timestampType);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return TemporalTransportCodec.timestampWithTimeZoneWriteMapping(timestampWithTimeZoneType);
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return TemporalTransportCodec.timeWithTimeZoneWriteMapping(timeWithTimeZoneType);
        }
        if (TrinoTypeClassifier.isIntervalYearToMonthType(type) || TrinoTypeClassifier.isIntervalDayToSecondType(type)) {
            return TemporalTransportCodec.intervalWriteMapping(type);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported parameter type for pushdown: " + type);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    // -------------------------------------------------------------------------
    // Read mapping: JDBC type -> Trino type
    // -------------------------------------------------------------------------

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        return readMappingFactory.createColumnMapping(session, typeHandle);
    }

    private PreparedQuery applyTransportProjection(PreparedQuery preparedQuery, List<JdbcColumnHandle> columns)
    {
        List<TrinoTypeClassifier.TransportKind> transportKinds = columns.stream()
                .map(JdbcColumnHandle::getColumnType)
                .map(TrinoTypeClassifier::transportKind)
                .toList();
        if (transportKinds.stream().allMatch(kind -> kind == TrinoTypeClassifier.TransportKind.NATIVE)) {
            return preparedQuery;
        }

        String relationAlias = quoted("_trino_transport");
        List<String> selectItems = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            JdbcColumnHandle column = columns.get(i);
            Type logicalType = column.getColumnType();
            TrinoTypeClassifier.TransportKind transportKind = transportKinds.get(i);
            String reference = relationAlias + "." + quoted(column.getColumnName());
            String alias = quoted(column.getColumnName());
            selectItems.add(switch (transportKind) {
                case NATIVE -> reference + " AS " + alias;
                case VARCHAR_CAST -> "CAST(" + reference + " AS VARCHAR) AS " + alias;
                case VARBINARY_CAST -> "CAST(" + reference + " AS VARBINARY) AS " + alias;
                case JSON_CAST -> "json_format(CAST(" + jsonTransportHelper.buildJsonTransportExpression(reference, logicalType) + " AS JSON)) AS " + alias;
            });
        }

        return preparedQuery.transformQuery(sql ->
                "SELECT " + String.join(", ", selectItems) + " FROM (" + sql + ") " + relationAlias);
    }

    private static String extractRemoteCatalog(String connectionUrl)
    {
        return TrinoConnectionUrl.extractRemoteCatalog(connectionUrl);
    }

    private static Estimate toEstimate(Double value)
    {
        return value == null ? Estimate.unknown() : Estimate.of(value);
    }

    private static Double getNullableDouble(ResultSet resultSet, String columnName)
            throws SQLException
    {
        Object value = resultSet.getObject(columnName);
        if (value == null) {
            return null;
        }
        return ((Number) value).doubleValue();
    }

    private static Optional<JdbcColumnHandle> findColumn(List<JdbcColumnHandle> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.getColumnName().equalsIgnoreCase(columnName))
                .findFirst();
    }

    static String stripTrailingSemicolon(String sql)
    {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
