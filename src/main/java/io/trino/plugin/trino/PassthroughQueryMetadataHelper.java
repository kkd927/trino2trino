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
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

final class PassthroughQueryMetadataHelper
{
    @FunctionalInterface
    interface ColumnMappingResolver
    {
        Optional<ColumnMapping> resolve(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle);
    }

    record DescribedOutputColumn(String name, String typeName) {}

    private final TypeManager typeManager;
    private final ColumnMappingResolver columnMappingResolver;

    PassthroughQueryMetadataHelper(TypeManager typeManager, ColumnMappingResolver columnMappingResolver)
    {
        this.typeManager = typeManager;
        this.columnMappingResolver = columnMappingResolver;
    }

    List<DescribedOutputColumn> describeOutputColumns(Connection connection, PreparedQuery preparedQuery)
            throws SQLException
    {
        String statementName = "t2t_" + UUID.randomUUID().toString().replace("-", "");
        String query = TrinoClient.stripTrailingSemicolon(preparedQuery.query());
        try (Statement statement = connection.createStatement()) {
            statement.execute("PREPARE " + statementName + " FROM " + query);
            try (ResultSet resultSet = statement.executeQuery("DESCRIBE OUTPUT " + statementName)) {
                List<DescribedOutputColumn> outputColumns = new ArrayList<>();
                while (resultSet.next()) {
                    outputColumns.add(new DescribedOutputColumn(resultSet.getString(1), resultSet.getString(5)));
                }
                return outputColumns;
            }
            finally {
                statement.execute("DEALLOCATE PREPARE " + statementName);
            }
        }
    }

    JdbcTableHandle buildPassthroughTableHandle(ConnectorSession session, Connection connection, PreparedQuery preparedQuery, List<DescribedOutputColumn> outputColumns)
    {
        return new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                List.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(toJdbcColumnHandles(session, connection, outputColumns)),
                Optional.empty(),
                0,
                Optional.empty(),
                List.of());
    }

    JdbcTableHandle rewriteColumns(ConnectorSession session, Connection connection, JdbcTableHandle tableHandle, List<DescribedOutputColumn> outputColumns)
    {
        if (tableHandle.getColumns().isEmpty()) {
            return tableHandle;
        }

        List<JdbcColumnHandle> columns = tableHandle.getColumns().orElseThrow();
        if (outputColumns.size() != columns.size()) {
            return tableHandle;
        }

        List<JdbcColumnHandle> rewrittenColumns = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            JdbcColumnHandle column = columns.get(index);
            DescribedOutputColumn outputColumn = outputColumns.get(index);
            String outputType = outputColumn.typeName();
            JdbcTypeHandle rewrittenTypeHandle = jdbcTypeHandleForTypeName(outputType);
            Type mappedType = columnMappingResolver.resolve(session, connection, rewrittenTypeHandle)
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported passthrough result type: " + outputType))
                    .getType();
            rewrittenColumns.add(new JdbcColumnHandle(column.getColumnName(), rewrittenTypeHandle, mappedType));
        }

        return new JdbcTableHandle(
                tableHandle.getRelationHandle(),
                tableHandle.getConstraint(),
                tableHandle.getConstraintExpressions(),
                tableHandle.getSortOrder(),
                tableHandle.getLimit(),
                Optional.of(rewrittenColumns),
                tableHandle.getOtherReferencedTables(),
                tableHandle.getNextSyntheticColumnId(),
                tableHandle.getAuthorization(),
                tableHandle.getUpdateAssignments());
    }

    private List<JdbcColumnHandle> toJdbcColumnHandles(ConnectorSession session, Connection connection, List<DescribedOutputColumn> outputColumns)
    {
        List<JdbcColumnHandle> columns = new ArrayList<>(outputColumns.size());
        for (DescribedOutputColumn outputColumn : outputColumns) {
            JdbcTypeHandle jdbcTypeHandle = jdbcTypeHandleForTypeName(outputColumn.typeName());
            Type mappedType = columnMappingResolver.resolve(session, connection, jdbcTypeHandle)
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported passthrough result type: " + outputColumn.typeName()))
                    .getType();
            columns.add(new JdbcColumnHandle(outputColumn.name(), jdbcTypeHandle, mappedType));
        }
        return columns;
    }

    private JdbcTypeHandle jdbcTypeHandleForTypeName(String typeName)
    {
        return TrinoJdbcTypeHandleResolver.resolve(typeManager, typeName);
    }
}
