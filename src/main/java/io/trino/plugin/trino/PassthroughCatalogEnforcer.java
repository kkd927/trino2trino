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

import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableFunctionInvocation;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

final class PassthroughCatalogEnforcer
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final String remoteCatalog;

    PassthroughCatalogEnforcer(String remoteCatalog)
    {
        this.remoteCatalog = remoteCatalog.toLowerCase(Locale.ENGLISH);
    }

    void validate(String sql)
    {
        Statement statement = SQL_PARSER.createStatement(sql);
        if (!(statement instanceof Query query)) {
            throw new TrinoException(NOT_SUPPORTED, "system.query only supports row-returning read queries");
        }

        Set<String> disallowedCatalogs = new LinkedHashSet<>();
        Set<String> nestedPassthroughQueries = new LinkedHashSet<>();
        new DefaultTraversalVisitor<Set<String>>()
        {
            @Override
            protected Void visitTable(Table node, Set<String> context)
            {
                rejectDisallowedCatalog(node.getName(), context);
                return super.visitTable(node, context);
            }

            @Override
            protected Void visitTableFunctionInvocation(TableFunctionInvocation node, Set<String> context)
            {
                rejectDisallowedCatalog(node.getName(), context);
                rejectNestedPassthroughQuery(node.getName(), nestedPassthroughQueries);
                return super.visitTableFunctionInvocation(node, context);
            }
        }.process(query, disallowedCatalogs);

        if (!disallowedCatalogs.isEmpty()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "system.query may reference only the configured remote catalog '" + remoteCatalog + "': " + String.join(", ", disallowedCatalogs));
        }
        if (!nestedPassthroughQueries.isEmpty()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Nested system.query calls are not supported in passthrough SQL: " + String.join(", ", nestedPassthroughQueries));
        }
    }

    private void rejectDisallowedCatalog(QualifiedName name, Set<String> disallowedCatalogs)
    {
        if (name.getParts().size() >= 3) {
            String catalog = name.getOriginalParts().getFirst().getCanonicalValue().toLowerCase(Locale.ENGLISH);
            if (!catalog.equals(remoteCatalog)) {
                disallowedCatalogs.add(name.toString());
            }
        }
    }

    private static void rejectNestedPassthroughQuery(QualifiedName name, Set<String> nestedPassthroughQueries)
    {
        if (name.getParts().size() >= 2) {
            int size = name.getOriginalParts().size();
            String schema = name.getOriginalParts().get(size - 2).getCanonicalValue().toLowerCase(Locale.ENGLISH);
            String function = name.getOriginalParts().get(size - 1).getCanonicalValue().toLowerCase(Locale.ENGLISH);
            if (schema.equals("system") && function.equals("query")) {
                nestedPassthroughQueries.add(name.toString());
            }
        }
    }
}
