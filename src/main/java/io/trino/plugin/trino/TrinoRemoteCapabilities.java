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

import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

record TrinoRemoteCapabilities(
        Optional<String> version,
        Optional<Set<String>> functions,
        Optional<String> timeZone)
{
    TrinoRemoteCapabilities
    {
        version = requireNonNull(version, "version is null");
        functions = requireNonNull(functions, "functions is null").map(Set::copyOf);
        timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    static TrinoRemoteCapabilities load(Connection connection)
            throws SQLException
    {
        requireNonNull(connection, "connection is null");
        return new TrinoRemoteCapabilities(
                Optional.ofNullable(connection.getMetaData().getDatabaseProductVersion()),
                Optional.of(loadFunctionNames(connection)),
                loadCurrentTimeZone(connection));
    }

    static TrinoRemoteCapabilities unavailable()
    {
        return new TrinoRemoteCapabilities(Optional.empty(), Optional.empty(), Optional.empty());
    }

    static TrinoRemoteCapabilities forTesting(Set<String> functions)
    {
        return new TrinoRemoteCapabilities(Optional.of("477"), Optional.of(functions), Optional.of("UTC"));
    }

    boolean hasFunction(String name)
    {
        requireNonNull(name, "name is null");
        return functions
                .map(values -> values.contains(name.toLowerCase(Locale.ENGLISH)))
                .orElse(false);
    }

    boolean hasSameTimeZone(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        return timeZone
                .map(remoteTimeZone -> remoteTimeZone.equalsIgnoreCase(session.getTimeZoneKey().getId()))
                .orElse(false);
    }

    private static Set<String> loadFunctionNames(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
            Set<String> names = new HashSet<>();
            while (resultSet.next()) {
                names.add(resultSet.getString(1).toLowerCase(Locale.ENGLISH));
            }
            return Set.copyOf(names);
        }
    }

    private static Optional<String> loadCurrentTimeZone(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT current_timezone()")) {
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getString(1));
            }
        }
        return Optional.empty();
    }
}
