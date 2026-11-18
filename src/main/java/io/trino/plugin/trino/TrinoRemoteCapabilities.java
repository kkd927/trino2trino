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

import io.trino.jdbc.TrinoResultSet;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.OPERATOR_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * What the connector knows about the remote server.
 * <p>
 * {@code sameVersionAsLocal} holds whether the remote server reports the version string
 * of the local server. {@code otherEvaluationSettings} holds the configuration properties
 * with which such a server evaluates expressions differently from a server with default
 * settings. A remote server of the local version without such a setting evaluates every
 * expression as a local server with default settings does. Any other server, including a
 * build of the same release with another version string, receives only the basic list of
 * {@link TrinoCompatibilityRegistry}.
 */
record TrinoRemoteCapabilities(
        Optional<String> version,
        boolean sameVersionAsLocal,
        List<String> otherEvaluationSettings,
        Optional<Set<String>> functions,
        Optional<String> timeZone)
{
    // One query for three behaviors of the remote server. Each value is true on a server
    // with default settings. A server of the local version returns false only when it sets
    // the configuration property at the same position of EVALUATION_SETTINGS: decimal
    // arithmetic with the precision and scale of the releases before Trino 477, regular
    // expressions with the RE2J library, and casts from char to varchar that keep the
    // padding, as before Trino 482.
    static final String EVALUATION_PROBE = "SELECT " +
            "TRY(DECIMAL '1.00' / DECIMAL '3.00' = DECIMAL '0.333333' " +
            "AND typeof(CAST(1 AS decimal(38,10)) * CAST(1 AS decimal(38,10))) = 'decimal(38,6)'), " +
            "TRY(regexp_like('a' || chr(10), 'a$')), " +
            "TRY(length(CAST(CAST('a' AS char(3)) AS varchar)) = 1)";

    static final List<String> EVALUATION_SETTINGS = List.of(
            "deprecated.legacy-arithmetic-decimal-operators",
            "deprecated.regex-library",
            "deprecated.legacy-varchar-to-char-coercion");

    // Errors that the remote server reports for every attempt of the same query, such as a
    // function or type that it does not have. Other errors, such as a full queue or a killed
    // query, can succeed on retry, so the connector does not keep their result.
    private static final Set<Integer> DETERMINISTIC_PROBE_ERRORS = Stream.of(
                    SYNTAX_ERROR,
                    FUNCTION_NOT_FOUND,
                    FUNCTION_IMPLEMENTATION_MISSING,
                    OPERATOR_NOT_FOUND,
                    AMBIGUOUS_FUNCTION_CALL,
                    TYPE_NOT_FOUND,
                    TYPE_MISMATCH,
                    COLUMN_NOT_FOUND,
                    INVALID_LITERAL,
                    INVALID_ARGUMENTS,
                    EXPRESSION_NOT_SCALAR,
                    SUBQUERY_MULTIPLE_ROWS,
                    NOT_SUPPORTED,
                    PERMISSION_DENIED,
                    INVALID_FUNCTION_ARGUMENT,
                    INVALID_CAST_ARGUMENT,
                    NUMERIC_VALUE_OUT_OF_RANGE,
                    DIVISION_BY_ZERO)
            .map(supplier -> supplier.toErrorCode().getCode())
            .collect(Collectors.toUnmodifiableSet());

    TrinoRemoteCapabilities
    {
        version = requireNonNull(version, "version is null");
        otherEvaluationSettings = List.copyOf(requireNonNull(otherEvaluationSettings, "otherEvaluationSettings is null"));
        functions = requireNonNull(functions, "functions is null").map(Set::copyOf);
        timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    /**
     * What one check of the remote server returns: its version, the values of
     * {@link #EVALUATION_PROBE}, and the identifier of its coordinator process. Trino ends
     * every query identifier with a random value that a coordinator chooses when it starts,
     * so another value means that the remote server restarted or that another server
     * answered. {@code serverProcess} is empty when the probe returned no query identifier.
     */
    record Probe(Optional<String> version, List<Boolean> evaluation, Optional<String> serverProcess)
    {
        Probe
        {
            requireNonNull(version, "version is null");
            evaluation = List.copyOf(requireNonNull(evaluation, "evaluation is null"));
            requireNonNull(serverProcess, "serverProcess is null");
        }

        // A value is false when the remote server returns false or NULL for it, and all
        // values are false when the remote server cannot plan the query
        static Probe read(Connection connection)
                throws SQLException
        {
            requireNonNull(connection, "connection is null");
            Optional<String> version = Optional.ofNullable(connection.getMetaData().getDatabaseProductVersion());
            List<Boolean> allFalse = Collections.nCopies(EVALUATION_SETTINGS.size(), false);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(EVALUATION_PROBE)) {
                Optional<String> serverProcess = serverProcess(resultSet);
                if (!resultSet.next()) {
                    return new Probe(version, allFalse, serverProcess);
                }
                List<Boolean> values = new ArrayList<>();
                for (int column = 1; column <= EVALUATION_SETTINGS.size(); column++) {
                    boolean value = resultSet.getBoolean(column);
                    values.add(value && !resultSet.wasNull());
                }
                return new Probe(version, values, serverProcess);
            }
            catch (SQLException e) {
                if (isDeterministicProbeFailure(e)) {
                    return new Probe(version, allFalse, Optional.empty());
                }
                throw e;
            }
        }

        /**
         * Whether a later check read the server of this check. Without the identifier of
         * the server process, a server that restarted with another time zone or function
         * list would pass, so such a check never counts as the same server.
         */
        boolean isSameServer(Probe later)
        {
            return serverProcess.isPresent() && equals(later);
        }

        private static Optional<String> serverProcess(ResultSet resultSet)
                throws SQLException
        {
            if (!resultSet.isWrapperFor(TrinoResultSet.class)) {
                return Optional.empty();
            }
            return serverProcess(resultSet.unwrap(TrinoResultSet.class).getQueryId());
        }

        // A query identifier is <date>_<time>_<counter>_<coordinator process>
        static Optional<String> serverProcess(String queryId)
        {
            int separator = queryId.lastIndexOf('_');
            if (separator < 0 || separator == queryId.length() - 1) {
                return Optional.empty();
            }
            return Optional.of(queryId.substring(separator + 1));
        }
    }

    /**
     * Loads the remote capabilities. A query that fails with an error that a retry could
     * avoid fails the whole load, so that the caller retries later instead of keeping a
     * result that disables pushdown.
     */
    static TrinoRemoteCapabilities load(Connection connection, String localVersion)
            throws SQLException
    {
        return load(connection, localVersion, Probe.read(connection));
    }

    static TrinoRemoteCapabilities load(Connection connection, String localVersion, Probe probe)
            throws SQLException
    {
        requireNonNull(connection, "connection is null");
        requireNonNull(localVersion, "localVersion is null");
        Optional<String> version = probe.version();
        boolean sameVersionAsLocal = version.filter(localVersion::equals).isPresent();
        List<Boolean> evaluation = probe.evaluation();
        List<String> otherEvaluationSettings = new ArrayList<>();
        if (sameVersionAsLocal) {
            for (int index = 0; index < EVALUATION_SETTINGS.size(); index++) {
                if (!evaluation.get(index)) {
                    otherEvaluationSettings.add(EVALUATION_SETTINGS.get(index));
                }
            }
        }
        return new TrinoRemoteCapabilities(
                version,
                sameVersionAsLocal,
                otherEvaluationSettings,
                Optional.of(loadFunctionNames(connection)),
                loadCurrentTimeZone(connection));
    }

    static TrinoRemoteCapabilities unavailable()
    {
        return new TrinoRemoteCapabilities(Optional.empty(), false, List.of(), Optional.empty(), Optional.empty());
    }

    static TrinoRemoteCapabilities forTesting(Set<String> functions)
    {
        return new TrinoRemoteCapabilities(Optional.of("484"), true, List.of(), Optional.of(functions), Optional.of("UTC"));
    }

    static TrinoRemoteCapabilities forTestingOtherVersion(Set<String> functions)
    {
        return new TrinoRemoteCapabilities(Optional.of("483"), false, List.of(), Optional.of(functions), Optional.of("UTC"));
    }

    static TrinoRemoteCapabilities forTestingOtherSettings(Set<String> functions, List<String> otherEvaluationSettings)
    {
        return new TrinoRemoteCapabilities(Optional.of("484"), true, otherEvaluationSettings, Optional.of(functions), Optional.of("UTC"));
    }

    /**
     * Whether the remote server evaluates every expression as a local server with default
     * settings does. The connector cannot read the settings of the local server.
     */
    boolean evaluatesAsLocal()
    {
        return sameVersionAsLocal && otherEvaluationSettings.isEmpty();
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

    static boolean isDeterministicProbeFailure(SQLException e)
    {
        // The Trino JDBC driver reports the remote error code as the vendor code. Client
        // failures, such as a lost connection, have no remote error code.
        return DETERMINISTIC_PROBE_ERRORS.contains(e.getErrorCode());
    }
}
