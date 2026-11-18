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

import io.trino.plugin.trino.TrinoRemoteCapabilities.Probe;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.trino.TrinoRemoteCapabilities.EVALUATION_PROBE;
import static io.trino.plugin.trino.TrinoRemoteCapabilities.EVALUATION_SETTINGS;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_QUEUE_FULL;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTrinoRemoteCapabilities
{
    @Test
    void testOnlyTheLocalVersionStringCountsAsTheSameVersion()
            throws SQLException
    {
        Map<String, List<List<Object>>> results = Map.of(EVALUATION_PROBE, List.of(List.of(true, true, true)));

        TrinoRemoteCapabilities sameVersion = TrinoRemoteCapabilities.load(connection("484", results), "484");
        assertThat(sameVersion.version()).contains("484");
        assertThat(sameVersion.sameVersionAsLocal()).isTrue();
        assertThat(sameVersion.evaluatesAsLocal()).isTrue();

        // An older release, a newer release, and another build of the local release
        for (String remoteVersion : List.of("483", "485", "484-patch1", "484-e.1")) {
            TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.load(connection(remoteVersion, results), "484");
            assertThat(otherVersion.sameVersionAsLocal()).as(remoteVersion).isFalse();
            assertThat(otherVersion.evaluatesAsLocal()).as(remoteVersion).isFalse();
        }
        assertThat(TrinoRemoteCapabilities.load(connection(null, results), "484").sameVersionAsLocal()).isFalse();
    }

    @Test
    void testSettingsOfTheLocalVersionThatChangeEvaluation()
            throws SQLException
    {
        // Each value of the probe is false or NULL on a server of the local version that sets
        // the configuration property at the same position
        for (int index = 0; index < EVALUATION_SETTINGS.size(); index++) {
            for (Object other : Arrays.asList(false, null)) {
                List<Object> row = new ArrayList<>(List.of(true, true, true));
                row.set(index, other);

                TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.load(
                        connection("484", Map.of(EVALUATION_PROBE, List.of(row))),
                        "484");

                assertThat(capabilities.sameVersionAsLocal()).isTrue();
                assertThat(capabilities.otherEvaluationSettings()).containsExactly(EVALUATION_SETTINGS.get(index));
                assertThat(capabilities.evaluatesAsLocal()).isFalse();
            }
        }

        TrinoRemoteCapabilities allSettings = TrinoRemoteCapabilities.load(
                connection("484", Map.of(EVALUATION_PROBE, List.of(List.of(false, false, false)))),
                "484");
        assertThat(allSettings.otherEvaluationSettings()).isEqualTo(EVALUATION_SETTINGS);

        // The values say nothing about the settings of a server of another version, which
        // receives the basic list anyway
        TrinoRemoteCapabilities otherVersion = TrinoRemoteCapabilities.load(
                connection("480", Map.of(EVALUATION_PROBE, List.of(List.of(true, true, false)))),
                "484");
        assertThat(otherVersion.otherEvaluationSettings()).isEmpty();
        assertThat(otherVersion.evaluatesAsLocal()).isFalse();
    }

    @Test
    void testLoadReadsFunctionsAndTimeZone()
            throws SQLException
    {
        TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.load(
                connection("484", Map.of(
                        EVALUATION_PROBE, List.of(List.of(true, true, true)),
                        "SHOW FUNCTIONS", List.of(List.of("json_parse"), List.of("REGEXP_LIKE")))),
                "484");

        assertThat(capabilities.hasFunction("regexp_like")).isTrue();
        assertThat(capabilities.hasFunction("JSON_PARSE")).isTrue();
        assertThat(capabilities.hasFunction("lower")).isFalse();
        assertThat(capabilities.timeZone()).contains("UTC");
    }

    @Test
    void testTransientFailureFailsLoad()
    {
        // A full queue or a killed query must not be kept as the result of the probe
        assertThatThrownBy(() -> TrinoRemoteCapabilities.load(connection("484", Map.of(), QUERY_QUEUE_FULL.toErrorCode().getCode()), "484"))
                .isInstanceOf(SQLException.class);
        // A lost connection has no remote error code
        assertThatThrownBy(() -> TrinoRemoteCapabilities.load(connection("484", Map.of(), 0), "484"))
                .isInstanceOf(SQLException.class);
    }

    @Test
    void testProbeReadsTheVersionAndTheProbeValues()
            throws SQLException
    {
        Probe probe = Probe.read(connection("484", Map.of(EVALUATION_PROBE, List.of(Arrays.asList(true, false, null)))));
        // Only a result set of the Trino JDBC driver returns a query identifier
        assertThat(probe).isEqualTo(new Probe(Optional.of("484"), List.of(true, false, false), Optional.empty()));

        // A server that cannot plan the probe returns no value
        assertThat(Probe.read(connection("448", Map.of())))
                .isEqualTo(new Probe(Optional.of("448"), List.of(false, false, false), Optional.empty()));

        assertThatThrownBy(() -> Probe.read(connection("484", Map.of(), QUERY_QUEUE_FULL.toErrorCode().getCode())))
                .isInstanceOf(SQLException.class);
    }

    @Test
    void testServerProcessOfAQueryIdentifier()
    {
        assertThat(Probe.serverProcess("20261116_225427_00098_kemux")).contains("kemux");
        assertThat(Probe.serverProcess("20261116_225427_00098_")).isEmpty();
        assertThat(Probe.serverProcess("kemux")).isEmpty();
    }

    @Test
    void testSameServerNeedsTheSameServerProcess()
    {
        Probe probe = new Probe(Optional.of("484"), List.of(true, true, true), Optional.of("kemux"));

        assertThat(probe.isSameServer(new Probe(Optional.of("484"), List.of(true, true, true), Optional.of("kemux")))).isTrue();
        // A restart, another version, and another setting
        assertThat(probe.isSameServer(new Probe(Optional.of("484"), List.of(true, true, true), Optional.of("mvphb")))).isFalse();
        assertThat(probe.isSameServer(new Probe(Optional.of("483"), List.of(true, true, true), Optional.of("kemux")))).isFalse();
        assertThat(probe.isSameServer(new Probe(Optional.of("484"), List.of(true, false, true), Optional.of("kemux")))).isFalse();

        // Without a server process, a restart that keeps the probe values would pass
        Probe unknownProcess = new Probe(Optional.of("484"), List.of(true, true, true), Optional.empty());
        assertThat(unknownProcess.isSameServer(unknownProcess)).isFalse();
    }

    @Test
    void testDeterministicProbeFailures()
    {
        assertThat(TrinoRemoteCapabilities.isDeterministicProbeFailure(new SQLException("missing", "", FUNCTION_NOT_FOUND.toErrorCode().getCode()))).isTrue();
        assertThat(TrinoRemoteCapabilities.isDeterministicProbeFailure(new SQLException("missing", "", TYPE_NOT_FOUND.toErrorCode().getCode()))).isTrue();
        assertThat(TrinoRemoteCapabilities.isDeterministicProbeFailure(new SQLException("rejected", "", QUERY_REJECTED.toErrorCode().getCode()))).isFalse();
        assertThat(TrinoRemoteCapabilities.isDeterministicProbeFailure(new SQLException("internal", "", GENERIC_INTERNAL_ERROR.toErrorCode().getCode()))).isFalse();
        assertThat(TrinoRemoteCapabilities.isDeterministicProbeFailure(new SQLException("client"))).isFalse();
    }

    @Test
    void testUnavailableCapabilities()
    {
        TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.unavailable();
        // An unknown remote server receives only the basic list
        assertThat(capabilities.sameVersionAsLocal()).isFalse();
        assertThat(capabilities.evaluatesAsLocal()).isFalse();
        assertThat(capabilities.hasFunction("count")).isFalse();
    }

    private static Connection connection(String version, Map<String, List<List<Object>>> results)
    {
        return connection(version, results, FUNCTION_NOT_FOUND.toErrorCode().getCode());
    }

    /**
     * Returns a connection that answers each query from {@code queryResults} and fails
     * other queries with {@code missingQueryErrorCode}.
     */
    private static Connection connection(String version, Map<String, List<List<Object>>> queryResults, int missingQueryErrorCode)
    {
        Map<String, List<List<Object>>> results = new HashMap<>(queryResults);
        results.putIfAbsent("SHOW FUNCTIONS", List.of(List.of("json_parse")));
        results.put("SELECT current_timezone()", List.of(List.of("UTC")));

        Statement statement = proxy(Statement.class, (method, args) -> switch (method) {
            case "executeQuery" -> {
                String query = (String) args[0];
                List<List<Object>> rows = results.get(query);
                if (rows == null) {
                    throw new SQLException("Unexpected query: " + query, "", missingQueryErrorCode);
                }
                yield resultSet(rows);
            }
            case "close" -> null;
            default -> throw new UnsupportedOperationException(method);
        });
        DatabaseMetaData metadata = proxy(DatabaseMetaData.class, (method, _) -> switch (method) {
            case "getDatabaseProductVersion" -> version;
            default -> throw new UnsupportedOperationException(method);
        });
        return proxy(Connection.class, (method, _) -> switch (method) {
            case "createStatement" -> statement;
            case "getMetaData" -> metadata;
            case "close" -> null;
            default -> throw new UnsupportedOperationException(method);
        });
    }

    private static ResultSet resultSet(List<List<Object>> rows)
    {
        int[] rowIndex = {-1};
        Object[] lastValue = {null};
        return proxy(ResultSet.class, (method, args) -> {
            if (method.equals("next")) {
                rowIndex[0]++;
                return rowIndex[0] < rows.size();
            }
            if (method.equals("wasNull")) {
                return lastValue[0] == null;
            }
            if (method.equals("close")) {
                return null;
            }
            if (method.equals("isWrapperFor")) {
                return false;
            }
            lastValue[0] = rows.get(rowIndex[0]).get((int) args[0] - 1);
            return switch (method) {
                case "getString" -> lastValue[0] == null ? null : lastValue[0].toString();
                case "getInt" -> lastValue[0] == null ? 0 : ((Number) lastValue[0]).intValue();
                case "getBoolean" -> lastValue[0] != null && (Boolean) lastValue[0];
                default -> throw new UnsupportedOperationException(method);
            };
        });
    }

    private interface Handler
    {
        Object handle(String method, Object[] args)
                throws Exception;
    }

    private static <T> T proxy(Class<T> type, Handler handler)
    {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (_, method, args) -> handler.handle(method.getName(), args == null ? new Object[0] : Arrays.copyOf(args, args.length))));
    }
}
