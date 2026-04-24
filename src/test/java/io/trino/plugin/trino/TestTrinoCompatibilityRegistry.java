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

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoCompatibilityRegistry
{
    private final TrinoCompatibilityRegistry registry = new TrinoCompatibilityRegistry();
    private final TrinoRemoteCapabilities capabilities = TrinoRemoteCapabilities.forTesting(Set.of("regexp_like", "count"));

    @Test
    void testAllowedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), capabilities)).isTrue();
    }

    @Test
    void testDeniedFunction()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("remote_only_custom_function"), capabilities)).isFalse();
    }

    @Test
    void testAllowlistedFunctionRequiresRemoteMetadata()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("regexp_like"), TrinoRemoteCapabilities.forTesting(Set.of()))).isFalse();
    }

    @Test
    void testSessionSensitiveFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(SESSION, call("current_timestamp"), capabilities)).isFalse();
    }

    @Test
    void testQualifiedFunctionDenied()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, new FunctionName(Optional.of(new CatalogSchemaName("memory", "default")), "regexp_like"), List.of()),
                capabilities)).isFalse();
    }

    @Test
    void testStandardOperatorDoesNotRequireRemoteFunctionMetadata()
    {
        assertThat(registry.isFunctionSupported(
                SESSION,
                new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of()),
                TrinoRemoteCapabilities.forTesting(Set.of()))).isTrue();
    }

    @Test
    void testVersionGate()
    {
        assertThat(registry.isVersionAtLeast(Optional.of("477"), 477)).isTrue();
        assertThat(registry.isVersionAtLeast(Optional.of("476-SNAPSHOT"), 477)).isFalse();
        assertThat(registry.isVersionAtLeast(Optional.empty(), 477)).isTrue();
    }

    private static Call call(String functionName)
    {
        return new Call(BOOLEAN, new FunctionName(functionName), List.of());
    }
}
