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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class TrinoRemoteDelegationSessionProperties
        implements SessionPropertiesProvider
{
    static final String REMOTE_DELEGATION_ENABLED = "remote_delegation_enabled";
    static final String REMOTE_DELEGATION_MODE = "remote_delegation_mode";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    TrinoRemoteDelegationSessionProperties(TrinoRemoteDelegationConfig config)
    {
        properties = List.of(
                booleanProperty(
                        REMOTE_DELEGATION_ENABLED,
                        "Enable Trino-native remote SQL delegation",
                        config.isEnabled(),
                        false),
                enumProperty(
                        REMOTE_DELEGATION_MODE,
                        "Remote delegation mode",
                        TrinoRemoteDelegationMode.class,
                        config.getMode(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    static boolean isRemoteDelegationEnabled(ConnectorSession session)
    {
        return session.getProperty(REMOTE_DELEGATION_ENABLED, Boolean.class);
    }

    static TrinoRemoteDelegationMode getRemoteDelegationMode(ConnectorSession session)
    {
        return session.getProperty(REMOTE_DELEGATION_MODE, TrinoRemoteDelegationMode.class);
    }
}
