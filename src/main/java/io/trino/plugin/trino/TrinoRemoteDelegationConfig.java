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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

public class TrinoRemoteDelegationConfig
{
    private boolean enabled = true;
    private TrinoRemoteDelegationMode mode = TrinoRemoteDelegationMode.AUTO;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("trino.remote-delegation.enabled")
    @LegacyConfig("remote-delegation.enabled")
    @ConfigDescription("Enable Trino-native remote SQL delegation")
    public TrinoRemoteDelegationConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public TrinoRemoteDelegationMode getMode()
    {
        return mode;
    }

    @Config("trino.remote-delegation.mode")
    @LegacyConfig("remote-delegation.mode")
    @ConfigDescription("Remote delegation mode: AUTO, OFF, or STRICT")
    public TrinoRemoteDelegationConfig setMode(TrinoRemoteDelegationMode mode)
    {
        this.mode = mode;
        return this;
    }
}
