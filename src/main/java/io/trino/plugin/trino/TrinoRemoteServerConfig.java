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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TrinoRemoteServerConfig
{
    private Duration checkInterval = new Duration(10, SECONDS);

    @NotNull
    @MinDuration("1s")
    public Duration getCheckInterval()
    {
        return checkInterval;
    }

    @Config("remote-server.check-interval")
    @ConfigDescription("Time after which the connector checks that the remote server did not restart or change before it plans the next query")
    public TrinoRemoteServerConfig setCheckInterval(Duration checkInterval)
    {
        this.checkInterval = checkInterval;
        return this;
    }
}
