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

import io.airlift.log.Logger;
import io.trino.plugin.trino.TrinoRemoteCapabilities.Probe;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Keeps what the connector read from the remote server, and checks at an interval that
 * the remote server is still the server that it read from.
 * <p>
 * A check reads the remote version and runs the evaluation probe. The capabilities of the
 * last check stay when the same server process returns the same version and probe values.
 * Otherwise the connector loads them again. Between two checks the connector plans with
 * the capabilities of the last check. When the remote server takes longer than the
 * interval to restart, every query that the connector plans after the restart uses what
 * the restarted server returned. When a host name moves to another running server, the
 * connector can plan for the server before the move until the interval ends. A query
 * that was planned before a change and reads from the remote server after it runs as
 * planned.
 * <p>
 * A check that fails returns the capabilities of an unknown server, and the next check
 * waits for the backoff. One check runs at a time. The queries that are planned meanwhile
 * wait for it and use its result.
 */
final class TrinoRemoteCapabilitiesCache
{
    private static final Logger log = Logger.get(TrinoRemoteCapabilitiesCache.class);

    interface ConnectionSupplier
    {
        Connection open()
                throws SQLException;
    }

    interface ProbeReader
    {
        Probe read(Connection connection)
                throws SQLException;
    }

    interface Loader
    {
        TrinoRemoteCapabilities load(Connection connection, Probe probe)
                throws SQLException;
    }

    interface Listener
    {
        /**
         * Called when a check stored capabilities that differ from the capabilities of the
         * check before it. {@code previous} is empty for the first check.
         */
        void changed(Optional<TrinoRemoteCapabilities> previous, TrinoRemoteCapabilities current);
    }

    private record Entry(Probe probe, TrinoRemoteCapabilities capabilities, long checkedNanos) {}

    private final ProbeReader probeReader;
    private final Loader loader;
    private final Listener listener;
    private final long checkIntervalNanos;
    private final FailureBackoff failureBackoff;
    private final LongSupplier nanoTicker;
    // Written only by check()
    private volatile Entry lastCheck;

    static TrinoRemoteCapabilitiesCache create(String localVersion, Duration checkInterval, Duration failureBackoff, Listener listener)
    {
        requireNonNull(localVersion, "localVersion is null");
        return new TrinoRemoteCapabilitiesCache(
                Probe::read,
                (connection, probe) -> TrinoRemoteCapabilities.load(connection, localVersion, probe),
                listener,
                checkInterval,
                failureBackoff,
                System::nanoTime);
    }

    TrinoRemoteCapabilitiesCache(
            ProbeReader probeReader,
            Loader loader,
            Listener listener,
            Duration checkInterval,
            Duration failureBackoff,
            LongSupplier nanoTicker)
    {
        this.probeReader = requireNonNull(probeReader, "probeReader is null");
        this.loader = requireNonNull(loader, "loader is null");
        this.listener = requireNonNull(listener, "listener is null");
        this.checkIntervalNanos = checkInterval.toNanos();
        this.nanoTicker = requireNonNull(nanoTicker, "nanoTicker is null");
        this.failureBackoff = new FailureBackoff(failureBackoff, nanoTicker);
    }

    TrinoRemoteCapabilities get(ConnectionSupplier connections)
    {
        Entry last = lastCheck;
        if (isCurrent(last)) {
            return last.capabilities();
        }
        if (!failureBackoff.shouldAttempt()) {
            return TrinoRemoteCapabilities.unavailable();
        }
        return check(connections);
    }

    // One check at a time. Concurrent checks would each open a connection and wait for the
    // same remote server, so a query that is planned during a check waits for that check.
    private synchronized TrinoRemoteCapabilities check(ConnectionSupplier connections)
    {
        Entry last = lastCheck;
        if (isCurrent(last)) {
            return last.capabilities();
        }
        if (!failureBackoff.shouldAttempt()) {
            return TrinoRemoteCapabilities.unavailable();
        }

        Entry checked;
        try (Connection connection = connections.open()) {
            Probe probe = probeReader.read(connection);
            // The interval starts when the server answered the probe. The other queries
            // of a load can take long, and the server can change while they run.
            long checkedNanos = nanoTicker.getAsLong();
            TrinoRemoteCapabilities capabilities = last != null && last.probe().isSameServer(probe)
                    ? last.capabilities()
                    : loader.load(connection, probe);
            checked = new Entry(probe, capabilities, checkedNanos);
        }
        catch (SQLException | RuntimeException e) {
            // A failure must not disable pushdown for longer than the backoff, and the
            // capabilities of the last check do not hold for a server that did not answer
            failureBackoff.recordFailure();
            log.warn(e, "Unable to read the capabilities of the remote Trino server, so functions, casts, and arithmetic run locally until the next check");
            return TrinoRemoteCapabilities.unavailable();
        }
        lastCheck = checked;
        if (last == null || !last.capabilities().equals(checked.capabilities())) {
            listener.changed(Optional.ofNullable(last).map(Entry::capabilities), checked.capabilities());
        }
        return checked.capabilities();
    }

    private boolean isCurrent(Entry check)
    {
        return check != null && nanoTicker.getAsLong() - check.checkedNanos() < checkIntervalNanos;
    }
}
