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

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoRemoteCapabilitiesCache
{
    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(10);
    private static final Duration FAILURE_BACKOFF = Duration.ofSeconds(10);

    private static final TrinoRemoteCapabilities LOCAL_VERSION = TrinoRemoteCapabilities.forTesting(Set.of("json_parse"));
    private static final TrinoRemoteCapabilities OTHER_VERSION = TrinoRemoteCapabilities.forTestingOtherVersion(Set.of("json_parse"));
    private static final TrinoRemoteCapabilities OTHER_SETTINGS = TrinoRemoteCapabilities.forTestingOtherSettings(Set.of("json_parse"), List.of("deprecated.regex-library"));

    @Test
    void testUsesTheCapabilitiesUntilTheNextCheck()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);

        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(1);
        assertThat(server.loads).isEqualTo(1);

        server.advance(CHECK_INTERVAL.minusNanos(1));
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(1);

        // The same server process answers the next check, so only the probe runs
        server.advance(Duration.ofNanos(1));
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(2);
        assertThat(server.loads).isEqualTo(1);

        // The interval starts again with every check
        server.advance(CHECK_INTERVAL.minusNanos(1));
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(2);
        assertThat(server.changes).containsExactly(LOCAL_VERSION);
    }

    @Test
    void testLoadsAgainAfterTheRemoteServerRestartedWithOtherSettings()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get().evaluatesAsLocal()).isTrue();

        // The restarted server answers before the interval ends only when it restarts
        // faster than the interval
        server.run("484", "bbbbb", List.of(true, false, true), OTHER_SETTINGS);
        assertThat(server.get().evaluatesAsLocal()).isTrue();

        server.advance(CHECK_INTERVAL);
        assertThat(server.get()).isEqualTo(OTHER_SETTINGS);
        assertThat(server.get().evaluatesAsLocal()).isFalse();
        assertThat(server.loads).isEqualTo(2);
        assertThat(server.changes).containsExactly(LOCAL_VERSION, OTHER_SETTINGS);
    }

    @Test
    void testLoadsAgainAfterAnotherVersionAnswers()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get().evaluatesAsLocal()).isTrue();

        server.run("483", "bbbbb", List.of(true, true, true), OTHER_VERSION);
        server.advance(CHECK_INTERVAL);
        assertThat(server.get()).isEqualTo(OTHER_VERSION);
        assertThat(server.get().evaluatesAsLocal()).isFalse();

        // An upgrade to the local version restores the full list
        server.run("484", "ccccc", List.of(true, true, true), LOCAL_VERSION);
        server.advance(CHECK_INTERVAL);
        assertThat(server.get().evaluatesAsLocal()).isTrue();
        assertThat(server.changes).containsExactly(LOCAL_VERSION, OTHER_VERSION, LOCAL_VERSION);
    }

    @Test
    void testLoadsAgainAfterARestartWithTheSameProbeValues()
    {
        // A restart can change the time zone and the function list, which the probe does
        // not read
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);

        server.run("484", "bbbbb", List.of(true, true, true), LOCAL_VERSION);
        server.advance(CHECK_INTERVAL);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.loads).isEqualTo(2);
        // Nothing changed for the connector
        assertThat(server.changes).containsExactly(LOCAL_VERSION);
    }

    @Test
    void testLoadsAgainWhenTheSameProcessReturnsOtherProbeValues()
    {
        // The remote server can set session properties by user
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get().evaluatesAsLocal()).isTrue();

        server.run("484", "aaaaa", List.of(true, true, false), OTHER_SETTINGS);
        server.advance(CHECK_INTERVAL);
        assertThat(server.get().evaluatesAsLocal()).isFalse();
        assertThat(server.loads).isEqualTo(2);
    }

    @Test
    void testLoadsWithEveryCheckWithoutAServerProcess()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", null, List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);

        server.advance(CHECK_INTERVAL);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.loads).isEqualTo(2);
    }

    @Test
    void testFailedCheckReturnsAnUnknownServer()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);

        // The capabilities of the last check do not hold for a server that does not answer
        server.failure = new SQLException("remote unavailable");
        server.advance(CHECK_INTERVAL);
        assertThat(server.get()).isEqualTo(TrinoRemoteCapabilities.unavailable());
        assertThat(server.probes).isEqualTo(2);

        // No check runs during the backoff, also when the server answers again
        server.failure = null;
        server.advance(FAILURE_BACKOFF.minusNanos(1));
        assertThat(server.get()).isEqualTo(TrinoRemoteCapabilities.unavailable());
        assertThat(server.probes).isEqualTo(2);

        // The same server process answers, so the capabilities hold again without a load
        server.advance(Duration.ofNanos(1));
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(3);
        assertThat(server.loads).isEqualTo(1);
    }

    @Test
    void testFailedFirstLoadIsNotKept()
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        server.loadFailure = new SQLException("queue full");

        assertThat(server.get()).isEqualTo(TrinoRemoteCapabilities.unavailable());
        assertThat(server.get()).isEqualTo(TrinoRemoteCapabilities.unavailable());
        assertThat(server.probes).isEqualTo(1);

        server.loadFailure = null;
        server.advance(FAILURE_BACKOFF);
        assertThat(server.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.changes).containsExactly(LOCAL_VERSION);
    }

    @Test
    void testOneCheckRunsAtATime()
            throws Exception
    {
        RemoteServer server = new RemoteServer();
        server.run("484", "aaaaa", List.of(true, true, true), LOCAL_VERSION);
        CountDownLatch probeStarted = new CountDownLatch(1);
        CountDownLatch finishProbe = new CountDownLatch(1);
        server.duringProbe = () -> {
            probeStarted.countDown();
            awaitUninterruptibly(finishProbe);
        };

        AtomicReference<TrinoRemoteCapabilities> first = new AtomicReference<>();
        AtomicReference<TrinoRemoteCapabilities> second = new AtomicReference<>();
        Thread firstQuery = new Thread(() -> first.set(server.get()));
        Thread secondQuery = new Thread(() -> second.set(server.get()));
        try {
            firstQuery.start();
            assertThat(probeStarted.await(10, SECONDS)).isTrue();

            // A query that is planned during the check waits for it
            secondQuery.start();
            assertEventually(() -> assertThat(secondQuery.getState()).isEqualTo(Thread.State.BLOCKED));
        }
        finally {
            finishProbe.countDown();
        }
        firstQuery.join(10_000);
        secondQuery.join(10_000);

        // The second query uses the result of the check that it waited for
        assertThat(first.get()).isEqualTo(LOCAL_VERSION);
        assertThat(second.get()).isEqualTo(LOCAL_VERSION);
        assertThat(server.probes).isEqualTo(1);
        assertThat(server.loads).isEqualTo(1);
    }

    private static final class RemoteServer
    {
        private final AtomicLong nanos = new AtomicLong();
        private final List<TrinoRemoteCapabilities> changes = new ArrayList<>();
        private final TrinoRemoteCapabilitiesCache cache = new TrinoRemoteCapabilitiesCache(
                _ -> probe(),
                (_, _) -> load(),
                (_, current) -> changes.add(current),
                CHECK_INTERVAL,
                FAILURE_BACKOFF,
                nanos::get);

        private Probe probe;
        private TrinoRemoteCapabilities capabilities;
        private SQLException failure;
        private SQLException loadFailure;
        private Runnable duringProbe;
        private int probes;
        private int loads;

        void run(String version, String serverProcess, List<Boolean> evaluation, TrinoRemoteCapabilities capabilities)
        {
            this.probe = new Probe(Optional.of(version), evaluation, Optional.ofNullable(serverProcess));
            this.capabilities = capabilities;
        }

        void advance(Duration duration)
        {
            nanos.addAndGet(duration.toNanos());
        }

        TrinoRemoteCapabilities get()
        {
            // The cache accepts a missing connection, as try-with-resources does
            return cache.get(() -> null);
        }

        private Probe probe()
                throws SQLException
        {
            probes++;
            if (duringProbe != null) {
                duringProbe.run();
            }
            if (failure != null) {
                throw failure;
            }
            return probe;
        }

        private TrinoRemoteCapabilities load()
                throws SQLException
        {
            loads++;
            if (loadFailure != null) {
                throw loadFailure;
            }
            return capabilities;
        }
    }
}
