package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectOverview;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.InspectSessionInfo;
import io.cobble.flink.inspect.InspectTarget;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.ScanRequest;

import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class InspectSessionRegistryTest {
    @Test
    void deleteWaitsForActiveLeaseAndClosesExactlyOnce() {
        FakeSession session = new FakeSession();
        InspectSessionRegistry registry = new InspectSessionRegistry(2, 10_000);
        String id = registry.add(session);
        InspectSessionRegistry.Lease lease = registry.acquire(id);

        assertTrue(id.length() >= 22);
        assertTrue(registry.delete(id));
        assertEquals(0, session.closes.get());
        assertThrows(
                InspectSessionRegistry.SessionMissingException.class, () -> registry.acquire(id));

        lease.close();
        lease.close();
        registry.close();
        assertEquals(1, session.closes.get());
    }

    @Test
    void activeLeaseIsNotIdleAndReleaseRestartsTimeout() {
        AtomicLong clock = new AtomicLong();
        InspectSessionRegistry registry =
                new InspectSessionRegistry(2, 100, clock::get, new SecureRandom());
        FakeSession first = new FakeSession();
        FakeSession second = new FakeSession();
        String firstId = registry.add(first);
        String secondId = registry.add(second);
        assertNotEquals(firstId, secondId);

        InspectSessionRegistry.Lease lease = registry.acquire(firstId);
        clock.set(101_000_000L);
        registry.expireIdle();
        assertEquals(1, registry.size());
        assertEquals(0, first.closes.get());
        assertEquals(1, second.closes.get());

        InspectSessionRegistry.Lease fresh = registry.acquire(firstId);
        registry.expireIdle();
        assertEquals(1, registry.size());
        fresh.close();
        lease.close();
        registry.expireIdle();
        assertEquals(1, registry.size());
        assertEquals(0, first.closes.get());

        clock.set(202_000_000L);
        registry.expireIdle();
        assertEquals(0, registry.size());
        assertEquals(1, first.closes.get());
    }

    @Test
    void freshAcquireCannotBeRetiredByAnExpirySweepUsingTheSameClockTick() {
        AtomicLong clock = new AtomicLong();
        InspectSessionRegistry registry =
                new InspectSessionRegistry(1, 100, clock::get, new SecureRandom());
        FakeSession session = new FakeSession();
        String id = registry.add(session);

        clock.set(99_000_000L);
        InspectSessionRegistry.Lease fresh = registry.acquire(id);
        clock.set(100_000_000L);
        registry.expireIdle();

        assertEquals(1, registry.size());
        assertEquals(0, session.closes.get());
        fresh.close();
        registry.expireIdle();
        assertEquals(1, registry.size());
        assertEquals(0, session.closes.get());

        clock.set(200_000_000L);
        registry.expireIdle();
        assertEquals(0, registry.size());
        assertEquals(1, session.closes.get());
    }

    @Test
    void closeContinuesAfterOneSessionCloseFails() {
        InspectSessionRegistry registry = new InspectSessionRegistry(2, 100);
        FakeSession failing = new FakeSession(true);
        FakeSession healthy = new FakeSession();
        registry.add(failing);
        registry.add(healthy);

        assertThrows(RuntimeException.class, registry::close);
        assertEquals(1, failing.closes.get());
        assertEquals(1, healthy.closes.get());
        assertEquals(0, registry.size());
    }

    private static final class FakeSession implements InspectSession {
        private final AtomicInteger closes = new AtomicInteger();
        private final boolean failOnClose;

        private FakeSession() {
            this(false);
        }

        private FakeSession(boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public InspectCatalog catalog() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InspectSessionInfo info() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InspectTarget> targets() {
            return Collections.emptyList();
        }

        @Override
        public InspectOverview overview() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InspectPage scan(ScanRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LookupResult lookup(LookupRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            if (failOnClose) {
                throw new RuntimeException("expected close failure");
            }
        }
    }
}
