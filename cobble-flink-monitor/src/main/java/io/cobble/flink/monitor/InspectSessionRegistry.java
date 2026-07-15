package io.cobble.flink.monitor;

import io.cobble.flink.inspect.InspectSession;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** Owns SDK sessions and prevents expiry/delete from closing an in-flight request. */
final class InspectSessionRegistry implements AutoCloseable {
    private static final int SESSION_ID_BYTES = 18;

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final int maxSessions;
    private final long idleTimeoutNanos;
    private final LongSupplier nanoClock;
    private final SecureRandom random;
    private boolean closed;

    InspectSessionRegistry(int maxSessions, long idleTimeoutMillis) {
        this(maxSessions, idleTimeoutMillis, System::nanoTime, new SecureRandom());
    }

    InspectSessionRegistry(
            int maxSessions, long idleTimeoutMillis, LongSupplier nanoClock, SecureRandom random) {
        if (maxSessions <= 0 || idleTimeoutMillis <= 0) {
            throw new IllegalArgumentException("session limits must be positive");
        }
        this.maxSessions = maxSessions;
        this.idleTimeoutNanos = idleTimeoutMillis * 1_000_000L;
        this.nanoClock = nanoClock;
        this.random = random;
    }

    synchronized String add(InspectSession session) {
        if (closed) {
            IllegalStateException error = new IllegalStateException("session registry is closed");
            closeRejectedSession(session, error);
            throw error;
        }
        try {
            expireIdle();
        } catch (RuntimeException error) {
            closeRejectedSession(session, error);
            throw error;
        }
        if (entries.size() >= maxSessions) {
            SessionLimitException error =
                    new SessionLimitException("Maximum inspect sessions reached");
            closeRejectedSession(session, error);
            throw error;
        }
        String id;
        do {
            byte[] value = new byte[SESSION_ID_BYTES];
            random.nextBytes(value);
            id = Base64.getUrlEncoder().withoutPadding().encodeToString(value);
        } while (entries.containsKey(id));
        entries.put(id, new Entry(id, session, nanoClock.getAsLong()));
        return id;
    }

    Lease acquire(String id) {
        Entry entry = entries.get(id);
        if (entry == null) {
            throw new SessionMissingException("Unknown or expired inspect session");
        }
        long now = nanoClock.getAsLong();
        Lease lease = entry.acquire(now, idleTimeoutNanos, nanoClock);
        if (lease == null) {
            entries.remove(id, entry);
            throw new SessionMissingException("Unknown or expired inspect session");
        }
        return lease;
    }

    boolean delete(String id) {
        Entry entry = entries.remove(id);
        if (entry == null) {
            return false;
        }
        entry.retire();
        return true;
    }

    void expireIdle() {
        long now = nanoClock.getAsLong();
        RuntimeException failure = null;
        for (Map.Entry<String, Entry> item : entries.entrySet()) {
            try {
                if (item.getValue().retireIfExpired(now, idleTimeoutNanos)) {
                    entries.remove(item.getKey(), item.getValue());
                }
            } catch (RuntimeException error) {
                entries.remove(item.getKey(), item.getValue());
                failure = accumulate(failure, error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    int size() {
        return entries.size();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        for (Entry entry : new ArrayList<>(entries.values())) {
            try {
                entry.retire();
            } catch (RuntimeException error) {
                failure = accumulate(failure, error);
            }
        }
        entries.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException accumulate(RuntimeException first, RuntimeException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private static void closeRejectedSession(InspectSession session, RuntimeException failure) {
        try {
            session.close();
        } catch (RuntimeException closeError) {
            failure.addSuppressed(closeError);
        }
    }

    static final class Lease implements AutoCloseable {
        private final Entry entry;
        private final LongSupplier nanoClock;
        private boolean closed;

        private Lease(Entry entry, LongSupplier nanoClock) {
            this.entry = entry;
            this.nanoClock = nanoClock;
        }

        InspectSession session() {
            return entry.session;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                entry.release(nanoClock.getAsLong());
            }
        }
    }

    private static final class Entry {
        private final String id;
        private final InspectSession session;
        private long lastAccessNanos;
        private int leases;
        private boolean retiring;
        private boolean closed;

        private Entry(String id, InspectSession session, long now) {
            this.id = id;
            this.session = session;
            this.lastAccessNanos = now;
        }

        private synchronized Lease acquire(long now, long timeout, LongSupplier nanoClock) {
            if (retiring || closed) {
                return null;
            }
            if (leases == 0 && now - lastAccessNanos >= timeout) {
                retiring = true;
                closeIfRetired();
                return null;
            }
            leases++;
            lastAccessNanos = now;
            return new Lease(this, nanoClock);
        }

        private synchronized void release(long now) {
            if (leases <= 0) {
                throw new IllegalStateException("Unbalanced session lease for " + id);
            }
            leases--;
            lastAccessNanos = now;
            closeIfRetired();
        }

        private synchronized boolean retireIfExpired(long now, long timeout) {
            if (retiring || closed || leases > 0 || now - lastAccessNanos < timeout) {
                return false;
            }
            retiring = true;
            closeIfRetired();
            return true;
        }

        private synchronized void retire() {
            retiring = true;
            closeIfRetired();
        }

        private void closeIfRetired() {
            if (retiring && leases == 0 && !closed) {
                closed = true;
                session.close();
            }
        }
    }

    static final class SessionMissingException extends RuntimeException {
        SessionMissingException(String message) {
            super(message);
        }
    }

    static final class SessionLimitException extends RuntimeException {
        private SessionLimitException(String message) {
            super(message);
        }
    }
}
