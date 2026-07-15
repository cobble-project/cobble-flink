package io.cobble.flink.inspect;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.inspect.internal.CheckpointEntry;
import io.cobble.flink.inspect.internal.FlinkInspectFileSystems;
import io.cobble.flink.inspect.internal.InspectCatalogDiscovery;
import io.cobble.flink.inspect.internal.InspectInputException;
import io.cobble.flink.inspect.internal.InspectSessions;
import io.cobble.flink.inspect.internal.MonitorReaderSession;
import io.cobble.flink.inspect.internal.OperatorEntry;
import io.cobble.flink.inspect.internal.UserClasspath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Entry point for read-only Cobble Flink inspection. */
public final class CobbleInspectClient implements AutoCloseable {
    private final CobbleConnectorStorageOptions storageOptions;
    private final UserClasspath userClasspath;
    private final int totalBuckets;
    private final List<InspectSession> sessions = new ArrayList<>();
    private volatile boolean closed;

    private CobbleInspectClient(
            CobbleConnectorStorageOptions storageOptions,
            List<String> userClasspathEntries,
            int totalBuckets,
            String flinkConfigPath) {
        this.storageOptions = storageOptions;
        FlinkInspectFileSystems.initialize(flinkConfigPath);
        this.userClasspath = UserClasspath.create(userClasspathEntries);
        this.totalBuckets = totalBuckets;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Opens a pinned session for a normal Cobble data-source root. */
    public InspectSession openDataSource(String sourceRoot) {
        return open(InspectSelection.latest(sourceRoot, null));
    }

    public InspectCatalog discover(String sourceRoot) {
        ensureOpen();
        try {
            InspectCatalogDiscovery.Result result =
                    InspectCatalogDiscovery.discover(sourceRoot, storageOptions);
            return toCatalog(result.sourceKind, result.rootDirectory, result.checkpoints);
        } catch (InspectInputException e) {
            throw new InspectException(InspectErrorCode.UNREADABLE, e.getMessage(), e);
        }
    }

    public InspectSession open(InspectSelection selection) {
        ensureOpen();
        if (selection == null
                || selection.sourcePath() == null
                || selection.sourcePath().trim().isEmpty()) {
            throw new InspectException(
                    InspectErrorCode.INVALID_INPUT, "InspectSelection must include a source path");
        }
        InspectCatalogDiscovery.Result discovery;
        try {
            discovery = InspectCatalogDiscovery.discover(selection.sourcePath(), storageOptions);
        } catch (InspectInputException e) {
            throw new InspectException(InspectErrorCode.UNREADABLE, e.getMessage(), e);
        }
        if (discovery.checkpoints.isEmpty()) {
            throw new InspectException(
                    InspectErrorCode.NOT_FOUND, "No inspectable snapshots found");
        }
        if (selection.latest()) {
            RuntimeException firstFailure = null;
            for (CheckpointEntry checkpoint : discovery.checkpoints) {
                try {
                    return openSession(selection, discovery, checkpoint);
                } catch (RuntimeException error) {
                    if (firstFailure == null) {
                        firstFailure = error;
                    }
                }
            }
            throw firstFailure == null
                    ? new InspectException(
                            InspectErrorCode.NOT_FOUND, "No readable snapshots found")
                    : firstFailure;
        }
        return openSession(
                selection,
                discovery,
                findCheckpoint(discovery.checkpoints, selection.checkpointId()));
    }

    private InspectSession openSession(
            InspectSelection selection,
            InspectCatalogDiscovery.Result discovery,
            CheckpointEntry checkpoint) {
        final OperatorEntry operator;
        final MonitorReaderSession readerSession;
        try {
            operator =
                    selection.operatorId() == null
                            ? checkpoint.defaultOperator()
                            : checkpoint.findOperator(selection.operatorId());
            readerSession =
                    MonitorReaderSession.open(
                            totalBuckets,
                            storageOptions,
                            discovery.sourceKind,
                            checkpoint,
                            operator);
        } catch (InspectInputException error) {
            throw new InspectException(InspectErrorCode.UNREADABLE, error.getMessage(), error);
        }
        InspectSelection pinned =
                new InspectSelection(
                        selection.sourcePath(), checkpoint.id, operator.operatorId, false);
        InspectSession session = null;
        try {
            session =
                    InspectSessions.open(
                            toCatalog(
                                    discovery.sourceKind,
                                    discovery.rootDirectory,
                                    discovery.checkpoints),
                            pinned,
                            readerSession,
                            discovery.sourceKind,
                            discovery.rootDirectory,
                            checkpoint,
                            operator,
                            storageOptions,
                            userClasspath.classLoader(),
                            totalBuckets);
            InspectSession managed = new ManagedSession(session);
            synchronized (sessions) {
                ensureOpen();
                sessions.add(managed);
            }
            return managed;
        } catch (RuntimeException error) {
            if (session == null) {
                readerSession.close();
            } else {
                session.close();
            }
            throw error;
        }
    }

    public ClassLoader userClassLoader() {
        ensureOpen();
        return userClasspath.classLoader();
    }

    public List<String> userClasspathEntries() {
        ensureOpen();
        return userClasspath.entries();
    }

    @Override
    public void close() {
        List<InspectSession> openSessions;
        synchronized (sessions) {
            if (closed) {
                return;
            }
            closed = true;
            openSessions = new ArrayList<>(sessions);
            sessions.clear();
        }
        RuntimeException failure = null;
        for (InspectSession session : openSessions) {
            try {
                session.close();
            } catch (RuntimeException error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        try {
            userClasspath.close();
        } catch (RuntimeException error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new InspectException(InspectErrorCode.CLOSED, "CobbleInspectClient is closed");
        }
    }

    private static InspectCatalog toCatalog(
            String sourceKind, String rootDirectory, List<CheckpointEntry> entries) {
        List<CheckpointInfo> checkpoints = new ArrayList<>(entries.size());
        for (CheckpointEntry entry : entries) {
            List<OperatorInfo> operators = new ArrayList<>(entry.operators.size());
            for (OperatorEntry operator : entry.operators) {
                operators.add(
                        new OperatorInfo(
                                operator.operatorId,
                                operator.globalSnapshotLayout,
                                operator.embeddedCheckpoint != null,
                                new ArrayList<>(operator.readerVolumeDirectories)));
            }
            checkpoints.add(new CheckpointInfo(entry.id, entry.directory, operators));
        }
        return new InspectCatalog(sourceKind, rootDirectory, checkpoints);
    }

    private static CheckpointEntry findCheckpoint(
            List<CheckpointEntry> entries, long checkpointId) {
        for (CheckpointEntry entry : entries) {
            if (entry.id == checkpointId) {
                return entry;
            }
        }
        throw new InspectException(
                InspectErrorCode.NOT_FOUND, "Unknown checkpoint id " + checkpointId);
    }

    private final class ManagedSession implements InspectSession {
        private final InspectSession delegate;
        private boolean sessionClosed;

        private ManagedSession(InspectSession delegate) {
            this.delegate = delegate;
        }

        @Override
        public InspectCatalog catalog() {
            return delegate.catalog();
        }

        @Override
        public InspectSessionInfo info() {
            return delegate.info();
        }

        @Override
        public List<InspectTarget> targets() {
            return delegate.targets();
        }

        @Override
        public InspectOverview overview() {
            return delegate.overview();
        }

        @Override
        public InspectPage scan(ScanRequest request) {
            return delegate.scan(request);
        }

        @Override
        public LookupResult lookup(LookupRequest request) {
            return delegate.lookup(request);
        }

        @Override
        public void close() {
            synchronized (this) {
                if (sessionClosed) {
                    return;
                }
                sessionClosed = true;
            }
            try {
                delegate.close();
            } finally {
                synchronized (sessions) {
                    sessions.remove(this);
                }
            }
        }
    }

    /** Builder for a client whose resources are owned by {@link #close()}. */
    public static final class Builder {
        private CobbleConnectorStorageOptions storageOptions =
                CobbleConnectorStorageOptions.empty();
        private List<String> userClasspathEntries = Collections.emptyList();
        private int totalBuckets = 32768;
        private String flinkConfigPath;

        public Builder storageOptions(CobbleConnectorStorageOptions storageOptions) {
            this.storageOptions =
                    storageOptions == null ? CobbleConnectorStorageOptions.empty() : storageOptions;
            return this;
        }

        public Builder userClasspathEntries(List<String> userClasspathEntries) {
            this.userClasspathEntries =
                    userClasspathEntries == null
                            ? Collections.emptyList()
                            : new ArrayList<>(userClasspathEntries);
            return this;
        }

        public Builder userJars(List<String> userJars) {
            return userClasspathEntries(userJars);
        }

        public Builder flinkConfigPath(String flinkConfigPath) {
            this.flinkConfigPath = flinkConfigPath;
            return this;
        }

        public Builder totalBuckets(int totalBuckets) {
            if (totalBuckets <= 0) {
                throw new IllegalArgumentException("totalBuckets must be positive");
            }
            this.totalBuckets = totalBuckets;
            return this;
        }

        public CobbleInspectClient build() {
            return new CobbleInspectClient(
                    storageOptions, userClasspathEntries, totalBuckets, flinkConfigPath);
        }
    }
}
