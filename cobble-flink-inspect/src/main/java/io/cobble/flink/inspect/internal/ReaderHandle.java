package io.cobble.flink.inspect.internal;

import io.cobble.Reader;

import java.io.File;
import java.util.List;

public final class ReaderHandle {
    public final Reader reader;
    public final List<File> temporaryDirectories;

    public ReaderHandle(Reader reader, List<File> temporaryDirectories) {
        this.reader = reader;
        this.temporaryDirectories = temporaryDirectories;
    }
}
