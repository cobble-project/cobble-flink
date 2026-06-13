package io.cobble.flink.monitor;

import io.cobble.Reader;

import java.io.File;
import java.util.List;

final class ReaderHandle {
    final Reader reader;
    final List<File> temporaryDirectories;

    ReaderHandle(Reader reader, List<File> temporaryDirectories) {
        this.reader = reader;
        this.temporaryDirectories = temporaryDirectories;
    }
}
