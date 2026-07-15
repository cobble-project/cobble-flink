package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One state, timer, or sink entry shown by an inspection overview. */
public final class InspectOverviewItem {
    private final String id;
    private final String title;
    private final String kind;
    private final String detail;
    private final List<Field> fields;
    private final SourceSqlExample sourceSql;

    public InspectOverviewItem(
            String id,
            String title,
            String kind,
            String detail,
            List<Field> fields,
            SourceSqlExample sourceSql) {
        this.id = Objects.requireNonNull(id, "id");
        this.title = Objects.requireNonNull(title, "title");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.detail = detail;
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<Field>(
                                fields == null ? Collections.<Field>emptyList() : fields));
        this.sourceSql = Objects.requireNonNull(sourceSql, "sourceSql");
    }

    public String id() {
        return id;
    }

    public String title() {
        return title;
    }

    public String kind() {
        return kind;
    }

    public String detail() {
        return detail;
    }

    public List<Field> fields() {
        return fields;
    }

    public SourceSqlExample sourceSql() {
        return sourceSql;
    }

    /** One physical source field and the logical state/sink role it belongs to. */
    public static final class Field {
        private final String role;
        private final String name;
        private final String logicalType;

        public Field(String role, String name, String logicalType) {
            this.role = Objects.requireNonNull(role, "role");
            this.name = Objects.requireNonNull(name, "name");
            this.logicalType = Objects.requireNonNull(logicalType, "logicalType");
        }

        public String role() {
            return role;
        }

        public String name() {
            return name;
        }

        public String logicalType() {
            return logicalType;
        }
    }
}
