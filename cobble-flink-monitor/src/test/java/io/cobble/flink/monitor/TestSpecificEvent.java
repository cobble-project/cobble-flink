package io.cobble.flink.monitor;

import io.cobble.flink.inspect.internal.*;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

/**
 * A minimal Avro {@link SpecificRecord} for testing the classless decoder path with real
 * SpecificRecord bytes (as opposed to GenericRecord bytes).
 *
 * <p>The schema is fixed and matches the one used in cross-version protocol tests. The class
 * provides the static {@code SCHEMA$} field that Flink's {@code AvroSerializer} expects when
 * constructed with {@code new AvroSerializer<>(TestSpecificEvent.class)}.
 */
public final class TestSpecificEvent implements SpecificRecord {

    public static final Schema SCHEMA$ =
            Schema.createRecord(
                    "TestSpecificEvent",
                    null,
                    "io.cobble.flink.monitor",
                    false,
                    java.util.Arrays.asList(
                            new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                            new Schema.Field(
                                    "label", Schema.create(Schema.Type.STRING), null, null),
                            new Schema.Field(
                                    "active", Schema.create(Schema.Type.BOOLEAN), null, null)));

    private int id;
    private CharSequence label;
    private boolean active;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public CharSequence getLabel() {
        return label;
    }

    public void setLabel(CharSequence label) {
        this.label = label;
    }

    public boolean getActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                id = (Integer) v;
                break;
            case 1:
                label = (CharSequence) v;
                break;
            case 2:
                active = (Boolean) v;
                break;
            default:
                throw new IndexOutOfBoundsException("Bad field index: " + i);
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return id;
            case 1:
                return label;
            case 2:
                return active;
            default:
                throw new IndexOutOfBoundsException("Bad field index: " + i);
        }
    }
}
