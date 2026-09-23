package io.cobble.flink.common.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.table.BucketHash;
import io.cobble.table.DataField;
import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalTypes;
import io.cobble.table.TableSchema;
import io.cobble.table.Value;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class CobbleTableRowConverterTest {

    @Test
    void cachedBucketEncoderPreservesStaticRoutingForReorderedBucketKeys() {
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(0, "id", LogicalTypes.int64()),
                                new DataField(1, "payload", LogicalTypes.string()),
                                new DataField(2, "partition", LogicalTypes.int32())),
                        Arrays.asList(2L, 0L),
                        Arrays.asList(2L, 0L));
        List<Value> first = Arrays.asList(Value.int64(101), Value.string("one"), Value.int32(7));
        List<Value> second = Arrays.asList(Value.int64(202), Value.string("two"), Value.int32(11));

        CobbleTableRowConverter.BucketEncoder encoder =
                CobbleTableRowConverter.bucketEncoder(schema, 128);
        assertEquals(expectedBucket(first), encoder.bucket(first));
        assertEquals(expectedBucket(second), encoder.bucket(second));
        assertEquals(expectedBucket(first), CobbleTableRowConverter.bucket(schema, first, 128));
    }

    private static int expectedBucket(List<Value> row) {
        return new BucketHash(128)
                .bucket(
                        KeyCodec.encode(
                                Arrays.asList(LogicalTypes.int32(), LogicalTypes.int64()),
                                Arrays.asList(row.get(2), row.get(0))));
    }
}
