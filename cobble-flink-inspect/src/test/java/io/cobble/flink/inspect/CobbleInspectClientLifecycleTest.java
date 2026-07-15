package io.cobble.flink.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class CobbleInspectClientLifecycleTest {

    @Test
    void closesClientAndRejectsFurtherClassLoaderAccess() {
        CobbleInspectClient client = CobbleInspectClient.builder().build();
        client.close();

        InspectException error = assertThrows(InspectException.class, client::userClassLoader);
        assertEquals(InspectErrorCode.CLOSED, error.errorCode());
    }

    @Test
    void publicCollectionModelsDefensivelyCopyInputs() {
        List<String> volumes = new ArrayList<>();
        volumes.add("file:///one");
        OperatorInfo operator = new OperatorInfo("operator", true, false, volumes);
        volumes.add("file:///two");

        List<OperatorInfo> operators = new ArrayList<>();
        operators.add(operator);
        CheckpointInfo checkpoint = new CheckpointInfo(3L, "file:///chk-3", operators);
        operators.clear();

        InspectCatalog catalog =
                new InspectCatalog(
                        "checkpoint", "file:///root", Collections.singletonList(checkpoint));
        assertEquals(1, catalog.checkpoints().size());
        assertEquals(Collections.singletonList("file:///one"), operator.readerVolumeDirectories());
        assertThrows(
                UnsupportedOperationException.class, () -> catalog.checkpoints().add(checkpoint));
    }

    @Test
    void rawBytesHasDefensiveValueSemantics() {
        byte[] bytes = new byte[] {1, 2, 3};
        RawBytes raw = new RawBytes(bytes);
        bytes[0] = 9;

        assertEquals(new RawBytes(new byte[] {1, 2, 3}), raw);
        assertEquals(new RawBytes(new byte[] {1, 2, 3}).hashCode(), raw.hashCode());
        assertNotEquals(new RawBytes(new byte[] {1, 2}), raw);

        byte[] exposed = raw.value();
        exposed[0] = 9;
        assertEquals(new RawBytes(new byte[] {1, 2, 3}), raw);
    }
}
