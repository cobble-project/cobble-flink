package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;

class CobbleConnectorMetricsTest {

    @Test
    void sumsOnlyPresentNativeColumnsWithoutOverflow() {
        assertEquals(
                7L,
                CobbleConnectorMetrics.nativeEntryBytes(
                        new byte[] {1, 2},
                        new byte[][] {new byte[] {3, 4, 5}, null, new byte[] {6, 7}}));
        assertEquals(Long.MAX_VALUE, CobbleConnectorMetrics.saturatingAdd(Long.MAX_VALUE - 1L, 2L));
    }

    @Test
    void registersCustomLookupCountersWithCamelNames() {
        Map<String, Counter> counters = new LinkedHashMap<String, Counter>();
        MetricGroup group =
                (MetricGroup)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {MetricGroup.class},
                                (proxy, method, args) -> {
                                    if ("counter".equals(method.getName()) && args.length == 1) {
                                        String name = (String) args[0];
                                        Counter counter = new TestCounter();
                                        counters.put(name, counter);
                                        return counter;
                                    }
                                    return null;
                                });

        CobbleConnectorMetrics.LookupMetrics metrics = CobbleConnectorMetrics.lookup(group);
        metrics.request();
        metrics.hit(new byte[] {1}, new byte[][] {new byte[] {2, 3}});
        assertEquals(1L, counters.get("cobble.lookupRequestsTotal").getCount());
        assertEquals(1L, counters.get("cobble.lookupHitsTotal").getCount());
        assertEquals(3L, counters.get("cobble.lookupBytesReadTotal").getCount());
        assertTrue(counters.keySet().stream().noneMatch(name -> name.contains("_")));
    }

    private static final class TestCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
