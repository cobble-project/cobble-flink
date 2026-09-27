package io.cobble.flink.table;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableKeyBuilder;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadSession;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Lookup function that resolves Cobble rows by PRIMARY KEY from a configured snapshot. */
public final class CobbleLookupFunction extends LookupFunction {

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final int[] lookupKeyPositions;
    private transient CobbleSinkLookupReadProvider provider;
    private transient TableReadSession<RowData, RowData> session;
    private transient CobbleConnectorMetrics.LookupMetrics metrics;

    CobbleLookupFunction(
            CobbleDynamicTableSource.SerializableConfig config, int[] lookupKeyPositions) {
        this.config = config;
        this.lookupKeyPositions = Arrays.copyOf(lookupKeyPositions, lookupKeyPositions.length);
    }

    @Override
    public void open(FunctionContext context) {
        this.metrics = lookupMetrics(context);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        metrics.request();
        try {
            if (!ensureReaderLoaded()) {
                metrics.miss();
                return Collections.emptyList();
            }
            if (config.isStreamingLatest()) provider.refresh();
            Collection<TableReadEntry<RowData>> rows = session.lookup(keyRow);
            if (rows.isEmpty()) {
                metrics.miss();
                return Collections.emptyList();
            }
            TableReadEntry<RowData> row = rows.iterator().next();
            metrics.hit(row.physicalBytes());
            return Collections.singletonList(row.value());
        } catch (IOException e) {
            metrics.error();
            throw e;
        } catch (RuntimeException e) {
            metrics.error();
            throw e;
        } catch (Exception e) {
            metrics.error();
            throw new IOException("Cobble lookup failed.", e);
        }
    }

    @Override
    public void close() {
        if (session != null) session.close();
        session = null;
        if (provider != null) provider.close();
        provider = null;
    }

    private boolean ensureReaderLoaded() throws IOException {
        if (session != null) {
            return true;
        }
        CobbleLoader.ensureCobbleLoaded();
        GlobalSnapshot initialSnapshot = CobbleSourceRuntime.loadConfiguredSnapshot(config);
        if (initialSnapshot == null) {
            return false;
        }
        CobbleSinkLookupReadProvider openedProvider =
                new CobbleSinkLookupReadProvider(config, initialSnapshot, lookupKeyPositions);
        try {
            this.session = openedProvider.open();
            this.provider = openedProvider;
        } catch (Exception e) {
            openedProvider.close();
            throw new IOException("Failed to open Cobble lookup read provider.", e);
        } catch (LinkageError e) {
            openedProvider.close();
            throw e;
        }
        return true;
    }

    private static CobbleConnectorMetrics.LookupMetrics lookupMetrics(FunctionContext context) {
        return CobbleConnectorMetrics.lookup(context == null ? null : context.getMetricGroup());
    }

    static final class RuntimeLookupKeyConverter {
        private final List<RuntimeLookupFieldConverter> fields;

        RuntimeLookupKeyConverter(
                List<CobbleDynamicTableSource.SerializableField> keyFields,
                int[] lookupKeyPositions) {
            this.fields = new ArrayList<>(keyFields.size());
            for (int i = 0; i < keyFields.size(); i++) {
                LogicalType type =
                        LogicalTypeParser.parse(
                                keyFields.get(i).logicalType,
                                CobbleLookupFunction.class.getClassLoader());
                this.fields.add(
                        new RuntimeLookupFieldConverter(
                                keyFields.get(i), lookupKeyPositions[i], type));
            }
        }

        void pushInto(RowData row, TableKeyBuilder builder) {
            for (RuntimeLookupFieldConverter field : fields) {
                builder.push(field.convertRequired(row));
            }
        }
    }

    private static final class RuntimeLookupFieldConverter {
        private final String name;
        private final LogicalType logicalType;
        private final RowData.FieldGetter fieldGetter;

        private RuntimeLookupFieldConverter(
                CobbleDynamicTableSource.SerializableField field,
                int lookupKeyPosition,
                LogicalType logicalType) {
            this.name = field.name;
            this.logicalType = logicalType;
            this.fieldGetter = RowData.createFieldGetter(logicalType, lookupKeyPosition);
        }

        private Value convertRequired(RowData row) {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Lookup key column " + name + " must not be null.");
            }
            return CobbleTableRowConverter.toValue(logicalType, value);
        }
    }
}
