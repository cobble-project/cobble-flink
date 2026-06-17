package io.cobble.flink.common.inspect;

/** Shared row-key layout rules for Cobble Flink state and monitor inspect metadata. */
public final class StateRowKeyLayout {

    private StateRowKeyLayout() {}

    /** key+namespace has two parts; one length field is needed only when both are variable. */
    public static boolean shouldStoreKeyLengthForKeyNamespace(int keyLength, int namespaceLength) {
        return keyLength < 0 && namespaceLength < 0;
    }

    /**
     * Map row keys have three variable-length candidates: key, namespace, and map user key. The row
     * layout only stores {@code unknownParts - 1} trailing lengths; this method decides whether key
     * length is one of those persisted lengths.
     */
    public static boolean shouldStoreMapKeyLength(
            int keyLength, int namespaceLength, int userKeyLength) {
        int unknownParts = unknownParts(keyLength, namespaceLength, userKeyLength);
        return keyLength < 0 && unknownParts >= 2;
    }

    /**
     * Map row keys have three variable-length candidates: key, namespace, and map user key. The row
     * layout only stores {@code unknownParts - 1} trailing lengths; this method decides whether
     * namespace length is one of those persisted lengths.
     */
    public static boolean shouldStoreMapNamespaceLength(
            int keyLength, int namespaceLength, int userKeyLength) {
        int unknownParts = unknownParts(keyLength, namespaceLength, userKeyLength);
        return namespaceLength < 0 && unknownParts >= 2;
    }

    private static int unknownParts(int keyLength, int namespaceLength, int userKeyLength) {
        return (keyLength < 0 ? 1 : 0)
                + (namespaceLength < 0 ? 1 : 0)
                + (userKeyLength < 0 ? 1 : 0);
    }
}
