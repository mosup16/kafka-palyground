package kafka.playground;

interface MessageSupplier<K, V> {
    KeyValuePair<K, V> get();
}
