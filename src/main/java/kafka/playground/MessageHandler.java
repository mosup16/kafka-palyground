package kafka.playground;

import org.apache.kafka.clients.consumer.ConsumerRecord;

interface MessageHandler<K, V> {
    void handle(ConsumerRecord<K, V> msg);
}
