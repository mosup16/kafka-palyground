package kafka.playground;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

public class KafkaConsumer<K, V> {

    private final String sourceTopic;
    private final MessageHandler<K, V> messageHandler;

    public KafkaConsumer(String sourceTopic, MessageHandler<K, V> messageHandler) {
        this.sourceTopic = sourceTopic;
        this.messageHandler = messageHandler;
    }

    private Runnable consumerRunnable(Properties props) {
        return () -> {
            try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(props)) {
                consumer.subscribe(Set.of(this.sourceTopic));
                while (true)
                    consumer.poll(Duration.ofMillis(1000))
                            .forEach(this.messageHandler::handle);
            }
        };
    }

    public void startOnNewThread(Properties props) {
        new Thread(consumerRunnable(props)).start();
    }

}
