package kafka.playground;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaDataGeneratorProducer<K, V> {


    private final String sinkTopic;
    private final MessageSupplier<K, V> messageSupplier;

    public KafkaDataGeneratorProducer(String sinkTopic ,MessageSupplier<K, V> messageSupplier) {
        this.sinkTopic = sinkTopic;
        this.messageSupplier = messageSupplier;
    }
    private Runnable producerRunnable(Properties props) {
        return () -> {
            try (var producer = new KafkaProducer<K, V>(props)) {
                var isDone = new AtomicBoolean(false);
                while (!isDone.get()) {
                    KeyValuePair<K, V> msg = messageSupplier.get();
                    var record = new ProducerRecord<>(sinkTopic, msg.key(), msg.value());
                    producer.send(record, (metadata, exception) -> {
                        if (exception == null)
                            log(metadata);
                        else {
                            exception.printStackTrace();
                            isDone.set(true);
                        }
                    });
                }
            }
        };
    }

    private static void log(RecordMetadata metadata) {
        String logEntry = String.format("produce to %s topic on partition number %s with offset %s",
                metadata.topic(), metadata.partition(), metadata.offset());
        System.out.println(logEntry);
    }

    public void startOnNewThread(Properties props){
        new Thread(producerRunnable(props)).start();
    }


}
