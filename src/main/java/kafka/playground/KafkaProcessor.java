package kafka.playground;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class KafkaProcessor<K1, V1, K2, V2> {
    private final String sourceTopic;
    private final String sinkTopic;
    private final Function<KeyValuePair<K1, V1>, KeyValuePair<K2, V2>> messageTransformer;

    public KafkaProcessor(String sourceTopic, String sinkTopic,
                          Function<KeyValuePair<K1, V1>, KeyValuePair<K2, V2>> messageTransformer) {
        this.sourceTopic = sourceTopic;
        this.sinkTopic = sinkTopic;
        this.messageTransformer = messageTransformer;
    }

    private Runnable processorRunnable(Properties consumerProps, Properties producerProps) {
        return () -> {
            try (var producer = new KafkaProducer<K2, V2>(producerProps)) {
                try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K1, V1>(consumerProps)) {
                    consumer.subscribe(Set.of(this.sourceTopic));

                    var isDone = new AtomicBoolean(false);
                    while (!isDone.get())
                        consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                            KeyValuePair<K2, V2> payload =
                                    this.messageTransformer.apply(new KeyValuePair<>(record.key(), record.value()));

                            var producerRecord = new ProducerRecord<>(this.sinkTopic, payload.key(), payload.value());
                            producer.send(producerRecord, (metadata, exception) -> {
                                if (exception == null)
                                    log(record, producerRecord, metadata);
                                else {
                                    exception.printStackTrace();
                                    isDone.set(true);
                                }
                            });
                        });
                }
            }
        };
    }

    private void log(ConsumerRecord<K1, V1> consumedRecord, ProducerRecord<K2,V2> producedRecord,
                     RecordMetadata producedMetadata) {
        String logEntry = String.format("transform from {%s:%S} to {%s:%S} " +
                        "and send to %s topic on partition number %s with offset %s",
                consumedRecord.key().toString(), consumedRecord.value().toString(), producedRecord.key(), producedRecord.value(),
                producedMetadata.topic(), producedMetadata.partition(), producedMetadata.offset());

        System.out.println(logEntry);
    }

    public void startOnNewThread(Properties consumerProps, Properties producerProps) {
        new Thread(processorRunnable(consumerProps, producerProps)).start();
    }
}
