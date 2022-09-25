package kafka.playground;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTestRunner {

    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092"; // can easily be externalized through CLI

        String cpuUsageTopic = createTopicIfNotExists(bootstrapServer, "cpu-usage",
                1, 1);
        String cpuUsagePercentage = createTopicIfNotExists(bootstrapServer, "cpu-usage-percentage",
                1, 1);

        createAndStartCpuUsageProducer(bootstrapServer, cpuUsageTopic);

        createAndStartCpuUsageToPercentileFormatProcessor(bootstrapServer, cpuUsageTopic, cpuUsagePercentage);

        createAndStartCpuUsagePrinter(bootstrapServer, cpuUsagePercentage);

    }

    private static String createTopicIfNotExists(String bootstrapServer, String topicName,
                                                 int partitions, int replicationFactor) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        try (Admin admin = Admin.create(adminProps)) {
            Map<String, String> configs = new HashMap<>(){{
                put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            }};

            NewTopic topic = new NewTopic(topicName, partitions, (short) replicationFactor).configs(configs);
            CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));
            var future = result.values().get(topicName);

            future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException)
                System.out.println("topic already exists, no need for re-creation");
            else throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return topicName;
    }

    private static void createAndStartCpuUsageProducer(String bootstrapServer, String cpuUsageTopic) {
        CpuUsageService cpuUsageService = new CpuUsageService();

        MessageSupplier<Long, Double> cpuUsageSupplier = () -> {
            double cpuUsage = cpuUsageService.readCpuUsage();
            return new KeyValuePair<>(System.currentTimeMillis(), cpuUsage);
        };

        new KafkaDataGeneratorProducer<>(cpuUsageTopic, cpuUsageSupplier)
                .startOnNewThread(new Properties() {{
                    setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                    setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                    setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());
                }});
    }

    private static void createAndStartCpuUsageToPercentileFormatProcessor(String bootstrapServer, String cpuUsageTopic,
                                                                          String cpuUsagePercentage) {
        Properties consumerProps = new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, "cpu-usage-to-percentile");
        }};
        Properties producerProps = new Properties() {{
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());
        }};

        new KafkaProcessor<Long, Double, Long, Double>(cpuUsageTopic, cpuUsagePercentage,
                pair -> new KeyValuePair<>(pair.key(), pair.value() * 100))
                .startOnNewThread(consumerProps, producerProps);
    }

    private static void createAndStartCpuUsagePrinter(String bootstrapServer, String cpuUsageTopic) {
        MessageHandler<Long, Double> cpuUsagePrinter = record -> {
            String logEntry = String.format("cpu usage : %.2f%s , usage read timestamp : %dms, " +
                            "time taken to consume the message : %dms",
                    record.value(),"%" , record.key(), System.currentTimeMillis() - record.key()
            );
            System.out.println(logEntry);
        };

        new KafkaConsumer<>(cpuUsageTopic, cpuUsagePrinter)
                .startOnNewThread(new Properties() {{
                    setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                    setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
                    setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
                    setProperty(ConsumerConfig.GROUP_ID_CONFIG, "cpu-monitor");
                }});
    }


}
