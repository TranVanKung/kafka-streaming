package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class RewardsApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(
                AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice())
        ).filter((key, value) ->
                value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME));

        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(AppConfigs.REWARDS_STORE_NAME),
                AppSerdes.String(), AppSerdes.Double()
        );

        builder.addStateStore(kvStoreBuilder);

        KS0.through(AppConfigs.REWARDS_TEMP_TOPIC,
                Produced.with(AppSerdes.String(), AppSerdes.PosInvoice(), new RewardsPartitioner()))
                .transformValues(() -> new RewardsTransformer(), AppConfigs.REWARDS_STORE_NAME)
                .to(AppConfigs.notificationTopic,
                        Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        logger.info("Starting Stream");
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            stream.cleanUp();
        }));
    }
}
