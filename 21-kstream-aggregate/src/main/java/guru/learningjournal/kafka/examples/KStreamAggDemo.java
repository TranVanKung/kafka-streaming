package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class KStreamAggDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(AppConfigs.topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
                .groupBy((k, v) -> v.getDepartment(), Grouped.with(AppSerdes.String(), AppSerdes.Employee()))
                .aggregate(
                        // initializer
                        () -> new DepartmentAggregate()
                                .withEmployeeCount(0)
                                .withTotalSalary(0)
                                .withAvgSalary(0.0),
                        // aggregator
                        (k, v, aggValue) -> new DepartmentAggregate()
                                .withEmployeeCount(aggValue.getEmployeeCount() + 1)
                                .withTotalSalary(aggValue.getTotalSalary() + v.getSalary())
                                .withAvgSalary((double) ((aggValue.getTotalSalary() + v.getSalary()) / (aggValue.getEmployeeCount() + 10))),
                        // serializer
                        Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(AppConfigs.stateStoreName)
                                .withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.DepartmentAggregate())
                ).toStream().print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department aggregation"));


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));

    }
}
