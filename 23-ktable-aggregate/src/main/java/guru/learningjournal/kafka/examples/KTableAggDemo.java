package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class KTableAggDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table(AppConfigs.topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
                .groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(AppSerdes.String(), AppSerdes.Employee()))
                .aggregate(
                        // initializer
                        () -> new DepartmentAggregate()
                                .withEmployeeCount(0)
                                .withTotalSalary(0)
                                .withAvgSalary(0D),
                        // adder
                        (k, v, aggV) -> new DepartmentAggregate()
                                .withEmployeeCount(aggV.getEmployeeCount() + 1)
                                .withTotalSalary(aggV.getTotalSalary() + v.getSalary())
                                .withAvgSalary((aggV.getTotalSalary() + v.getSalary()) / (aggV.getEmployeeCount() + 1D)),
                        // substractor
                        (k, v, aggV) -> new DepartmentAggregate()
                                .withEmployeeCount(aggV.getEmployeeCount() - 1)
                                .withTotalSalary(aggV.getTotalSalary() - v.getSalary())
                                .withAvgSalary((aggV.getTotalSalary() + v.getSalary()) / (aggV.getEmployeeCount() - 1D)),
                        // serializer
                        Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(AppConfigs.stateStoreName).withValueSerde(AppSerdes.DepartmentAggregate())
                ).toStream().print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department aggregate"));


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
