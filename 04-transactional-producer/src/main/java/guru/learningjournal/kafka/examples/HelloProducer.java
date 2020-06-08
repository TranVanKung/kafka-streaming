package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*
* để dùng được transaction cho kafka thì cần 2 điều kiện bắt buộc sau:
* 1. replication factor >= 3
* 2. min.insync.replicas >= 2
* kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 5 --topic hello-producer-2 min.insync.replicas=2
*
* Có 3 bước để tạo 1 transaction:
* 1. gọi initTransaction()
* */
public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*
        * để dùng được transaction trong kafka thì phải thoả mãn hai điều kiện sau
        * 1. transaction depends on Idempotence
        * 2. transaction_id_config must be unique for each Producer instance
        * */
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transactionId);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Start first transaction");
        producer.beginTransaction();

        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T1" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T1" + i));
            }
            logger.info("Committing first transaction");
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            logger.error("Exception in first transaction. Aborting ...");
            producer.abortTransaction();
            producer.close();
            e.printStackTrace();
        }

        // second transaction
        logger.info("Start second transaction");
        producer.beginTransaction();

        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T2" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T2" + i));
            }
            logger.info("Aborting second transaction");
            producer.abortTransaction();
        } catch (ProducerFencedException e) {
            logger.error("Exception in first transaction. Aborting ...");
            producer.abortTransaction();
            producer.close();
            e.printStackTrace();
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
