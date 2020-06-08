package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RewardsPartitioner implements StreamPartitioner<String, PosInvoice> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        int i = Math.abs(value.getCustomerCardNo().hashCode()) % numPartitions;
        return i;
    }
}
