package guru.learningjournal.kafka.examples;

public class AppConfigs {
    public final static String applicationID = "SyncHelloProducer";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String topicName = "sync-hello-producer";
    public final static int numEvents = 100;
}
