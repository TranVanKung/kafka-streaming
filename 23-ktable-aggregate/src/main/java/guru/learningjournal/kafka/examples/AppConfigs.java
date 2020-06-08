package guru.learningjournal.kafka.examples;

class AppConfigs {

    final static String applicationID = "KTableAggDemo";
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "employees";
    final static String stateStoreName = "state-store";
    final static String stateStoreLocation = "tmp/state-store";
}
