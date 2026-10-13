package pulse.storm;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

/**
 * gh.events (Kafka) -> parse -> { events/min by type + bot split | rolling top-K repos | spike detection }
 *                    -> HBase (live_minute, live_topk, spike_alerts) and spike alerts -> gh.alerts (Kafka).
 * PLAN.md §6.3. Submit:  storm jar pulse-storm.jar pulse.storm.PulseTopology [name] [topic]
 * (topic gh.events = GH Archive replay, gh.events.live = GitHub API poller; never mix them: windows use event time)
 */
public class PulseTopology {
  static final String KAFKA = "kafka:9092";

  /** Spout and sink are parameters so the LocalCluster test can feed fixed tuples and capture output. */
  static StormTopology build(IRichSpout spout, Sink sink, boolean alertsToKafka) {
    TopologyBuilder tb = new TopologyBuilder();
    tb.setSpout("kafka", spout, 1);
    tb.setBolt("parse", new ParseBolt(), 1).shuffleGrouping("kafka");
    Duration lag = Duration.seconds(10);
    tb.setBolt("type-minute", new TypeMinuteBolt().withTumblingWindow(Duration.minutes(1))
        .withTimestampField("ts").withLag(lag).withWatermarkInterval(Duration.seconds(1)), 1)
        .fieldsGrouping("parse", new Fields("type"));
    tb.setBolt("repo-counts", new RepoWindowCountBolt(10).withWindow(Duration.minutes(5), Duration.minutes(1))
        .withTimestampField("ts").withLag(lag).withWatermarkInterval(Duration.seconds(1)), 2)
        .fieldsGrouping("parse", new Fields("repo_id"));
    tb.setBolt("ranker", new TotalRankerBolt(10), 1).globalGrouping("repo-counts");
    tb.setBolt("spikes", new SpikeBolt(15, 4.0, 5).withTumblingWindow(Duration.minutes(1))
        .withTimestampField("ts").withLag(lag).withWatermarkInterval(Duration.seconds(1)), 2)
        .fieldsGrouping("parse", new Fields("repo_id"));
    tb.setBolt("sink", new SinkBolt(sink), 1)
        .shuffleGrouping("type-minute").shuffleGrouping("ranker").shuffleGrouping("spikes");
    if (alertsToKafka) {
      Properties p = new Properties();
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA);
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      tb.setBolt("alerts-kafka", new KafkaBolt<String, String>().withProducerProperties(p)
          .withTopicSelector(new DefaultTopicSelector("gh.alerts"))
          .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message")), 1)
          .shuffleGrouping("spikes");
    }
    return tb.createTopology();
  }

  static Config config() {
    Config c = new Config();
    c.setNumWorkers(1);
    // Windowed bolts hold tuples (un-acked) until their window closes: pending must exceed one
    // 5-minute window of events (~15k at GH Archive rates) or the spout stalls and windows never close.
    c.setMaxSpoutPending(50_000);
    c.setMessageTimeoutSecs(600);
    c.put("pulse.bots.file", "/opt/pulse-data/known_bots.txt");
    c.put("pulse.zookeeper", "zookeeper");
    return c;
  }

  public static void main(String[] args) throws Exception {
    String topic = args.length > 1 ? args[1] : "gh.events";
    KafkaSpoutConfig<String, String> kc = KafkaSpoutConfig.builder(KAFKA, topic)
        .setProp(ConsumerConfig.GROUP_ID_CONFIG, "pulse-storm-" + topic)
        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
        .build();
    StormSubmitter.submitTopology(args.length > 0 ? args[0] : "pulse",
        config(), build(new KafkaSpout<>(kc), new HBaseSink(), true));
  }
}
