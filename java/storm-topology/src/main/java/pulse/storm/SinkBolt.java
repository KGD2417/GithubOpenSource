package pulse.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

/** Buffers results from the three analysis bolts and writes them once a second; acks only after a flush. */
public class SinkBolt extends BaseRichBolt {
  private final Sink sink;
  private final List<Tuple> unacked = new ArrayList<>();
  private OutputCollector out;

  public SinkBolt(Sink sink) {
    this.sink = sink;
  }

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
    sink.open(conf);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(Tuple t) {
    if (TupleUtils.isTick(t)) {
      sink.flush();
      unacked.forEach(out::ack);
      unacked.clear();
      return;
    }
    switch (t.getSourceComponent()) {
      case "type-minute":
        sink.minute(t.getLongByField("minute"), (Map<String, Long>) t.getValueByField("counts"));
        break;
      case "ranker":
        sink.topK(t.getLongByField("window_end"), (List<String>) t.getValueByField("topk"));
        break;
      case "spikes":
        sink.alert(t.getLongByField("window_end"), t.getLongByField("repo_id"), t.getStringByField("repo_name"),
            t.getLongByField("count"), t.getDoubleByField("mean"), t.getDoubleByField("z"));
        break;
      default:
        break;
    }
    unacked.add(t);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config c = new Config();
    c.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
    return c;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {}
}
