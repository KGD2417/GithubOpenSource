package pulse.storm;

import java.util.Map;
import java.util.TreeMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 * Events per minute by type, plus bot vs human, over 1-minute EVENT-TIME tumbling windows: at a 60x
 * replay each window still covers one GitHub minute. Emits (minute_start_ms, {type -> n, _bot, _human}).
 */
public class TypeMinuteBolt extends BaseWindowedBolt {
  private OutputCollector out;

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
  }

  @Override
  public void execute(TupleWindow w) {
    Map<String, Long> counts = new TreeMap<>();
    for (Tuple t : w.get()) {
      counts.merge(t.getStringByField("type"), 1L, Long::sum);
      counts.merge(t.getBooleanByField("is_bot") ? "_bot" : "_human", 1L, Long::sum);
    }
    if (!counts.isEmpty()) out.emit(new Values(w.getEndTimestamp() - 60_000L, counts));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("minute", "counts"));
  }
}
