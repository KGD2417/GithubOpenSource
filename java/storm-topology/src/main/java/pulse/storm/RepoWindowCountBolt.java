package pulse.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 * Rolling counts per repo over a 5-minute window sliding every minute. Fields grouping on repo_id means
 * each task owns a disjoint set of repos, so its local top-K is a correct partial ranking; the
 * TotalRankerBolt merges the partials (classic two-stage top-K).
 */
public class RepoWindowCountBolt extends BaseWindowedBolt {
  private final int k;
  private OutputCollector out;

  public RepoWindowCountBolt(int k) {
    this.k = k;
  }

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
  }

  @Override
  public void execute(TupleWindow w) {
    Map<Long, long[]> counts = new HashMap<>();
    Map<Long, String> names = new HashMap<>();
    for (Tuple t : w.get()) {
      if (t.getBooleanByField("is_bot")) continue;          // "what humans are doing right now"
      long repo = t.getLongByField("repo_id");
      counts.computeIfAbsent(repo, r -> new long[1])[0]++;
      names.putIfAbsent(repo, t.getStringByField("repo_name"));
    }
    out.emit(new Values(w.getEndTimestamp(), TopK.top(counts, names, k)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("window_end", "partial"));
  }

  /** Shared top-K helper: entries are "repo_id|name|count", highest count first. */
  static class TopK {
    static List<String> top(Map<Long, long[]> counts, Map<Long, String> names, int k) {
      List<Map.Entry<Long, long[]>> es = new ArrayList<>(counts.entrySet());
      es.sort((a, b) -> a.getValue()[0] != b.getValue()[0]
          ? Long.compare(b.getValue()[0], a.getValue()[0]) : Long.compare(a.getKey(), b.getKey()));
      List<String> top = new ArrayList<>();
      for (int i = 0; i < Math.min(k, es.size()); i++) {
        Map.Entry<Long, long[]> e = es.get(i);
        top.add(e.getKey() + "|" + names.get(e.getKey()) + "|" + e.getValue()[0]);
      }
      return top;
    }
  }
}
