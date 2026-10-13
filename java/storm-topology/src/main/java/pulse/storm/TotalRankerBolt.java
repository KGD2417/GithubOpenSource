package pulse.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Global (single-task) merge of the partial top-K lists. A window's ranking is final once every counting
 * task has reported it, or once any task reports a newer window (a task with no events never reports).
 */
public class TotalRankerBolt extends BaseRichBolt {
  private final int k;
  private int upstreamTasks;
  private final TreeMap<Long, Map<Long, long[]>> pending = new TreeMap<>();
  private final Map<Long, Integer> reports = new HashMap<>();
  private final Map<Long, String> names = new HashMap<>();
  private OutputCollector out;

  public TotalRankerBolt(int k) {
    this.k = k;
  }

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
    upstreamTasks = ctx.getComponentTasks("repo-counts").size();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(Tuple t) {
    long end = t.getLongByField("window_end");
    Map<Long, long[]> merged = pending.computeIfAbsent(end, e -> new HashMap<>());
    for (String entry : (List<String>) t.getValueByField("partial")) {
      String[] f = entry.split("\\|", 3);
      long repo = Long.parseLong(f[0]);
      int last = entry.lastIndexOf('|');
      names.put(repo, entry.substring(f[0].length() + 1, last));
      merged.computeIfAbsent(repo, r -> new long[1])[0] += Long.parseLong(entry.substring(last + 1));
    }
    int n = reports.merge(end, 1, Integer::sum);
    while (!pending.isEmpty() && (pending.firstKey() < end || (pending.firstKey() == end && n >= upstreamTasks))) {
      long w = pending.firstKey();
      out.emit(new Values(w, RepoWindowCountBolt.TopK.top(pending.remove(w), names, k)));
      reports.remove(w);
    }
    if (names.size() > 100_000) names.clear();
    out.ack(t);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("window_end", "topk"));
  }
}
