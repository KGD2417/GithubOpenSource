package pulse.storm;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * Spike detection per repo on 1-minute event-time windows. Each repo keeps an exponentially weighted
 * mean and variance of its per-minute event count (alpha = 0.2); minutes in which a repo was silent count
 * as zeros. Alert when count >= minCount AND count > mean + z * stddev, after a warm-up period.
 * Fields grouping on repo_id keeps each repo's state in exactly one task.
 */
public class SpikeBolt extends BaseWindowedBolt {
  static final double ALPHA = 0.2;
  private final int minCount, warmupWindows;
  private final double z;
  private Map<Long, double[]> state;          // repo -> {mean, var, lastWindow}
  private long windowsSeen;
  private OutputCollector out;

  public SpikeBolt(int minCount, double z, int warmupWindows) {
    this.minCount = minCount;
    this.z = z;
    this.warmupWindows = warmupWindows;
  }

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
    state = new LinkedHashMap<Long, double[]>(1024, 0.75f, true) {   // LRU cap bounds memory
      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, double[]> e) {
        return size() > 200_000;
      }
    };
  }

  static void update(double[] s, double x) {
    double diff = x - s[0], incr = ALPHA * diff;
    s[0] += incr;
    s[1] = (1 - ALPHA) * (s[1] + diff * incr);
  }

  @Override
  public void execute(TupleWindow w) {
    long idx = w.getEndTimestamp() / 60_000L;
    windowsSeen++;
    Map<Long, long[]> counts = new HashMap<>();
    Map<Long, String> names = new HashMap<>();
    for (Tuple t : w.get()) {
      long repo = t.getLongByField("repo_id");
      counts.computeIfAbsent(repo, r -> new long[1])[0]++;
      names.putIfAbsent(repo, t.getStringByField("repo_name"));
    }
    for (Map.Entry<Long, long[]> e : counts.entrySet()) {
      double[] s = state.computeIfAbsent(e.getKey(), r -> new double[] {0, 0, idx - 1});
      for (long gap = Math.min(idx - (long) s[2] - 1, 60); gap > 0; gap--) update(s, 0);   // silent minutes
      long c = e.getValue()[0];
      double threshold = s[0] + z * Math.sqrt(s[1]);
      if (windowsSeen > warmupWindows && c >= minCount && c > threshold) {
        double zScore = s[1] > 0 ? (c - s[0]) / Math.sqrt(s[1]) : Double.POSITIVE_INFINITY;
        String msg = String.format("{\"window_end\":%d,\"repo_id\":%d,\"repo_name\":\"%s\",\"count\":%d,\"mean\":%.2f,\"z\":%.1f}",
            w.getEndTimestamp(), e.getKey(), names.get(e.getKey()), c, s[0], Math.min(zScore, 999));
        out.emit(new Values(String.valueOf(e.getKey()), msg, w.getEndTimestamp(), e.getKey(),
            names.get(e.getKey()), c, s[0], Math.min(zScore, 999)));
      }
      update(s, c);
      s[2] = idx;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("key", "message", "window_end", "repo_id", "repo_name", "count", "mean", "z"));
  }
}
