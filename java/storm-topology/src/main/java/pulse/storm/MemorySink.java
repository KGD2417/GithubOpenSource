package pulse.storm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test sink: static so the LocalCluster test can inspect what the (serialized) bolt wrote. */
public class MemorySink implements Sink {
  static final Map<Long, Map<String, Long>> MINUTES = new ConcurrentHashMap<>();
  static final Map<Long, List<String>> TOPK = new ConcurrentHashMap<>();
  static final List<String> ALERTS = new CopyOnWriteArrayList<>();

  @Override public void open(Map<String, Object> conf) {}

  @Override
  public void minute(long minuteStart, Map<String, Long> counts) {
    Map<String, Long> m = MINUTES.computeIfAbsent(minuteStart, k -> new ConcurrentHashMap<>());
    counts.forEach((k, v) -> m.merge(k, v, Long::sum));
  }

  @Override public void topK(long windowEnd, List<String> top) { TOPK.put(windowEnd, top); }

  @Override
  public void alert(long windowEnd, long repoId, String repoName, long count, double mean, double z) {
    ALERTS.add(repoName + "@" + windowEnd + ":" + count);
  }

  @Override public void flush() {}
}
