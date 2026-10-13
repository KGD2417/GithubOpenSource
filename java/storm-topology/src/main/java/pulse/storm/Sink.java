package pulse.storm;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Where live results go: HBase in the cluster, memory in the LocalCluster test. */
public interface Sink extends Serializable {
  void open(Map<String, Object> conf);

  void minute(long minuteStart, Map<String, Long> counts);

  void topK(long windowEnd, List<String> top);

  void alert(long windowEnd, long repoId, String repoName, long count, double mean, double z);

  void flush();
}
