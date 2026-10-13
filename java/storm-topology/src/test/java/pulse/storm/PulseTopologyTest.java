package pulse.storm;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.FixedTupleSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

/** Runs the real topology in a LocalCluster with fixed input and the in-memory sink (PLAN.md §13). */
class PulseTopologyTest {
  static final long T0 = Instant.parse("2025-09-01T12:00:00Z").toEpochMilli();

  static Values event(long ms, String type, long repo, String name, String login) {
    return new Values(String.format(
        "{\"type\":\"%s\",\"created_at\":\"%s\",\"repo_id\":%d,\"repo_name\":\"%s\",\"actor_login\":\"%s\"}",
        type, Instant.ofEpochMilli(ms), repo, name, login));
  }

  @Test
  void countsRanksAndSpikes() throws Exception {
    List<Values> in = new ArrayList<>();
    for (int m = 0; m < 12; m++) {
      long base = T0 + m * 60_000L;
      for (int i = 0; i < 3; i++) in.add(event(base + 1000 + i, "PushEvent", 1, "a/one", "alice"));
      in.add(event(base + 2000, "WatchEvent", 2, "b/two", "ci[bot]"));
      int spikeEvents = m == 8 ? 40 : 1;                       // repo 42: quiet, then a burst in minute 8
      for (int i = 0; i < spikeEvents; i++) in.add(event(base + 3000 + i, "IssuesEvent", 42, "s/spike", "user" + i));
    }
    in.add(event(T0 + 20 * 60_000L, "PushEvent", 1, "a/one", "alice"));   // advances the watermark

    Config conf = PulseTopology.config();
    conf.put("pulse.bots.file", "/nonexistent");
    try (LocalCluster cluster = new LocalCluster()) {
      cluster.submitTopology("test", conf,
          PulseTopology.build(new FixedTupleSpout(in, new Fields("value")), new MemorySink(), false));
      long deadline = System.currentTimeMillis() + 90_000;
      while (System.currentTimeMillis() < deadline
          && (MemorySink.ALERTS.isEmpty() || MemorySink.MINUTES.size() < 10)) {
        Thread.sleep(500);
      }
    }

    Map<String, Long> first = MemorySink.MINUTES.get(T0);
    assertNotNull(first, "minute 0 counted");
    assertEquals(3L, first.get("PushEvent"));
    assertEquals(1L, first.get("WatchEvent"));
    assertEquals(1L, first.get("_bot"));                        // ci[bot]
    assertEquals(4L, first.get("_human"));
    assertEquals(40L, MemorySink.MINUTES.get(T0 + 8 * 60_000L).get("IssuesEvent"));   // the burst

    assertEquals(1, MemorySink.ALERTS.size(), "exactly one spike: " + MemorySink.ALERTS);
    assertTrue(MemorySink.ALERTS.get(0).startsWith("s/spike@" + (T0 + 9 * 60_000L)), MemorySink.ALERTS.get(0));

    boolean spikeLeads = MemorySink.TOPK.values().stream().anyMatch(t -> !t.isEmpty() && t.get(0).startsWith("42|s/spike|"));
    assertTrue(spikeLeads, "spiking repo tops a 5-minute window: " + MemorySink.TOPK);
    assertTrue(MemorySink.TOPK.values().stream().allMatch(t -> t.stream().noneMatch(e -> e.startsWith("2|"))),
        "bot-only repo never ranked");
  }
}
