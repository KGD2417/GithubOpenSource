package pulse.pig;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ParseGhEventTest {
  static List<String> lines;
  static final ParseGhEvent udf = new ParseGhEvent();

  @BeforeAll
  static void load() throws Exception {
    lines = Files.readAllLines(Paths.get("../../tests/fixtures/events_small.json"), StandardCharsets.UTF_8);
  }

  static Tuple parse(String line) throws Exception {
    return udf.exec(TupleFactory.getInstance().newTuple((Object) line));
  }

  static Object col(Tuple t, String name) throws Exception {
    for (int i = 0; i < ParseGhEvent.FIELDS.length; i++) if (ParseGhEvent.FIELDS[i][0].equals(name)) return t.get(i);
    throw new IllegalArgumentException(name);
  }

  @Test
  void fixtureCounts() throws Exception {
    int ok = 0, bad = 0;
    for (String l : lines) if (parse(l) == null) bad++; else ok++;
    assertEquals(39, ok, "37 real + duplicate + trimmed parse");
    assertEquals(1, bad, "one malformed line");
  }

  @Test
  void everyRowHasKeysAndSchemaWidth() throws Exception {
    for (String l : lines) {
      Tuple t = parse(l);
      if (t == null) continue;
      assertEquals(ParseGhEvent.FIELDS.length, t.size());
      assertNotNull(col(t, "event_id"));
      assertNotNull(col(t, "event_type"));
      assertNotNull(col(t, "repo_id"));
      assertEquals(((org.joda.time.DateTime) col(t, "created_at")).getMillis() / 1000, col(t, "created_ts"));
    }
  }

  @Test
  void mergedPullRequestHasTimestamps() throws Exception {
    boolean found = false;
    for (String l : lines) {
      Tuple t = parse(l);
      if (t != null && Boolean.TRUE.equals(col(t, "pr_merged"))) {
        found = true;
        assertNotNull(col(t, "pr_created_at"));
        assertNotNull(col(t, "pr_merged_at"));
        assertEquals("closed", col(t, "action"));
        assertEquals("full", col(t, "payload_version"));
      }
    }
    assertTrue(found);
  }

  @Test
  void trimmedPayloadIsTagged() throws Exception {
    Tuple t = parse(lines.get(lines.size() - 1));
    assertEquals("PullRequestEvent", col(t, "event_type"));
    assertEquals("trimmed", col(t, "payload_version"));
    assertNull(col(t, "pr_merged"));
    assertNotNull(col(t, "pr_number"));
  }

  @Test
  void typeSpecificFields() throws Exception {
    Map<String, Tuple> byType = new HashMap<>();
    for (String l : lines) {
      Tuple t = parse(l);
      if (t != null) byType.putIfAbsent((String) col(t, "event_type"), t);
    }
    assertNotNull(col(byType.get("PushEvent"), "push_size"));
    assertNotNull(col(byType.get("ForkEvent"), "forkee_repo_id"));
    assertNotNull(col(byType.get("ReleaseEvent"), "release_tag"));
    assertEquals("started", col(byType.get("WatchEvent"), "action"));
    assertTrue(((String) col(byType.get("WatchEvent"), "repo_name")).startsWith(
        (String) col(byType.get("WatchEvent"), "repo_owner") + "/"));
  }

  @Test
  void nullAndGarbageInput() throws Exception {
    assertNull(udf.exec(null));
    assertNull(parse("not json"));
    assertNull(parse("{\"id\":\"1\",\"type\":\"PushEvent\"}"), "no repo/created_at");
  }
}
