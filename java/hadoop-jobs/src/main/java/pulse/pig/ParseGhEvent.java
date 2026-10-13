package pulse.pig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Flattens one GH Archive JSON line into one row per event (PLAN.md §6.2).
 *
 * Why a UDF rather than Pig's JsonLoader: JsonLoader needs one fixed schema for the whole record, but
 * `payload` changes shape with every event type and GitHub trimmed it in Oct 2025. Here a missing field
 * is just null, and a malformed line returns null (the Pig script counts and drops those).
 */
public class ParseGhEvent extends EvalFunc<Tuple> {

  /** Output columns, in order. Names match gh.events_clean so HCatStorer can map them. */
  static final Object[][] FIELDS = {
      {"event_id", DataType.LONG}, {"event_type", DataType.CHARARRAY}, {"created_at", DataType.DATETIME},
      {"created_ts", DataType.LONG}, {"actor_id", DataType.LONG}, {"actor_login", DataType.CHARARRAY},
      {"repo_id", DataType.LONG}, {"repo_name", DataType.CHARARRAY}, {"repo_owner", DataType.CHARARRAY},
      {"org_login", DataType.CHARARRAY}, {"action", DataType.CHARARRAY}, {"ref_type", DataType.CHARARRAY},
      {"push_size", DataType.INTEGER}, {"push_distinct_size", DataType.INTEGER},
      {"pr_number", DataType.INTEGER}, {"pr_merged", DataType.BOOLEAN}, {"pr_created_at", DataType.DATETIME},
      {"pr_closed_at", DataType.DATETIME}, {"pr_merged_at", DataType.DATETIME}, {"pr_additions", DataType.INTEGER},
      {"pr_deletions", DataType.INTEGER}, {"pr_changed_files", DataType.INTEGER}, {"pr_author", DataType.CHARARRAY},
      {"issue_number", DataType.INTEGER}, {"issue_created_at", DataType.DATETIME},
      {"issue_closed_at", DataType.DATETIME}, {"issue_comments", DataType.INTEGER},
      {"issue_author", DataType.CHARARRAY}, {"review_state", DataType.CHARARRAY},
      {"release_tag", DataType.CHARARRAY}, {"forkee_repo_id", DataType.LONG},
      {"payload_version", DataType.CHARARRAY},
  };

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TupleFactory TUPLES = TupleFactory.getInstance();

  @Override
  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) return null;
    JsonNode e;
    try {
      e = JSON.readTree(input.get(0).toString());
    } catch (IOException malformed) {
      return null;
    }
    Long id = asLong(e.path("id"));
    String type = text(e.path("type"));
    DateTime created = time(e.path("created_at"));
    Long repoId = asLong(e.path("repo").path("id"));
    if (id == null || type == null || created == null || repoId == null) return null;

    String repoName = text(e.path("repo").path("name"));
    JsonNode p = e.path("payload");
    JsonNode pr = p.path("pull_request");
    JsonNode issue = p.path("issue");

    Tuple t = TUPLES.newTuple(FIELDS.length);
    int i = 0;
    t.set(i++, id);
    t.set(i++, type);
    t.set(i++, created);
    t.set(i++, created.getMillis() / 1000);
    t.set(i++, asLong(e.path("actor").path("id")));
    t.set(i++, text(e.path("actor").path("login")));
    t.set(i++, repoId);
    t.set(i++, repoName);
    t.set(i++, repoName == null || repoName.indexOf('/') < 0 ? null : repoName.substring(0, repoName.indexOf('/')));
    t.set(i++, text(e.path("org").path("login")));
    t.set(i++, text(p.path("action")));
    t.set(i++, text(p.path("ref_type")));
    t.set(i++, asInt(p.path("size")));
    t.set(i++, asInt(p.path("distinct_size")));
    t.set(i++, asInt(pr.has("number") ? pr.path("number") : p.path("number")));
    t.set(i++, pr.has("merged") ? pr.path("merged").asBoolean() : null);
    t.set(i++, time(pr.path("created_at")));
    t.set(i++, time(pr.path("closed_at")));
    t.set(i++, time(pr.path("merged_at")));
    t.set(i++, asInt(pr.path("additions")));
    t.set(i++, asInt(pr.path("deletions")));
    t.set(i++, asInt(pr.path("changed_files")));
    t.set(i++, text(pr.path("user").path("login")));
    t.set(i++, asInt(issue.path("number")));
    t.set(i++, time(issue.path("created_at")));
    t.set(i++, time(issue.path("closed_at")));
    t.set(i++, asInt(issue.path("comments")));
    t.set(i++, text(issue.path("user").path("login")));
    t.set(i++, text(p.path("review").path("state")));
    t.set(i++, text(p.path("release").path("tag_name")));
    t.set(i++, asLong(p.path("forkee").path("id")));
    t.set(i++, payloadVersion(type, p, pr));
    return t;
  }

  /** "trimmed" marks the post-Oct-2025 Events API format, which lacks the fields our analytics need. */
  static String payloadVersion(String type, JsonNode p, JsonNode pr) {
    if ("PushEvent".equals(type) && !p.has("size")) return "trimmed";
    if ("PullRequestEvent".equals(type) && pr.isObject() && !pr.has("merged")) return "trimmed";
    return "full";
  }

  private static String text(JsonNode n) {
    return n.isMissingNode() || n.isNull() ? null : n.asText();
  }

  private static Long asLong(JsonNode n) {
    if (n.isNumber()) return n.asLong();
    if (n.isTextual()) {
      try {
        return Long.parseLong(n.asText());
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private static Integer asInt(JsonNode n) {
    return n.isNumber() ? n.asInt() : null;
  }

  private static DateTime time(JsonNode n) {
    if (!n.isTextual()) return null;
    try {
      return new DateTime(n.asText(), DateTimeZone.UTC);
    } catch (IllegalArgumentException bad) {
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema s = new Schema();
    for (Object[] f : FIELDS) s.add(new Schema.FieldSchema((String) f[0], (Byte) f[1]));
    try {
      return new Schema(new Schema.FieldSchema("event", s, DataType.TUPLE));
    } catch (Exception impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
