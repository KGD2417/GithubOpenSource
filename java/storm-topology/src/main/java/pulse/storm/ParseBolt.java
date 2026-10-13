package pulse.storm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** Kafka JSON event -> (ts, type, repo_id, repo_name, is_bot). Bot rules match the Pig ETL. */
public class ParseBolt extends BaseRichBolt {
  private static final Pattern NAME_BOT = Pattern.compile("(?i).*[-_]bot$");
  private transient ObjectMapper json;
  private transient Set<String> knownBots;
  private OutputCollector out;

  @Override
  public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
    out = collector;
    json = new ObjectMapper();
    knownBots = new HashSet<>();
    Object file = conf.get("pulse.bots.file");     // Sqoop-imported known_bots, one login per line
    if (file != null && Files.exists(Paths.get(file.toString()))) {
      try {
        for (String l : Files.readAllLines(Paths.get(file.toString()))) knownBots.add(l.trim());
      } catch (Exception e) {
        throw new IllegalStateException("cannot read bots file " + file, e);
      }
    }
  }

  static boolean isBot(String login, Set<String> known) {
    return login != null && (login.endsWith("[bot]") || known.contains(login) || NAME_BOT.matcher(login).matches());
  }

  @Override
  public void execute(Tuple t) {
    try {
      JsonNode e = json.readTree(t.getStringByField("value"));
      long ts = Instant.parse(e.path("created_at").asText()).toEpochMilli();
      String login = e.path("actor_login").asText(null);
      out.emit(t, new Values(ts, e.path("type").asText(), e.path("repo_id").asLong(),
          e.path("repo_name").asText(), isBot(login, knownBots)));
    } catch (Exception malformed) {
      // count-and-drop, like the batch ETL: a bad record must not stall the stream
    }
    out.ack(t);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer d) {
    d.declare(new Fields("ts", "type", "repo_id", "repo_name", "is_bot"));
  }
}
