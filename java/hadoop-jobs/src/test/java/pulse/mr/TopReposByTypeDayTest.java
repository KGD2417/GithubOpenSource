package pulse.mr;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopReposByTypeDayTest {
  static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message events { optional int64 event_id; optional int64 repo_id; optional binary repo_name (UTF8);"
          + " optional boolean is_bot; }");

  /** Writes one Parquet file in Hive's partition layout: rows are {repo_id, is_bot(0/1)}. */
  static void writePartition(File root, String dt, String type, long[][] rows) throws Exception {
    Path file = new Path(new File(root, "dt=" + dt + "/event_type=" + type + "/part-0.parquet").toURI());
    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> w = ExampleParquetWriter.builder(file).withType(SCHEMA).withConf(new Configuration()).build()) {
      long id = 0;
      for (long[] r : rows) {
        Group g = f.newGroup().append("event_id", id++).append("repo_id", r[0]).append("repo_name", "o/r" + r[0])
            .append("is_bot", r[1] == 1);
        w.write(g);
      }
    }
  }

  static long[][] repeat(long repo, int times, int bot) {
    long[][] r = new long[times][];
    for (int i = 0; i < times; i++) r[i] = new long[] {repo, bot};
    return r;
  }

  static long[][] concat(long[][]... parts) {
    List<long[]> all = new ArrayList<>();
    for (long[][] p : parts) for (long[] r : p) all.add(r);
    return all.toArray(new long[0][]);
  }

  @Test
  void topNPerTypeAndDay(@TempDir File tmp) throws Exception {
    File in = new File(tmp, "in");
    writePartition(in, "2025-09-01", "WatchEvent", concat(repeat(1, 5, 0), repeat(2, 9, 0), repeat(3, 2, 0), repeat(4, 7, 1)));
    writePartition(in, "2025-09-01", "ForkEvent", concat(repeat(7, 3, 0), repeat(8, 1, 0)));
    writePartition(in, "2025-09-02", "WatchEvent", repeat(1, 4, 0));

    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    conf.setInt("mapreduce.job.reduces", 2);
    String out = new File(tmp, "out").getPath();
    int rc = ToolRunner.run(conf, new TopReposByTypeDay(),
        new String[] {"-D", "pulse.topN=2", "-D", "pulse.excludeBots=true", in.getPath(), out});
    assertEquals(0, rc);

    List<String> lines = new ArrayList<>();
    for (File f : new File(out).listFiles((d, n) -> n.startsWith("part-"))) {
      lines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
    }
    lines.sort(null);
    assertEquals(java.util.Arrays.asList(
        "2025-09-01\tForkEvent\t1\t7\t3",
        "2025-09-01\tForkEvent\t2\t8\t1",
        "2025-09-01\tWatchEvent\t1\t2\t9",     // repo 4 (7 bot stars) excluded
        "2025-09-01\tWatchEvent\t2\t1\t5",
        "2025-09-02\tWatchEvent\t1\t1\t4"), lines);
  }

  @Test
  void partitionerKeepsGroupTogether() {
    TopReposByTypeDay.TypeDayPartitioner p = new TopReposByTypeDay.TypeDayPartitioner();
    LongWritable one = new LongWritable(1);
    int first = p.getPartition(new TypeDayRepoKey("2025-09-01", "PushEvent", 1), one, 7);
    for (long repo = 2; repo < 500; repo++) {
      assertEquals(first, p.getPartition(new TypeDayRepoKey("2025-09-01", "PushEvent", repo), one, 7));
    }
  }

  @Test
  void partitionValueFromPath() {
    Path f = new Path("hdfs://nn/gh/clean/events/dt=2025-09-01/event_type=WatchEvent/part-0.parquet");
    assertEquals("WatchEvent", TopReposByTypeDay.partitionValue(f, "event_type"));
    assertEquals("2025-09-01", TopReposByTypeDay.partitionValue(f, "dt"));
  }
}
