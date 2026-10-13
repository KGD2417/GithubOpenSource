package pulse.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

/**
 * Daily top-N repositories per event type (PLAN.md §6.1), reading gh.events_clean Parquet directly.
 *
 *   map:       (Void, row)            -> ((dt, type, repo_id), 1)
 *   combine:   ((dt, type, repo), [1,1,..]) -> ((dt, type, repo), n)      sums map-side, ~90% less shuffle
 *   partition: hash(dt + type) only, so ONE reducer owns every repo of a (dt, type) group
 *   reduce:    sum per repo; keep a size-N min-heap per (dt, type); flush when the group changes
 *   output:    dt  event_type  rank  repo_id  count
 *
 * Usage: hadoop jar pulse-hadoop.jar pulse.mr.TopReposByTypeDay [-D pulse.topN=25] [-D pulse.excludeBots=true]
 *          [-D pulse.eventTypes=WatchEvent,ForkEvent] <input-dir-or-glob> <output-dir>
 */
public class TopReposByTypeDay extends Configured implements Tool {

  public enum Pulse { RECORDS_IN, BOT_SKIPPED, NULL_REPO, TYPE_SKIPPED, GROUPS_OUT }

  /** Only the columns we need are read from Parquet (column projection). */
  static final String READ_SCHEMA =
      "message events { optional int64 repo_id; optional boolean is_bot; }";

  public static class EventMapper extends Mapper<Void, Group, TypeDayRepoKey, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final TypeDayRepoKey key = new TypeDayRepoKey();
    private String dt, eventType;
    private boolean excludeBots, skipFile;

    @Override
    protected void setup(Context ctx) {
      // dt and event_type are Hive partition columns: they live in the path, not in the file.
      Path file = ((FileSplit) ctx.getInputSplit()).getPath();
      eventType = partitionValue(file, "event_type");
      dt = partitionValue(file, "dt");
      excludeBots = ctx.getConfiguration().getBoolean("pulse.excludeBots", false);
      String types = ctx.getConfiguration().get("pulse.eventTypes", "");
      skipFile = !types.isEmpty() && !("," + types + ",").contains("," + eventType + ",");
    }

    @Override
    protected void map(Void k, Group row, Context ctx) throws IOException, InterruptedException {
      ctx.getCounter(Pulse.RECORDS_IN).increment(1);
      if (skipFile) {
        ctx.getCounter(Pulse.TYPE_SKIPPED).increment(1);
        return;
      }
      if (row.getFieldRepetitionCount("repo_id") == 0) {
        ctx.getCounter(Pulse.NULL_REPO).increment(1);
        return;
      }
      if (excludeBots && row.getFieldRepetitionCount("is_bot") > 0 && row.getBoolean("is_bot", 0)) {
        ctx.getCounter(Pulse.BOT_SKIPPED).increment(1);
        return;
      }
      key.set(dt, eventType, row.getLong("repo_id", 0));
      ctx.write(key, ONE);
    }
  }

  static String partitionValue(Path file, String name) {
    for (Path p = file.getParent(); p != null; p = p.getParent()) {
      if (p.getName().startsWith(name + "=")) return p.getName().substring(name.length() + 1);
    }
    throw new IllegalArgumentException("no " + name + "= directory above " + file);
  }

  /** Combiner and reducer-side sum: associative and commutative, so it's safe to run 0..n times. */
  public static class SumCombiner extends Reducer<TypeDayRepoKey, LongWritable, TypeDayRepoKey, LongWritable> {
    private final LongWritable total = new LongWritable();

    @Override
    protected void reduce(TypeDayRepoKey k, Iterable<LongWritable> vs, Context ctx)
        throws IOException, InterruptedException {
      long s = 0;
      for (LongWritable v : vs) s += v.get();
      total.set(s);
      ctx.write(k, total);
    }
  }

  /** Ignores repo_id on purpose: every repo of one (dt, type) must meet in the same reducer. */
  public static class TypeDayPartitioner extends Partitioner<TypeDayRepoKey, LongWritable> {
    @Override
    public int getPartition(TypeDayRepoKey k, LongWritable v, int numPartitions) {
      return ((k.dt + "|" + k.eventType).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
  }

  public static class TopNReducer extends Reducer<TypeDayRepoKey, LongWritable, Text, NullWritable> {
    private int n;
    private String curDt, curType;
    private final PriorityQueue<long[]> heap = new PriorityQueue<>((a, b) ->
        a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(b[1], a[1]));  // min count first; ties: bigger id out
    private final Text line = new Text();

    @Override
    protected void setup(Context ctx) {
      n = ctx.getConfiguration().getInt("pulse.topN", 25);
    }

    @Override
    protected void reduce(TypeDayRepoKey k, Iterable<LongWritable> vs, Context ctx)
        throws IOException, InterruptedException {
      if (curDt == null || !k.sameGroup(curDt, curType)) {
        flush(ctx);
        curDt = k.dt;
        curType = k.eventType;
      }
      long s = 0;
      for (LongWritable v : vs) s += v.get();
      heap.offer(new long[] {s, k.repoId});
      if (heap.size() > n) heap.poll();
    }

    private void flush(Context ctx) throws IOException, InterruptedException {
      if (heap.isEmpty()) return;
      List<long[]> top = new ArrayList<>(heap);
      heap.clear();
      top.sort(Collections.reverseOrder(heap.comparator()));
      for (int rank = 1; rank <= top.size(); rank++) {
        long[] e = top.get(rank - 1);
        line.set(curDt + "\t" + curType + "\t" + rank + "\t" + e[1] + "\t" + e[0]);
        ctx.write(line, NullWritable.get());
      }
      ctx.getCounter(Pulse.GROUPS_OUT).increment(1);
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
      flush(ctx);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: TopReposByTypeDay [-D pulse.topN=25] <input> <output>");
      return 2;
    }
    Configuration conf = getConf();
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, org.apache.parquet.hadoop.example.GroupReadSupport.class.getName());
    conf.set("parquet.read.schema", READ_SCHEMA);
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    Job job = Job.getInstance(conf, "pulse-top-repos " + args[0]);
    job.setJarByClass(TopReposByTypeDay.class);
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setMapperClass(EventMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setPartitionerClass(TypeDayPartitioner.class);
    job.setReducerClass(TopNReducer.class);
    job.setMapOutputKeyClass(TypeDayRepoKey.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new TopReposByTypeDay(), args));
  }
}
