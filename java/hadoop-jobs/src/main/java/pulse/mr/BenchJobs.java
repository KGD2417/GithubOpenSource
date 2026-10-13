package pulse.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/**
 * The MapReduce side of the engine benchmark (PLAN.md §10); Q2 is TopReposByTypeDay itself.
 *   q1  events per (dt, event_type)                          hadoop jar ... pulse.mr.BenchJobs q1 <in> <out>
 *   q3  events and stars per (dt, primary_language): map-side join with the Sqoop-imported catalogue,
 *       loaded into a HashMap in setup() (small dimension -> no shuffle for the join)
 *                                                            hadoop jar ... pulse.mr.BenchJobs q3 <in> <out> <catalogue-dir>
 */
public class BenchJobs extends Configured implements Tool {

  /** q1: key = dt \t event_type. Reads no columns at all: the key comes from the partition path. */
  public static class TypeDayMapper extends Mapper<Void, Group, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final Text key = new Text();

    @Override
    protected void setup(Context ctx) {
      Path f = ((FileSplit) ctx.getInputSplit()).getPath();
      key.set(TopReposByTypeDay.partitionValue(f, "dt") + "\t" + TopReposByTypeDay.partitionValue(f, "event_type"));
    }

    @Override
    protected void map(Void k, Group row, Context ctx) throws IOException, InterruptedException {
      ctx.write(key, ONE);
    }
  }

  /** q3: key = dt \t language \t E|S (events or stars); repos outside the catalogue are skipped. */
  public static class LanguageMapper extends Mapper<Void, Group, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final Map<Long, String> language = new HashMap<>();
    private final Text key = new Text();
    private String dt;
    private boolean isStar;

    @Override
    protected void setup(Context ctx) throws IOException {
      Path f = ((FileSplit) ctx.getInputSplit()).getPath();
      dt = TopReposByTypeDay.partitionValue(f, "dt");
      isStar = "WatchEvent".equals(TopReposByTypeDay.partitionValue(f, "event_type"));
      Configuration conf = ctx.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      for (FileStatus st : fs.listStatus(new Path(conf.get("pulse.catalogue")))) {
        if (!st.getPath().getName().startsWith("part-")) continue;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(st.getPath()), StandardCharsets.UTF_8))) {
          for (String line; (line = r.readLine()) != null; ) {
            String[] c = line.split("\t", -1);              // repo_id, full_name, owner, owner_type, language, ...
            if (c.length > 4 && !c[4].isEmpty() && !c[4].equals("\\N")) language.put(Long.parseLong(c[0]), c[4]);
          }
        }
      }
    }

    @Override
    protected void map(Void k, Group row, Context ctx) throws IOException, InterruptedException {
      if (row.getFieldRepetitionCount("repo_id") == 0) return;
      String lang = language.get(row.getLong("repo_id", 0));
      if (lang == null) return;
      key.set(dt + "\t" + lang + "\tE");
      ctx.write(key, ONE);
      if (isStar) {
        key.set(dt + "\t" + lang + "\tS");
        ctx.write(key, ONE);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getName());
    boolean q3 = args[0].equals("q3");
    conf.set("parquet.read.schema", q3 ? "message e { optional int64 repo_id; }" : "message e { optional int64 event_id; }");
    if (q3) conf.set("pulse.catalogue", args[3]);
    Job job = Job.getInstance(conf, "pulse-bench-" + args[0]);
    job.setJarByClass(BenchJobs.class);
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setMapperClass(q3 ? LanguageMapper.class : TypeDayMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class SumReducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable total = new LongWritable();

    @Override
    protected void reduce(Text k, Iterable<LongWritable> vs, Context ctx) throws IOException, InterruptedException {
      long s = 0;
      for (LongWritable v : vs) s += v.get();
      total.set(s);
      ctx.write(k, total);
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new BenchJobs(), args));
  }
}
