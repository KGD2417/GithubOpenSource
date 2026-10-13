package pulse.mahout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

/**
 * Glue around `mahout kmeans` (PLAN.md §7.1 bonus).
 *
 *   vectors     <in.tsv> <out.seq>            "repo_name \t f1 \t f2 ..." -> SequenceFile<Text, VectorWritable>
 *                                             of NamedVectors, so clustered points keep their repo name
 *   assignments <clusteredPoints-dir> <out.tsv> SequenceFile<IntWritable, WeightedPropertyVectorWritable>
 *                                             -> "repo_name \t cluster_id \t distance"
 */
public class KMeansIO {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    if (args.length == 3 && args[0].equals("vectors")) {
      int n = 0;
      try (BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1])), StandardCharsets.UTF_8));
           SequenceFile.Writer out = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(args[2])),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(VectorWritable.class))) {
        for (String line; (line = in.readLine()) != null; ) {
          String[] f = line.split("\t");
          double[] v = new double[f.length - 1];
          for (int i = 1; i < f.length; i++) v[i - 1] = Double.parseDouble(f[i]);
          out.append(new Text(f[0]), new VectorWritable(new NamedVector(new DenseVector(v), f[0])));
          n++;
        }
      }
      System.out.println("vectors written: " + n);
    } else if (args.length == 3 && args[0].equals("assignments")) {
      int n = 0;
      try (PrintWriter out = new PrintWriter(fs.create(new Path(args[2]), true))) {
        for (FileStatus st : fs.listStatus(new Path(args[1]))) {
          if (!st.getPath().getName().startsWith("part-")) continue;
          try (SequenceFile.Reader r = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()))) {
            IntWritable cluster = new IntWritable();
            WeightedPropertyVectorWritable point = new WeightedPropertyVectorWritable();
            while (r.next(cluster, point)) {
              String name = ((NamedVector) point.getVector()).getName();
              Text dist = point.getProperties() == null ? null : point.getProperties().get(new Text("distance"));
              out.println(name + "\t" + cluster.get() + "\t" + (dist == null ? "" : dist.toString()));
              n++;
            }
          }
        }
      }
      System.out.println("assignments written: " + n);
    } else {
      System.err.println("usage: KMeansIO vectors <in.tsv> <out.seq> | assignments <clusteredPoints> <out.tsv>");
      System.exit(2);
    }
  }
}
