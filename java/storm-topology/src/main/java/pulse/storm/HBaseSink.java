package pulse.storm;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Batches mutations and writes them on flush (every second via tick tuples).
 *   live_minute   (minute % 8)|yyyyMMddHHmm   t:<EventType>, b:bot, b:human   atomic Increments
 *   live_topk     yyyyMMddHHmm (window end)    k:01..k:10 = "repo_id|name|count"
 *   spike_alerts  (Long.MAX - window_end)|repo k:name,count,mean,z    (reverse time: newest first)
 * The minute key is time-ordered, so it gets a (minute % 8) bucket prefix to spread writes over regions.
 */
public class HBaseSink implements Sink {
  static final DateTimeFormatter MINUTE = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);
  private transient Connection conn;
  private transient Table minute, topk, alerts;
  private transient List<Row> mMin, mTop, mAlert;

  static String minuteKey(long ms) {
    long m = ms / 60_000L;
    return (m % 8) + "|" + MINUTE.format(Instant.ofEpochMilli(ms));
  }

  @Override
  public void open(Map<String, Object> conf) {
    try {
      Configuration c = HBaseConfiguration.create();
      c.set("hbase.zookeeper.quorum", String.valueOf(conf.getOrDefault("pulse.zookeeper", "zookeeper")));
      conn = ConnectionFactory.createConnection(c);
      minute = conn.getTable(TableName.valueOf("live_minute"));
      topk = conn.getTable(TableName.valueOf("live_topk"));
      alerts = conn.getTable(TableName.valueOf("spike_alerts"));
      mMin = new ArrayList<>();
      mTop = new ArrayList<>();
      mAlert = new ArrayList<>();
    } catch (Exception e) {
      throw new IllegalStateException("HBase connection failed", e);
    }
  }

  @Override
  public void minute(long minuteStart, Map<String, Long> counts) {
    Increment inc = new Increment(Bytes.toBytes(minuteKey(minuteStart)));
    counts.forEach((k, v) -> {
      if (k.startsWith("_")) inc.addColumn(Bytes.toBytes("b"), Bytes.toBytes(k.substring(1)), v);
      else inc.addColumn(Bytes.toBytes("t"), Bytes.toBytes(k), v);
    });
    mMin.add(inc);
  }

  @Override
  public void topK(long windowEnd, List<String> top) {
    Put p = new Put(Bytes.toBytes(MINUTE.format(Instant.ofEpochMilli(windowEnd))));
    for (int i = 0; i < top.size(); i++) {
      p.addColumn(Bytes.toBytes("k"), Bytes.toBytes(String.format("%02d", i + 1)), Bytes.toBytes(top.get(i)));
    }
    if (!top.isEmpty()) mTop.add(p);
  }

  @Override
  public void alert(long windowEnd, long repoId, String repoName, long count, double mean, double z) {
    Put p = new Put(Bytes.toBytes((Long.MAX_VALUE - windowEnd) + "|" + repoId));
    byte[] a = Bytes.toBytes("a");
    p.addColumn(a, Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(repoName)));
    p.addColumn(a, Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(count)));
    p.addColumn(a, Bytes.toBytes("mean"), Bytes.toBytes(String.format("%.2f", mean)));
    p.addColumn(a, Bytes.toBytes("z"), Bytes.toBytes(String.format("%.1f", z)));
    p.addColumn(a, Bytes.toBytes("minute"), Bytes.toBytes(MINUTE.format(Instant.ofEpochMilli(windowEnd))));
    mAlert.add(p);
  }

  @Override
  public void flush() {
    try {
      if (!mMin.isEmpty()) minute.batch(mMin, new Object[mMin.size()]);
      if (!mTop.isEmpty()) topk.batch(mTop, new Object[mTop.size()]);
      if (!mAlert.isEmpty()) alerts.batch(mAlert, new Object[mAlert.size()]);
      mMin.clear();
      mTop.clear();
      mAlert.clear();
    } catch (Exception e) {
      throw new IllegalStateException("HBase write failed", e);   // tuples stay un-acked -> replayed
    }
  }
}
