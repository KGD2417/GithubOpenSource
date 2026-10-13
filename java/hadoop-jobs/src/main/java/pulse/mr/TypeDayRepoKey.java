package pulse.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite map-output key (dt, eventType, repoId).
 * Sort order dt -> eventType -> repoId means one reducer sees each (dt, eventType) group as a
 * contiguous run of keys, which is what lets it keep a single top-N heap per group.
 */
public class TypeDayRepoKey implements WritableComparable<TypeDayRepoKey> {
  String dt = "";
  String eventType = "";
  long repoId;

  public TypeDayRepoKey() {}

  TypeDayRepoKey(String dt, String eventType, long repoId) {
    set(dt, eventType, repoId);
  }

  void set(String dt, String eventType, long repoId) {
    this.dt = dt;
    this.eventType = eventType;
    this.repoId = repoId;
  }

  boolean sameGroup(String otherDt, String otherType) {
    return dt.equals(otherDt) && eventType.equals(otherType);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(dt);
    out.writeUTF(eventType);
    out.writeLong(repoId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dt = in.readUTF();
    eventType = in.readUTF();
    repoId = in.readLong();
  }

  @Override
  public int compareTo(TypeDayRepoKey o) {
    int c = dt.compareTo(o.dt);
    if (c == 0) c = eventType.compareTo(o.eventType);
    return c != 0 ? c : Long.compare(repoId, o.repoId);
  }

  @Override
  public int hashCode() {
    return (dt.hashCode() * 31 + eventType.hashCode()) * 31 + Long.hashCode(repoId);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TypeDayRepoKey && compareTo((TypeDayRepoKey) o) == 0;
  }

  @Override
  public String toString() {
    return dt + "\t" + eventType + "\t" + repoId;
  }
}
