import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements WritableComparable<IntPairWritable> {
private int first;
private int second;

public IntPairWritable() {}
public IntPairWritable(int first, int second) { set(first, second); }
public void set(int first, int second) { this.first = first; this.second = second; }
public int getFirst() { return first; }
public int getSecond() { return second; }

@Override
public void write(DataOutput out) throws IOException {
out.writeInt(first);
out.writeInt(second);
}
@Override
public void readFields(DataInput in) throws IOException {
first = in.readInt();
second = in.readInt();
}
@Override
public int hashCode() { return first * 163 + second; }
@Override
public boolean equals(Object o) {
if (o instanceof IntPairWritable) {
IntPairWritable tp = (IntPairWritable) o;
return first == tp.first && second == tp.second;
}
return false;
}
@Override
public int compareTo(IntPairWritable tp) {
int cmp = Integer.compare(first, tp.first);
if (cmp != 0) return cmp;
return Integer.compare(second, tp.second);
}
@Override
public String toString() { return first + "\t" + second; }
}