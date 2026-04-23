import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancellationReason {
public static class CancelMapper extends Mapper<Object, Text, Text, IntWritable> {
private final static IntWritable one = new IntWritable(1);
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] parts = line.split(",");

// Cancelled=21, CancellationCode=22
if (parts.length > 22 && !parts[0].equals("Year")) {
String cancelled = parts[21];
String code = parts[22];

if (cancelled.equals("1") && !code.equals("NA") && !code.isEmpty()) {
context.write(new Text(code), one);
}
}
}
}
public static class CancelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) { sum += val.get(); }
context.write(key, new IntWritable(sum));
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Cancellation Reason");
job.setJarByClass(CancellationReason.class);
job.setMapperClass(CancelMapper.class);
job.setReducerClass(CancelReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}