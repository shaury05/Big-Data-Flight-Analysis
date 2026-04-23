import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OnTimeProbability {
public static class ProbabilityMapper extends Mapper<Object, Text, Text, IntPairWritable> {
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] parts = line.split(",");

// ASA Data Expo 2009 format:
// Year=0, UniqueCarrier=8, ArrDelay=14
if (parts.length > 14 && !parts[0].equals("Year")) {
try {
String carrier = parts[8];
String arrDelayStr = parts[14];

if (!arrDelayStr.equals("NA") && !carrier.equals("NA")) {
int delay = (int) Double.parseDouble(arrDelayStr); // Handle potential decimals
int onTime = (delay <= 10) ? 1 : 0;
context.write(new Text(carrier), new IntPairWritable(onTime, 1));
}
} catch (NumberFormatException e) {
// Ignore malformed lines
}
}
}
}
public static class ProbabilityReducer extends Reducer<Text, IntPairWritable, Text, DoubleWritable> {
public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
int totalOnTime = 0;
int totalFlights = 0;
for (IntPairWritable val : values) {
totalOnTime += val.getFirst();
totalFlights += val.getSecond();
}
if (totalFlights > 0) {
context.write(key, new DoubleWritable((double) totalOnTime / totalFlights));
}
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "On Time Probability");
job.setJarByClass(OnTimeProbability.class);
job.setMapperClass(ProbabilityMapper.class);
job.setReducerClass(ProbabilityReducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntPairWritable.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}