import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageDistance {

public static class DistMapper extends Mapper<Object, Text, Text, DoubleWritable> {
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] parts = line.split(",");

// Indexes based on ASA Data Expo 2009:
// Year=0, UniqueCarrier=8, Origin=16, Dest=17, Distance=18
if (parts.length > 18 && !parts[0].equals("Year")) {
try {
String carrier = parts[8];
String origin = parts[16];
String dest = parts[17];
String distStr = parts[18];

if (!carrier.equals("NA") && !origin.equals("NA") && !dest.equals("NA") && !distStr.equals("NA")) {
double distance = Double.parseDouble(distStr);
// Key format: "Airline Origin Dest"
context.write(new Text(carrier + "\t" + origin + "\t" + dest), new DoubleWritable(distance));
}
} catch (NumberFormatException e) {
// Ignore bad lines
}
}
}
}

public static class DistReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
// Map to store results for sorting in cleanup
private Map<String, Double> allRoutes = new HashMap<>();

public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
double sum = 0;
int count = 0;
for (DoubleWritable val : values) {
sum += val.get();
count++;
}
if (count > 0) {
double avg = sum / count;
allRoutes.put(key.toString(), avg);
}
}

@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
// Sort the map by Value (Distance) in Descending order
List<Map.Entry<String, Double>> list = new ArrayList<>(allRoutes.entrySet());

// Custom comparator for descending sort values
Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
return o2.getValue().compareTo(o1.getValue());
}
});

// Output Top 3
context.write(new Text("--- Top 3 Longest Average Distances ---"), null);
context.write(new Text("Airline\tOrigin\tDest"), new DoubleWritable(0.0)); // Header

int count = 0;
for (Map.Entry<String, Double> entry : list) {
if (count >= 3) break;
context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
count++;
}
}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Average Distance");
job.setJarByClass(AverageDistance.class);
job.setMapperClass(DistMapper.class);
job.setReducerClass(DistReducer.class);

job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(DoubleWritable.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);

// Force 1 reducer so specific 'Top 3' logic works globally
job.setNumReduceTasks(1);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}