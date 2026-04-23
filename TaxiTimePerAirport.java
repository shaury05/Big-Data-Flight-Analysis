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

public class TaxiTimePerAirport {
public static class TaxiMapper extends Mapper<Object, Text, Text, IntPairWritable> {
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] parts = line.split(",");

// Origin=16, Dest=17, TaxiIn=19, TaxiOut=20
if (parts.length > 20 && !parts[0].equals("Year")) {
try {
String origin = parts[16];
String dest = parts[17];
String taxiInStr = parts[19];
String taxiOutStr = parts[20];

if (!taxiOutStr.equals("NA") && !origin.equals("NA")) {
int taxiOut = (int) Double.parseDouble(taxiOutStr);
context.write(new Text(origin), new IntPairWritable(taxiOut, 1));
}
if (!taxiInStr.equals("NA") && !dest.equals("NA")) {
int taxiIn = (int) Double.parseDouble(taxiInStr);
context.write(new Text(dest), new IntPairWritable(taxiIn, 1));
}
} catch (NumberFormatException e) {
// Ignore errors
}
}
}
}
public static class TaxiReducer extends Reducer<Text, IntPairWritable, Text, DoubleWritable> {
public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
int totalTaxiTime = 0;
int totalFlights = 0;
for (IntPairWritable val : values) {
totalTaxiTime += val.getFirst();
totalFlights += val.getSecond();
}
if (totalFlights > 0) {
context.write(key, new DoubleWritable((double) totalTaxiTime / totalFlights));
}
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Taxi Time");
job.setJarByClass(TaxiTimePerAirport.class);
job.setMapperClass(TaxiMapper.class);
job.setReducerClass(TaxiReducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntPairWritable.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}