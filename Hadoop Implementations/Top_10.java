import java.util.*;
import java.lang.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.w3c.dom.Text;
public class Top_10 {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\t");

            String key1 = mydata[0];
            if (mydata.length > 1) {
                String[] secondary_data = mydata[1].split(",");
                for (String data : secondary_data) {
                    String key2 = data;
                    String mainString = "";

                    if (key1.compareTo(key2) < 0) {
                        mainString = key1 + key2;
                    } else {
                        mainString = key2 + key1;
                    }

                    Text keyPair = new Text(mainString);
                    //Text val = new Text(String.join(",", secondary_data));
                    Text val = new Text(mydata[1]);

                    context.write(keyPair, val);
                }
            }

        }
    }

    public static class Map2
            extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\t");
            String key1 = mydata[0];
            if (mydata.length > 1) {
                int count = 0;
                String[] secondary_data = mydata[1].split(",");
                count=secondary_data.length;
                count--;

                String textValue = key1 + ':' + mydata[1];

                LongWritable keyPair = new LongWritable(count);

                Text val = new Text(textValue);

                context.write(keyPair, val);
            }
        }


    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashSet<String> h = new HashSet<>();
            boolean flag = true;
            String result = "";

            for (Text t : values) {
                String[] a = t.toString().split(",");
                for (int i = 0; i < a.length; i++) {

                    if (flag)
                        h.add(a[i]);
                    else if (h.contains(a[i]))
                        result += a[i] + ", ";
                }
                flag = false;
            }
            context.write(key, new Text(result));
        }
    }

    public static class Reduce2
            extends Reducer<LongWritable, Text, Text, Text> {
        private LongWritable result = new LongWritable();
        private Text word = new Text();
        public static int count = 10;

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            for (Text t : values) {
                String[] data = t.toString().split(":");
                if (count-- > 0) {
                    String ky = data[0];
                    String ve = key + "\t " + data[1];
                    context.write(new Text(ky), new Text(ve));
                } else
                    break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            System.err.println(otherArgs[0]);
            System.err.println(otherArgs.length);
            System.err.println("Usage: Top_10 <in> <out>");
            System.exit(-2);
        }
        {
            // create a job with name "Q2MRA"
            Job job = Job.getInstance(conf, "Q2MRA");
            job.setJarByClass(Top_10.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

            // set output key type
            job.setOutputKeyClass(Text.class);
            // set output value type
            job.setOutputValueClass(Text.class);
            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
            //Wait till job completion
            if (!job.waitForCompletion(true))
                System.exit(1);
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        {
            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "Q2MRB");
            job1.setJarByClass(Top_10.class);
            job1.setMapperClass(Map2.class);
            job1.setReducerClass(Reduce2.class);
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setNumReduceTasks(1);
            job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
            // set output key type
            job1.setOutputKeyClass(Text.class);
            // set output value type
            job1.setOutputValueClass(Text.class);
            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job1, new Path(otherArgs[2]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
            //Wait till job completion
            if (!job1.waitForCompletion(true))
                System.exit(1);
        }
        System.exit(0);
    }
}