import java.util.*;
import java.io.*;
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
public class In_Map_Reduce {

    protected static String Path;
    private static HashMap<Integer, String> hmap = new HashMap<Integer, String>();

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String l;
            try {
                FileReader file = new FileReader("/Users/srichaitrakareddy/Desktop/userdata.txt");
                BufferedReader b = new BufferedReader(file);
                while ((l = b.readLine()) != null) {
                    String[] us_data = l.split(",");

                    hmap.put(Integer.parseInt(us_data[0]), new String(us_data[1] + "," + us_data[4]));
                }
            } catch (Exception e) {

                System.out.println("It is failing to read and perform operations on the second block");
            }

        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //ArrayList<String> table = data();
            String[] L = value.toString().split("\t");
            int key1 = Integer.parseInt(L[0]);
            if (L.length > 1) {
                String[] values = L[1].split(",");
                ArrayList<String> val  = new ArrayList<String>();
                for(String data: values) {
                    val.add(data+","+hmap.get(Integer.parseInt(data)));
                }
                for (String data : values) {
                    if (key1 < Integer.parseInt(data))
                        context.write(new Text(key1 + "," + data), new Text(String.join(":", val)));
                    else
                        context.write(new Text(data + "," + key1), new Text(String.join(":", val)));// set word as each input keyword
                    // create a pair <keyword, 1>
                }
            }
        }

        }

    // create a pair <key,occurences>
public static class Reduce
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0; // initialize the sum for each keyword
        ArrayList<String> join = new ArrayList<String>(Arrays.asList(values.iterator().next().toString().split(":")));
        for (Text v: values) {
            join.retainAll(new ArrayList<String>(Arrays.asList(v.toString().split(":"))));
        }
        for(String j:join) {
            if(j!=null || j!="") {
                sum++;
            }
        }
        if(sum>0) {
            context.write(key, new Text(String.join(";",join)));
        }

    }
}
public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println(otherArgs[0]);
            System.err.println(otherArgs.length);
            System.err.println("Usage: Reducer_side_join <in> <out>");
            System.exit(2);
        }

        // create a job with name "Question3"
        Job job = new Job(conf, "Reducer_side_join");
        job.setJarByClass(In_Map_Reduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        Path= otherArgs[1];
        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
