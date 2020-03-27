import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Reducer_side_join {

    public static class Map1
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] Line = value.toString().split("\t");
            int k = Integer.parseInt(Line[0]);
            if (Line.length > 1) {
                String[] values = Line[1].split(",");
                for (String data : values)
                    context.write(new IntWritable(Integer.parseInt(data)), new Text("soc" + "," + k));
            }
        }
    }

    public static class Mapping_User
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] L = value.toString().split(",");
            int key1 = Integer.parseInt(L[0]);
            if (L.length > 1) {
                Calendar date = Calendar.getInstance(Locale.US);
                date.setTime(new Date(L[9]));
                Calendar sysdate = Calendar.getInstance();
                Integer age = sysdate.get(Calendar.YEAR)-date.get(Calendar.YEAR);
                if( (date.get(Calendar.MONTH) > sysdate.get(Calendar.MONTH)) || (sysdate.get(Calendar.MONTH) == date.get(Calendar.MONTH)) && (date.get(Calendar.DATE) > sysdate.get(Calendar.DATE)) )
                    age--;
                context.write(new IntWritable(key1), new Text("u"+","+age.toString()));
            }
        }
    }


    public static class Reducer_Age1
            extends Reducer<IntWritable,Text,Text,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //int sum = 0; // initialize the sum for each keyword
            ArrayList<String> i= new ArrayList<String>();
            Integer age=new Integer(0);
            for (Text v: values) {
                String[] l = v.toString().split(",");
                if(l[0].equals("u")) {
                    age = Integer.parseInt(l[1]);
                }
                else {
                    i.add(l[1]);
                }
                //intersection.retainAll(new ArrayList<String>(Arrays.asList(v.toString().split(","))));
            }
            for(String k: i) {
                context.write(new Text(k), new Text(age.toString()));
            }
            // create a pair <keyword, number of occurences>
        }
    }

    public static class Mapping_Average_Age
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] myLine = value.toString().split("\t");
            String key1 = myLine[0];
            if (myLine.length > 1) {
                Integer age = Integer.parseInt(myLine[1]);
                //String[] values = new String[]{count.toString(), key1, myLine[1]};
                context.write(new Text(key1), new Text("a"+";"+age.toString()));
            }
        }
    }

    public static class MapUserAddress
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] Line = value.toString().split(",");
            String address_of_individual="u"+";"+Line[3]+"; "+Line[4]+"; "+Line[5];
            context.write(new Text(Line[0]), new Text(address_of_individual));
        }
    }

    public static class ReduceAvgAge
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String address_of_individual = null;
            ArrayList<Double> ages = new ArrayList<Double>();
            int sum_value = 0, count_value=0;
            for(Text s: values) {
                String[] line = s.toString().split(";");
                if (line[0].equals("u")) {
                    address_of_individual = line[1];
                }
                else {
                    ages.add(Double.parseDouble(line[1]));
                }
            }
            for(Double d: ages) {
                sum_value += d;
                count_value++;
            }
            Double average = sum_value*1.0/count_value;
            context.write(key, new Text(average.toString()+";"+address_of_individual));
        }
    }

    public static class MapAvgAgeSort
            extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] Line = value.toString().split("\t");
            String key1 = Line[0];
            if (Line.length > 1) {
                String[] line = Line[1].split(";");
                Double age = Double.parseDouble(line[0]);
                //String[] values = new String[]{count.toString(), key1, myLine[1]};
                context.write(new DoubleWritable(age), new Text(key1+";"+line[1]));
            }
        }
    }

    public static class Reducer_Average_Age_Sort
            extends Reducer<DoubleWritable,Text,Text,Text> {

        public static ArrayList<String> l = new ArrayList<String>(15);

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text i : values) {
                String item = i.toString()+";"+key.toString();
                if(l.size()<15) {
                    l.add(item);
                }
                else {
                    l.remove(0);
                    l.add(item);
                }
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(String s : l) {
                String[] b = s.split(";");
                context.write(new Text(b[0]+","+b[1]), new Text(b[2]));
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 6) {
            System.err.println(otherArgs[0]);
            System.err.println(otherArgs.length);
            System.err.println("Usage: Reducer_side_join <in> <out>");
            System.exit(-2);
        }
        {
            // create a job for part A
            Job job = Job.getInstance(conf, "Question_4A");
            job.setJarByClass(Reducer_side_join.class);
            job.setMapperClass(Map1.class);
            job.setMapperClass(Mapping_User.class);
            job.setReducerClass(Reducer_Age1.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

            // set output key type
            job.setOutputKeyClass(Text.class);
            // set output value type
            job.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job,new Path(otherArgs[1]),
                    TextInputFormat.class, Map1.class);
            MultipleInputs.addInputPath(job,new Path(otherArgs[2]),
                    TextInputFormat.class, Mapping_User.class);
            //set the HDFS path of the input data
            //FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
            //Wait till job completion
            if (!job.waitForCompletion(true))
                System.exit(1);
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        {
            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "Question_4B");
            job1.setJarByClass(Reducer_side_join.class);
            job1.setMapperClass(Mapping_Average_Age.class);
            job1.setMapperClass(MapUserAddress.class);
            job1.setReducerClass(ReduceAvgAge.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job1,new Path(otherArgs[3]),
                    TextInputFormat.class, Mapping_Average_Age.class);
            MultipleInputs.addInputPath(job1,new Path(otherArgs[2]),
                    TextInputFormat.class, MapUserAddress.class);
            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
            // set output key type
            job1.setOutputKeyClass(Text.class);
            // set output value type
            job1.setOutputValueClass(Text.class);
            //set the HDFS path of the input data
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4]));
            //Wait till job completion
            if (!job1.waitForCompletion(true))
                System.exit(1);
        }

        {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Q4C");
            job2.setJarByClass(Reducer_side_join.class);
            job2.setMapperClass(MapAvgAgeSort.class);
            job2.setReducerClass(Reducer_Average_Age_Sort.class);
            job2.setMapOutputKeyClass(DoubleWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
            job2.setSortComparatorClass(SortFloatComparator.class);
            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
            // set output key type
            job2.setOutputKeyClass(Text.class);
            // set output value type
            job2.setOutputValueClass(Text.class);
            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job2, new Path(otherArgs[4]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
            //Wait till job completion
            if (!job2.waitForCompletion(true))
                System.exit(1);
        }
        System.exit(0);

    }
}
class SortFloatComparator extends WritableComparator {
    protected SortFloatComparator() {
        super(FloatWritable.class, true);
    }
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        FloatWritable a1 = (FloatWritable)wc1;
        FloatWritable a2 = (FloatWritable)wc2;
        return -1*a1.compareTo(a2);
    }
}