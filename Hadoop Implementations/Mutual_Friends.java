import java.util.*;
import java.io.*;
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
public class Mutual_Friends{
    public static class Map
            extends Mapper<LongWritable,Text,Text,Text>{
        private final static IntWritable one=new IntWritable(1);
        private Text word=new Text();
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
            String[] mydata = value.toString().split("\t");

            String key1=mydata[0];
            if(mydata.length>1)
            {
                String[] secondary_data = mydata[1].split(",");
                for(String data: secondary_data){
                    String key2 = data;
                    String mainString="";

                    if(key1.compareTo(key2) < 0){
                        mainString = key1+":"+key2;
                    }
                    else{
                        mainString = key2+":"+key1;
                    }

                    Text keyPair = new Text(mainString);
                    Text val = new Text(String.join(",", secondary_data));

                    context.write(keyPair,val);
                }
            }

        }

    }
    //(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)

    public static class Reduce
            extends Reducer<Text,Text,Text,Text>{
        private IntWritable result= new IntWritable();
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            HashSet<String> h = new HashSet<>();
            boolean flag = true;
            String result = "";

            for(Text t : values){
                String[] a = t.toString().split(",");
                for(int i=0; i<a.length; i++){
                    if(flag)
                        h.add(a[i]);
                    else if(h.contains(a[i]))
                        result += a[i] + ",";
                }
                flag = false;
            }
            String a=key.toString();
            String[] s_data = a.split(":");
            int one = Integer.parseInt(s_data[0]);
            int two = Integer.parseInt(s_data[1]);

            if ((one == 0 && two == 1) ||
                    (one == 20 && two == 28193) ||
                    (one == 1 && two == 29826)  ||
                    (one == 28041 && two == 28056) ||
                    (two == 6222 && one == 19272)
            ) {
                context.write(key, new Text(result));
            }

        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf =new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: Mutual Friends <in> <out>");
            System.exit(2);
        }
        Job job =new Job(conf, "mutualfriend");
        job.setJarByClass(Mutual_Friends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job , new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}