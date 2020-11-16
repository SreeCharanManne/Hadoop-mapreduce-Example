package hadoop;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class GradingDriver {
    public static class GradingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
        private static int mapNumber = 0;
        public void map(LongWritable longWritable, Text text, OutputCollector<Text,IntWritable>output,Reporter reporter) throws IOException{
            String line = text.toString();
            String[] keyValue = line.split("\t");
            String studentId = keyValue[0];
            int score = Integer.parseInt(keyValue[1]);
            // context.write(C, D);
            output.collect(new Text(studentId), new IntWritable(score));
        }
    
      
    }
    public static class GradingReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
        private static int reduceNumber = 0;
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable>output,Reporter reporter) throws IOException{
            String studentId = key.toString();
            int totalScore = 0;
            while(values.hasNext()) {
                int score = (int)values.next().get();
                totalScore += score;
            }
            output.collect(new Text(studentId), new IntWritable(totalScore));
        }

    }
    
    public static void main(String[] args) throws Exception {
        JobConf job = new JobConf(GradingDriver.class);
        job.setJobName("Grading_Driver");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(GradingMapper.class);
        job.setCombinerClass(GradingReducer.class);
        job.setReducerClass(GradingReducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        JobClient.runJob(job);
    }
}
