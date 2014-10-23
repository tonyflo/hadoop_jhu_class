package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    IntWritable one = new IntWritable(1);
    Text rating = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println("in setup of " + context.getTaskAttemptID().toString());
        String filename = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println("in stdout" + context.getTaskAttemptID().toString() + " " + filename);
        System.err.println("in stderr" + context.getTaskAttemptID().toString());
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //StringTokenizer tokenizer = new StringTokenizer(line);
        String lineArray[] = line.split("\t");
        //while(tokenizer.hasMoreTokens()) {
            rating.set(lineArray[2]);
            context.write(rating, one);
        //}
    }
}
