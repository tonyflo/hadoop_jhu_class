package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    IntWritable rating = new IntWritable();
    Text movie_id = new Text();
    
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
        String lineArray[] = line.split("\t");
        
        //Set Value: rating
        rating.set(Integer.parseInt(lineArray[2]));
        
        //Set Key: movie id
        movie_id.set(lineArray[1]);
        
        context.write(movie_id, rating);
    }
}
