package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<Text, IntWritable, Text, Text>{
    int running_rating_sum = 0;
    int num_ratings = 0;
    double average_rating = 0;
    Text value = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        running_rating_sum = 0;
        num_ratings = 0;
        average_rating = 0;
        
        for(IntWritable val : values) {
            running_rating_sum = running_rating_sum + val.get();
            num_ratings++;
        }
   
        //calculate average
        average_rating = (double) running_rating_sum / num_ratings;
   
        //formate value string
        String str = ""+ average_rating + "\t" + "?" + "\t" + running_rating_sum;
        
        value.set(str);
        
        context.write(key, value);
    }
}
