package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<Text, Rating, Text, IntWritable>{

    IntWritable count = new IntWritable();
    int sum_rating = 0;
    
    @Override
    protected void reduce(Text key, Iterable<Rating> ratings, Context context) throws IOException, InterruptedException {

        for(Rating rating : ratings)
        {
            sum_rating += rating.getRating();
        }
        count.set(sum_rating);
        context.write(key, count);
    }
}
