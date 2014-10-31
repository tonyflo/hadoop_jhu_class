package bdpuh.hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<Text, Text, Text, Text>{
    int running_rating_sum = 0;
    int num_ratings = 0;
    int unique_ratings = 0;
    double average_rating = 0;
    Text value = new Text();
    
    List<Integer> ints = new ArrayList<Integer>();
    
    //determine if value is unique
    private int uniqueValue(int value)
    {
        for(int i = 0; i < ints.size(); i++)
        {
            if(ints.get(i) == value)
            {
                return 0;
            }
        }
        return 1;
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        running_rating_sum = 0;
        num_ratings = 0;
        unique_ratings = 0;
        average_rating = 0;
        
        ints.clear();
        
        for(Text val : values) {
            String rating = val.toString();
            String rate[] = rating.split("\t");
            
            //sum up ratings
            running_rating_sum += Integer.parseInt(rate[1]);
            
            //count ratings
            num_ratings++;
            
            //is user id unique
            unique_ratings += uniqueValue(Integer.parseInt(rate[0]));
        }
   
        //calculate average
        average_rating = (double) running_rating_sum / num_ratings;
   
        //formate value string
        String str = ""+ average_rating + "\t" + unique_ratings + "\t" + running_rating_sum;
        
        value.set(str);
        
        context.write(key, value);
    }
}
