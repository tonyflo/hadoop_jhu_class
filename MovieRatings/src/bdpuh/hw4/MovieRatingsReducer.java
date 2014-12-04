package bdpuh.hw4;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<Text, Text, Text, Text>{
    int running_rating_sum = 0;
    int num_ratings = 0;
    int unique_ratings = 0;
    double average_rating = 0;
    Text value = new Text();
    String title;
    String release;
    String url;
    
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
        title = "UNKNOWN";
        release = "UNKNOWN";
        url = "UNKNOWN";
        
        ints.clear();
        
        //array to hold all movie item info
        ArrayList al = new ArrayList();
        
        //read u.item
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path file = new Path("/movie-and-ratings/u.item");
        //Path file = new Path("hdfs://localhost:9000/movie-and-ratings/u.item");
        
        //InputStream fileStream = new FileInputStream("hdfs://localhost:9000/movie-and-ratings/u.item.gz");
        //InputStream gzipStream = new GZIPInputStream(fileStream);
        //Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
        
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
        //BufferedReader br = new BufferedReader(decoder);
        String line;
        line = br.readLine();
        while(line != null){
            al.add(line);
            line = br.readLine();
        }
        
        // find matching movie id
        for(int i = 0; i < al.size(); i++)
        {
            // parse this movie's info
            String movie[] = al.get(i).toString().split("\\|");
            if(movie[0].equals(key.toString()))
            {
                title = movie[1];
                release = movie[2];
                url = movie[4];
            }
        }
        
        //sum and count
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
   
        //format value string
        String str = title + "\t" + release + "\t" + url + "\t" + average_rating + "\t" + unique_ratings + "\t" + running_rating_sum;
        value.set(str);
        
        br.close();
        
        context.write(key, value);
        
        Counter counter = context.getCounter(MovieRatingsCounter.UNIQUE_MOVIES);
        counter.increment(1);
    }
}
