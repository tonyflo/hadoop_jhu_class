package bdpuh.hw4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovieRatingsPartitioner extends Partitioner<Text, Text> {
    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    public int getPartition(Text key, Text value, int numReducers) {
        int int_key = Integer.parseInt(key.toString());
        if(int_key >= 500) {
            return 0;
        } else {
            return 1;
        }
    }
    
}
