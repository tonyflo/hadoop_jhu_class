/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hdadmin
 */
public class RatingDistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        i = 0;
        for(IntWritable val : values) {
            i = i + 1;
        }
        count.set(i);
        context.write(key, count);
    }
}
