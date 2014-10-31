package bdpuh.hw4;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author hdadmin
 */
public class MovieRatingsPartitioner extends Partitioner<Text, IntWritable> {
    int i = 0;
    IntWritable count = new IntWritable();
    
    @Override
    public int getPartition(Text key, IntWritable value, int numReducers) {
        int int_key = Integer.parseInt(key.toString());
        if(int_key >= 500) {
            return 0;
        } else {
            return 1;
        }
    }
    
}
