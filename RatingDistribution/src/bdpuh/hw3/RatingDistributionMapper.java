/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author hdadmin
 */
public class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    IntWritable one = new IntWritable(1);
    Text word = new Text();
    
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
        //line = line.replaceAll("[.,;?\"!\\/]", "");
        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}
