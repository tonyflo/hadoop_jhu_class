/*
 * Tony Florida
 * 2014-10-22
 * JHU - Big Data Processing with Hadoop
 * Assignment 3
 */
package bdpuh.hw3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RatingDistribution {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Job wordCountJob = null;
        
        Configuration conf = new Configuration();
        try{
            wordCountJob = Job.getInstance(conf, "RatingDistribution");
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        //Specify the input path
        try{
            FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        // set the input data format
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        
        // set the mapper and reducer class
        wordCountJob.setMapperClass(RatingDistributionMapper.class);
        wordCountJob.setReducerClass(RatingDistributionReducer.class);
        
        // set the jar file
        wordCountJob.setJarByClass(bdpuh.hw3.RatingDistribution.class);
        
        // set the output path
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        
        // set the output data format
        wordCountJob.setOutputFormatClass(TextOutputFormat.class);
        
        // set the output key and value class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        
        try {
            wordCountJob.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            System.out.println(ex);
        }
    }
}
