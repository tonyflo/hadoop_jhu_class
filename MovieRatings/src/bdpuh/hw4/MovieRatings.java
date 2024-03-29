/*
 * Tony Florida
 * 2014-10-29
 * JHU - Big Data Processing with Hadoop
 * Assignment 4
 */
package bdpuh.hw4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieRatings {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Job wordCountJob = null;
        
        Configuration conf = new Configuration();
        
        //Job input compression
        //conf.setStrings("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec");
        
        // Job output compression
        conf.setBoolean("mapreduce.output.compress", true);
        conf.setBoolean("mapred.output.compress", true);
        conf.setStrings("mapreduce.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setStrings("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");        
        
        try{
            wordCountJob = Job.getInstance(conf, "MovieRatings");
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        //Specify the input path
        try{
            FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        } catch (IOException ex) {
            System.out.println(ex);
        }

        //set reducer
        wordCountJob.setPartitionerClass(MovieRatingsPartitioner.class);
        
        // set the input data format
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        
        // set the mapper and reducer class
        wordCountJob.setMapperClass(MovieRatingsMapper.class);
        wordCountJob.setReducerClass(MovieRatingsReducer.class);
        
        // set the jar file
        wordCountJob.setJarByClass(bdpuh.hw4.MovieRatings.class);
        
        // set the output path
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        
        // set the output data format
        wordCountJob.setOutputFormatClass(TextOutputFormat.class);
        
        // set the output key and value class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(Text.class);
        
        // set number of reducers
        wordCountJob.setNumReduceTasks(2);
        
        try {
            wordCountJob.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            System.out.println(ex);
        }
    }
}
