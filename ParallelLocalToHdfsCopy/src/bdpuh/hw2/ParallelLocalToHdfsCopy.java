/*
 * Tony Florida
 * 2014-10-09
 * JHU - Big Data Processing with Hadoop
 * Assignment #2
 */
package bdpuh.hw2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 * Main class for assignment #2.
 * Copies files from local file system to Hadoop file system
 */
public class ParallelLocalToHdfsCopy {
    
    static private void usage()
    {
        System.out.println("Usage: This program takes 3 arguments");
        System.out.println("  1) absolute directory name on the local filesystem");
        System.out.println("  2) absolute directory name in the HDFS filesystem");
        System.out.println("  3) number of threads to use for copying");
    }
    
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        // Print app bannar
        System.out.println("Hadoop Assignment 2");
        System.out.println("Tony Florida");
        System.out.println("2013-10-08");
        
        // 2) Input checking on 3 arguments
        if(args.length != 3)
        {
            usage();
        }
     
        // 3) Check existance of local directory
        final Configuration conf = new Configuration();
        LocalFileSystem localFs = FileSystem.getLocal(conf);
        FileStatus[] localStatus = null;
        Path srcPath = new Path(args[0]);
        try {
             localStatus = localFs.listStatus(srcPath);
        } catch (IOException ex) {
            System.out.println("Source directory does not exist.");
            return;
        }

        // 4) Checks existance of remote directory
        FileSystem destFs = FileSystem.get(conf);
        Path destPath = new Path(args[1]);
        try {
            FileStatus[] destStatus = destFs.listStatus(destPath);
            System.out.println("Destination directory already exists. Please delete before running the program");
            return;
        } catch (IOException ex) {
            try {
                // Create the directory since it does not exist
                destFs.mkdirs(destPath);
            } catch (IOException e) {
                System.out.println("Error making directory");
            }
        }
        
        //Streams declaration
        FSDataOutputStream fsDataOutputStream = null;
        
        for(int i = 0; i < localStatus.length; i++) {
            // 7)Ignore directories and only copy files
            if(!localStatus[i].isDir())
            {
                String fileString = srcPath + "/" + localStatus[i].getPath().getName();
                Path fileToRead = new Path(fileString);
                
                // Open a File for Reading
                final FSDataInputStream fSDataInputStream = localFs.open(fileToRead);
        
                
                // 6) Open a File for Writing .gz file
                System.out.println(fileToRead.getName());
                Path compressedFileToWrite = new Path(destPath + "/" + fileToRead.getName() + ".gz");

                try {
                    fsDataOutputStream = destFs.create(compressedFileToWrite);
                } catch (IOException ex) {
                    System.out.println(ex);
                }
                
                // 6) Get Compressed FileOutputStream
                CompressionCodec compressionCodec = new GzipCodec();

                final CompressionOutputStream compressedOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);

                // 5) Copy in parallel
                new Thread(new Runnable()
                {
                    public void run()
                    {
                        try {
                            IOUtils.copyBytes(fSDataInputStream, compressedOutputStream, conf);
                        } catch (IOException ex) {
                            System.out.println(ex);
                        }
                    }
                }).start();
                
                // Close streams
                try {
                    fSDataInputStream.close();
                    compressedOutputStream.close();
                } catch (IOException | NullPointerException ex) {
                    System.out.println(ex);
                }
            }
        }

        // Close streams and filesystems
        try {
            fsDataOutputStream.close();
            localFs.close();
            destFs.close();
        } catch (IOException | NullPointerException ex) {
            System.out.println(ex);
        }
        System.out.println("Goodbye");
    }
}