/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw2;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 *
 * @author hdadmin
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
        Configuration conf = new Configuration();
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
        
        
        for(int i = 0; i < localStatus.length; i++) {
            // Ignore directories and only copy files
            if(!localStatus[i].isDir())
            {
                String fileString = srcPath + "/" + localStatus[i].getPath().getName();
                Path fromPath = new Path(fileString);
                destFs.copyFromLocalFile(fromPath, destPath);
            }
        }

        
        //Close the filesystems
        localFs.close();
        destFs.close();
        System.out.println("Goodbye");
    }
}