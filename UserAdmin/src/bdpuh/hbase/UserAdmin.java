/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;


/**
 *
 * @author hdadmin
 */
public class UserAdmin {
    
    // Global HBase declarations
    static Configuration conf;
    static HTable hTable;
    
    // Usage
    private static int[] args_len = {8, 2, 2, 1, 4};
    
    private static String[] usage =
    {
        "Usage: java UserAdmin add kss k.s@gmail.com mypasswd married 1970/06/03 \"favorite color\" \"red\"",
        "Usage: java UserAdmin delete kss",
        "Usage: java UserAdmin show kss",
        "Usage: java UserAdmin listall",
        "Usage: java UserAdmin login kss mypasswd 128.220.101.100",
        "Usage: java UserAdmin <cmd> [options]"
    };

    private static boolean check_usage(String[] args, int index)
    {
        System.out.println(args[0]);
        if(args.length != args_len[index])
        {
            System.out.println(usage[index]);
            return false;
        }
        return true;
    }
    
    // Command line commands
    private static void put(String row_id, String email, String password) throws IOException
    {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "User");
        
        Put put = new Put(toBytes(row_id));
        
        put.add(toBytes("creds"), toBytes("email"), toBytes(email));
        put.add(toBytes("creds"), toBytes("email"), toBytes(password));
        hTable.put(put);
    }
    
    private static void delete(String row_id) throws IOException
    {
        Delete delete = new Delete(toBytes(row_id));
        hTable.delete(delete);
    }
    
    private static void listall() throws IOException
    {

        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            print(rr);
        }
    }

    private static void print(Result result)
    {
        System.out.println("--------------------------");
        System.out.println("RowID: " + Bytes.toString(result.getRow()));
        byte[] email = result.getValue(toBytes("creds"), toBytes("email"));
        System.out.println("creds:email=" + Bytes.toString(email));
        byte[] password = result.getValue(toBytes("creds"), toBytes("password"));
        System.out.println("creds:password=" +Bytes.toString(password));
        
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {  
        
        conf = HBaseConfiguration.create();
        hTable = new HTable(conf, "User");

        int cmd_index = -1;
        
        switch(args[0])
        {
            case "add":
                cmd_index = 0;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }
                put(args[1], args[2], args[3]);

                break;
            case "delete":
                cmd_index = 1;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }
                delete(args[1]);

                break;
            case "show":
                cmd_index = 2;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }

                break;
            case "listall":
                cmd_index = 3;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }
                
                listall();

                break;
            case "login":
                cmd_index = 4;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }

                break;
            default:
                cmd_index = 5;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }

                break;
        }
        
        hTable.close();
    }    
}
