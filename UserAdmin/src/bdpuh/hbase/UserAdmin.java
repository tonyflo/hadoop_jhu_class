/*
 * Tony Florida
 * 2014-12-04
 * JHU - Big Data Processing with Hadoop
 * HBase Project
 */
package bdpuh.hbase;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
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
    private static void put(String[] args) throws IOException
    {
        String row_id = args[1];
        String email = args[2];
        String password = args[3];
        String status = args[4];
        String date_of_birth = args[5];
        String security_question = args[6];
        String security_answer = args[7];
        
        //validate input
        if(!status.equals("married") && !status.equals("single"))
        {
            System.out.println("Error: status must be 'single' or 'married'");
            return;
        }
        
        Put put = new Put(toBytes(row_id));
        
        put.add(toBytes("creds"), toBytes("email"), toBytes(email));
        put.add(toBytes("creds"), toBytes("password"), toBytes(password));
        put.add(toBytes("prefs"), toBytes("status"), toBytes(status));
        put.add(toBytes("prefs"), toBytes("date_of_birth"), toBytes(date_of_birth));
        put.add(toBytes("prefs"), toBytes("security_question"), toBytes(security_question));
        put.add(toBytes("prefs"), toBytes("security_answer"), toBytes(security_answer));
        
        hTable.put(put);
    }
    
    private static void delete(String row_id) throws IOException
    {
        Delete delete = new Delete(toBytes(row_id));
        hTable.delete(delete);
    }
    
    private static void show(String row_id) throws IOException
    {
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            if(Bytes.toString(rr.getRow()).equals(row_id))
            {
                print(rr);
                break;
            }
        }
    }
    
    private static void listall() throws IOException
    {
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            System.out.println("--------------------------");
            print(rr);
        }
    }
    
    private static void login(String[] args) throws IOException
    {
        String row_id = args[1];
        String password = args[2];
        String ip = args[3];
        
        // Get date and time
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:SS");
        Date date_obj = new Date();
        String date = dateFormat.format(date_obj);
        String time = timeFormat.format(date_obj);
        
        //Check if password matches
        Get get = new Get(toBytes(row_id));
        get.addColumn(toBytes("creds"), toBytes("password"));
        Result result = hTable.get(get);
        String given_password = Bytes.toString(result.value());
        
        if(given_password.equals(password))
        {
            //Update table
            Put put = new Put(toBytes(row_id));

            put.add(toBytes("lastlogin"), toBytes("ip"), toBytes(ip));
            put.add(toBytes("lastlogin"), toBytes("date"), toBytes(date));
            put.add(toBytes("lastlogin"), toBytes("time"), toBytes(time));
            put.add(toBytes("lastlogin"), toBytes("success"), toBytes("yes"));

            hTable.put(put);
        }
       

    }

    private static void print(Result result)
    {
        //creds
        System.out.println("rowid=" + Bytes.toString(result.getRow()));
        byte[] email = result.getValue(toBytes("creds"), toBytes("email"));
        System.out.println("creds:email=" + Bytes.toString(email));
        byte[] password = result.getValue(toBytes("creds"), toBytes("password"));
        System.out.println("creds:password=" +Bytes.toString(password));
        
        //prefs
        byte[] status = result.getValue(toBytes("prefs"), toBytes("status"));
        System.out.println("prefs:status=" +Bytes.toString(status));
        byte[] date_of_birth = result.getValue(toBytes("prefs"), toBytes("date_of_birth"));
        System.out.println("prefs:date_of_birth=" +Bytes.toString(date_of_birth));
        byte[] security_question = result.getValue(toBytes("prefs"), toBytes("security_question"));
        System.out.println("prefs:security_question=" +Bytes.toString(security_question));
        byte[] security_answer = result.getValue(toBytes("prefs"), toBytes("security_answer"));
        System.out.println("prefs:security_answer=" +Bytes.toString(security_answer));

        //lastlogin
        byte[] ip = result.getValue(toBytes("lastlogin"), toBytes("ip"));
        System.out.println("last_login:ip=" +Bytes.toString(ip));
        byte[] date = result.getValue(toBytes("lastlogin"), toBytes("date"));
        System.out.println("last_login:date=" +Bytes.toString(date));
        byte[] time = result.getValue(toBytes("lastlogin"), toBytes("time"));
        System.out.println("last_login:time=" +Bytes.toString(time));
        byte[] success = result.getValue(toBytes("lastlogin"), toBytes("success"));
        System.out.println("last_login:success=" +Bytes.toString(success));
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
                put(args);

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
                show(args[1]);

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
                login(args);

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
