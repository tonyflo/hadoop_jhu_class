/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package useradmin;

/**
 *
 * @author hdadmin
 */
public class UserAdmin {
    
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
        index = 0;
        System.out.println(args[0]);
        if(args.length != args_len[index])
        {
            System.out.println(usage[index]);
            return false;
        }
        return true;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {      

        int cmd_index = -1;
        
        switch(args[0])
        {
            case "add":
                cmd_index = 0;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }

                break;
            case "delete":
                cmd_index = 1;
                if (!check_usage(args, cmd_index))
                {
                    break;
                }

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
    }    
}
