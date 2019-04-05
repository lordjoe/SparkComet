package com.lordjoe.comet;

import java.io.*;
import java.util.UUID;

import static com.lordjoe.comet.CommandLineExecutor.executeCommandLine;

/**
 * com.lordjoe.comet.MultiFileExecutor
 * User: Steve
 * Date: 11/14/2018
 */
public class MultiFileExecutor {
    public static final MultiFileExecutor[] EMPTY_ARRAY = {};

    private static String simpleConcat(String s, String s1) {
        return s + "\n" + s1;
    }

    public static synchronized boolean osIsWindows()
    {

        String osName = System.getProperty("os.name").toLowerCase();
        return  (osName.indexOf("windows") != -1);

    }


    public static File makeTempFile( )     {
        try {
            String prefix = UUID.randomUUID().toString();
            String suffix = ".txt";

            // by calling deleteOnExit the temp file is deleted when the jvm is
            // shut down
            File tempFile2 = File.createTempFile(prefix, suffix);
            tempFile2.deleteOnExit();
            return tempFile2;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
    private static String fileCallingCat(String s0, String s1) {
        try {
            File f1 = makeTempFile( );
            writeFile(f1, s0);
            File f2 = makeTempFile( ) ;
            writeFile(f2, s1);
            File f3 = makeTempFile( );

            boolean success = false;
            String ret = null;
            String f1path = f1.getAbsolutePath();
            String f2path = f2.getAbsolutePath();
            String f3Path = f3.getAbsolutePath();
            String command = "cat " + f1path + " "  + f2path + " > "  + f3Path;
            StringBuilder output = new StringBuilder();
            StringBuilder errors = new StringBuilder();
            if(osIsWindows())
                success = CommandLineExecutor.executeCommandLine(output,errors,"cmd","/c",command);
            else
                success = CommandLineExecutor.executeCommandLine(output,errors,"/bin/sh","-c",command);


            if(success) {
                ret = readFile(f3);
                //             System.out.println(ret);
            }

            f1.delete();
            f2.delete();
            f3.delete();
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * { method
     *
     * @param FileName name of file to create
     * @param data     date to write
     * @return true = success
     * }
     * @name writeFile
     * @function write the string data to the file Filename
     */
    public static boolean writeFile(File f, String data) {
        try {
            PrintWriter out = new PrintWriter(new FileWriter(f));
            if (out != null) {
                out.print(data);
                out.close();
                return (true);
            }
            return (false);
            // failure
        }
        catch (Exception ex) {
            throw new RuntimeException(ex) ;
        }
    }
    /**
     * { method
     *
     * @param FileName name of file to create
     * @param data     date to write
     * @return true = success
     * }
     * @name writeFile
     * @function write the string data to the file Filename
     */
    public static String readFile(File f ) {
        try {
            LineNumberReader rdr = new LineNumberReader(new FileReader(f));
            StringBuilder sb = new StringBuilder();
            String line = rdr.readLine();
            while(line != null) {
                sb.append(line);
                sb.append("\n");
                line = rdr.readLine();
            }
            rdr.close();
            return sb.toString();
            // failure
        }
        catch (Exception ex) {
            throw new RuntimeException(ex) ;
        }
    }

    public static String concat(String s, String s1) {
        //   return simpleConcat(  s,   s1);
        return fileCallingCat(  s,   s1);
    }
}
