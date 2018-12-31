package com.lordjoe.comet;

import java.io.*;

/**
 * com.lordjoe.comet.CommandLineExecutor
 * User: Steve
 * Date: 11/10/2018
 */
public class CommandLineExecutor implements Serializable {


    private static boolean showCommandLine = true;
    private static boolean showSystemErr = true;
    private static boolean showSystemOut = false;

    public static boolean isShowCommandLine() {
        return showCommandLine;
    }

    public static void setShowCommandLine(boolean showCommandLine) {
        CommandLineExecutor.showCommandLine = showCommandLine;
    }

    public static boolean isShowSystemErr() {
        return showSystemErr;
    }

    public static void setShowSystemErr(boolean showSystemErr) {
        CommandLineExecutor.showSystemErr = showSystemErr;
    }

    public static boolean isShowSystemOut() {
        return showSystemOut;
    }

    public static void setShowSystemOut(boolean showSystemOut) {
        CommandLineExecutor.showSystemOut = showSystemOut;
    }

    public static boolean executeCommandLine(String... args ) throws IOException {
        ProcessBuilder p = new ProcessBuilder(  args);
        String commandLine = buildCommandLine(p);
        if(isShowCommandLine())
          System.out.println("Started  " + commandLine);


        Process process = p.start();
        try {
            String line;
            StringBuilder output = new StringBuilder();
            StringBuilder errors = new StringBuilder();

            BufferedReader bri = new BufferedReader
                    (new InputStreamReader(process.getInputStream()));
            BufferedReader bre = new BufferedReader
                    (new InputStreamReader(process.getErrorStream()));
            int lineCount = 0;
            while ((line = bri.readLine()) != null) {
                output.append(line);
                output.append("\n");

                lineCount++;
              //   System.out.println(line);
            }
            bri.close();
            while ((line = bre.readLine()) != null) {
                errors.append(line);
                errors.append("\n");
                lineCount++;
              //   System.out.println(line);
            }
            bre.close();
            int result = process.waitFor();
            int returnVal = process.exitValue();

            if(isShowSystemErr() && errors.length() >  0)
                System.out.println("Errors  " + errors );
            if(isShowSystemOut())
                System.out.println("Output  " + output );

            if(isShowCommandLine())
                System.out.println("Ended  " + commandLine + "Result" + result );

            return returnVal == 0;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        }
    }


    public static String buildCommandLine( ProcessBuilder p )
    {
        StringBuilder sb = new StringBuilder();
        for (String s : p.command()) {
           boolean inQuotes = s.contains(" ");
           if(inQuotes)
               sb.append('\"');
           sb.append(s);
            if(inQuotes)
                sb.append('\"');
            sb.append(" ");
        }
         
        return sb.toString();
    }


}
