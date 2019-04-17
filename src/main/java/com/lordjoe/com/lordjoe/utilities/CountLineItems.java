package com.lordjoe.com.lordjoe.utilities;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class CountLineItems {
    public static int countLineItems(File fileOrDir, String lineText)
    {
        if(fileOrDir.isDirectory()) {
            int ret = 0;
            File[] files = fileOrDir.listFiles();
            if(files != null){
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    ret += doCountLineItems(file,lineText);

                }
             }
            return ret;
        }
        else {
            return doCountLineItems(fileOrDir,lineText);
        }
    }

    private static int doCountLineItems(File fileOrDir, String lineText) {
        try {
            int ret = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(fileOrDir));
            String line = rdr.readLine();
            while(line != null) {
                if(countLine(line,lineText))
                    ret++;
                line = rdr.readLine();
            }
            return ret;
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    private static boolean countLine(String line, String lineText) {
        return line.contains(lineText);
    }

    public static void main(String[] args) {
        File f = new File(args[0]);
        String delimiter = args[1];
        if("mslevel".equals(delimiter))
            delimiter = "msLevel=\"2\"";
        int count = countLineItems(f,delimiter);
        System.out.println(args[0] + " contains " + count + " items");
    }
}
