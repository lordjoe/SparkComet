package com.lordjoe.comet;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class CountSpectra {

    public static void main(String[] args) {
        for (String arg : args) {
            countSpectra(new File(arg));
        }
    }

    private static void countSpectra(File file) {
        if(file.getName().toLowerCase().endsWith(".mzxml")) {
            countMZXMLSpectra(file);
            return;
        }
        if(file.getName().toLowerCase().endsWith(".fasta")) {
            countFastapectra(file);
            return;
        }
        if(file.getName().toLowerCase().endsWith(".xml")) {
            countPepXMLSpectra(file);
            return;
        }
    }

    private static void countPepXMLSpectra(File file) {
        try {
            int count = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(file));
            String line = rdr.readLine();
            while(line != null) {
                if(line.contains("<spectrum_query"))
                    count++;
                line = rdr.readLine();

            }
            handleCount(count,file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void countFastapectra(File file) {
        try {
            int count = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(file));
            String line = rdr.readLine();
            while(line != null) {
                if(line.startsWith(">"))
                    count++;
                line = rdr.readLine();

            }
            handleCount(count,file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void countMZXMLSpectra(File file) {
        try {
            int count = 0;
            int count2 = 0;
            LineNumberReader rdr = new LineNumberReader(new FileReader(file));
            String line = rdr.readLine();
            int mslevel = 0;
            while(line != null) {
                if(line.contains("msLevel=\"")) {
                    String rem = line.trim().replace("msLevel=\"","");
                     rem = rem.replace("\"","");
                     mslevel = Integer.parseInt(rem);
                  }
                if( line.contains("</scan>")) {
                    count2++;
                    if(mslevel == 2)
                        count++;
                }
                 line = rdr.readLine();
            }
            System.out.println("total " + count2 + " ms2 " + count);
            handleCount(count,file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void handleCount(int count, File file) {
        System.out.println("File " + file.getName() + " contains " + count);
    }
}
