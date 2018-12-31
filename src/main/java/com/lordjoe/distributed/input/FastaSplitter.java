package com.lordjoe.distributed.input;

import com.lordjoe.utilities.FileUtilities;

import java.io.*;
import java.util.UUID;

/**
 * com.lordjoe.distributed.input.MZXMLSplitter
 * User: Steve
 * Date: 9/24/2014
 */
public class FastaSplitter implements Serializable {
    public static final String SCAN_START = ">";
    public static final String EXTENSION = ".fasta";

    public static final int DEFAULT_SPECTRA_SPLIT = 5000;

    private static File buildFile(File directory) {
        return new File(directory, UUID.randomUUID().toString() + EXTENSION);
    }


    public FastaSplitter() {
    }


    public static void splitFasta(File input, File outDir, int maxScans) throws IOException {
        FileUtilities.expungeDirectory(outDir);
        boolean ok = outDir.mkdirs();
        String[] lineHolder = new String[1];
        LineNumberReader rdr = new LineNumberReader(new FileReader(input));
        lineHolder[0] = rdr.readLine();
        while (readAndSaveScans(outDir, lineHolder, rdr, maxScans)) ;
        rdr.close();
    }

    public static boolean readAndSaveScans(File outDir, String[] lineHolder, LineNumberReader rdr, int maxScans) throws IOException {
        String line = lineHolder[0];
        if (line == null)
            return false;
        if (!line.contains(SCAN_START))
            return false;
        int numberScans = 0;
        StringBuilder sb = new StringBuilder();
        lineHolder[0] = line;
        while (numberScans < maxScans) {
            String result = readNextScan(rdr, lineHolder);
            if (result != null) {
                sb.append(result);
                numberScans++;
            }
            else {
                break;
            }
        }
        if (sb.length() > 0) {
            saveFasta(outDir, sb.toString());
            return true;
        }
        return false;
    }

    public static String readNextScan(LineNumberReader rdr, String[] lineHolder) throws IOException {
        String line = lineHolder[0];
        if (line == null)
            return null;
        if (!line.contains(SCAN_START))
            return null;
        StringBuilder sb = new StringBuilder();
        while (line != null) {
            sb.append(line);
            sb.append("\n");
            line = rdr.readLine();
            lineHolder[0] = line;
            if (line == null)
                break;

            if (line.contains(SCAN_START)) {
                return sb.toString();
            }
        }

        return null;
    }

    public static void saveFasta(File outDir, String fasta) throws IOException {
        File out = buildFile(outDir);
        System.out.println(out.getName());
        StringBuilder sb = new StringBuilder();
        FileUtilities.writeFile(out, fasta);
    }

    public static void main(String[] args) throws IOException {
        File input = new File(args[0]);
        File outDir = new File(args[1]);
        int split = DEFAULT_SPECTRA_SPLIT;
        if(args.length > 2)
            split = Integer.parseInt(args[2]);
        splitFasta(input, outDir, split);


    }
}
