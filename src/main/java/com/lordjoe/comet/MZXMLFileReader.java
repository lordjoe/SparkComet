package com.lordjoe.comet;

import com.lordjoe.utilities.FileUtilities;

import java.io.*;

/**
 * com.lordjoe.comet.MZXMLReader
 * User: Steve
 * Date: 11/6/2018
 */
public class MZXMLFileReader  implements Serializable {

    public static String readHeader(LineNumberReader rdr,String[] firstLine) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line = rdr.readLine();
        while(line != null) {
            if(line.contains("<scan"))
                break;
            sb.append(line) ;
            sb.append("\n");
            line = rdr.readLine();
        }
        if(line == null)
            throw new IllegalArgumentException("bad file header not terminated");
        firstLine[0] = line;
        return sb.toString();
    }

    public static String readScan(LineNumberReader rdr,String line) throws IOException {
         int scancount = 0;
         StringBuilder sb = new StringBuilder();
          while(line != null) {
              if(line.contains("<scan"))
                  scancount++;
              sb.append(line) ;
            sb.append("\n");
            if(line.contains("</scan>")) {
                scancount--;
                if(scancount <= 0)
                    break;
            }
            line = rdr.readLine();
        }
        if(line == null)
            throw new IllegalArgumentException("bad scan not terminated");
        return sb.toString();
    }

    public static MZXMLFile readMZXMLFile(LineNumberReader rdr)  throws IOException   {
        String[] firstLine = new String[1];
        String header = readHeader(rdr,firstLine);
        MZXMLFile file = new MZXMLFile(header);
        String line = firstLine[0];
        while(!line.contains("<scan") ) {
            line = rdr.readLine();
        }
        while(line.contains("<scan"))  {
            String scan = readScan(  rdr,  line);
            file.addScan(scan);
            line = rdr.readLine();
        }
       return file;
    }

    public static void main(String[] args) throws IOException {
        LineNumberReader rdr = new LineNumberReader(new FileReader(new File(args[0])));
            MZXMLFile file =  readMZXMLFile(rdr);
         String text = file.makeIndexedString();
        FileUtilities.writeFile(args[1],text);
    }

}
