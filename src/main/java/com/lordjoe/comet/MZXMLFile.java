package com.lordjoe.comet;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * com.lordjoe.comet.MZXMLFile
 * User: Steve
 * Date: 11/6/2018
 */
public class MZXMLFile  implements Serializable {

    public static Integer scanToId(String scan)   {
          int start = scan.indexOf("num=\"") + "num=\"".length();
          int end =  scan.indexOf("\"",start + 1);
        String substring = scan.substring(start, end);
        return new Integer(substring);
    }

    public final String header;
    public final Map<Integer,String> scans = new HashMap<>();

    public MZXMLFile(String header) {
        this.header = header;
    }

    public void addScan(String scan)   {
        scans.put(scanToId(  scan),scan);
        addSubscans(scan);
    }

    public int getNumberScans() {
        return scans.size();
    }

    private void addSubscans(String scan) {
        String[] lines = scan.split("\n") ;
        boolean inScan = false;
         // ignore start
        StringBuilder internalScan = new StringBuilder();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if(line.contains("<scan"))  {
                internalScan.setLength(0);
                internalScan.append(line);
                internalScan.append("/n");
                inScan = true;
             }
             else {
                 if(inScan)    {
                     internalScan.append(line);
                     internalScan.append("/n");
                     if(line.contains("</scan>")) {
                         addScan(internalScan.toString());
                         internalScan.setLength(0);
                         inScan = false;
                     }
                 }
            }

        }
    }

    public static final int GLOBAL_OFFSET = 4;
    public static final int INDEX_OFFSET = 2;
    public static final String RUN_END = "  </msRun>\n";



    public String makeIndexedString() {
         StringBuilder out = new StringBuilder();
        List<Integer> ids = new ArrayList<>(scans.keySet());
        Collections.sort(ids);
        out.append(header);
        int offset = header.length();
        for (Integer id : ids) {
            String scan = scans.get(id) ;
              out.append(scan);
            offset += scan.length();
        }
        out.append(RUN_END);


        return MzXMLUtilities.generateIndexedData(out.toString());
    }


    public void makeIndexedFileOld(Appendable out) throws IOException
    {
        List<IdentifiedOffset> offsets = new ArrayList<>();
        List<Integer> ids = new ArrayList<>(scans.keySet());
        Collections.sort(ids);
        out.append(header);
        int offset = header.length();
        for (Integer id : ids) {
            String scan = scans.get(id) ;
            offsets.add(new IdentifiedOffset(id,offset + GLOBAL_OFFSET)) ;
            out.append(scan);
            offset += scan.length();
        }
        out.append(RUN_END);
        if(out instanceof  CharSequence)    {
            offset = ((CharSequence)out).length()  + GLOBAL_OFFSET + 1;
        }
        else {
            offset += RUN_END.length()  + INDEX_OFFSET;
        }
        out.append("<index name=\"scan\">\n");
        for (IdentifiedOffset identifiedOffset : offsets) {
            out.append("    <offset id=\"");
            out.append(Integer.toString(identifiedOffset.id)) ;
            out.append("\">") ;
            out.append(Integer.toString(identifiedOffset.offset)) ;
            out.append("</offset>\n");
        }
        out.append(" </index>\n");
        out.append("  <indexOffset>");
        out.append(Integer.toString(offset)) ;
        out.append("</indexOffset>\n") ;

        out.append("  <sha1>0</sha1>\n");
        out.append("</mzXML>");

    }

    public class IdentifiedOffset  {
        public final int id;
        public final int  offset;

        public IdentifiedOffset(int id, int offset) {
            this.id = id;
            this.offset = offset;
        }
    }
}
