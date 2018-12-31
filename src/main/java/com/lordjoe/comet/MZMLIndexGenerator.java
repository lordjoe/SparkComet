package com.lordjoe.comet;


import com.lordjoe.distributed.XMLUtilities;

import java.util.ArrayList;
import java.util.List;

/**
 * com.lordjoe.comet.MZMLIndexGenerator
 * User: Steve
 * Date: 12/19/2018
 */
public class MZMLIndexGenerator {
    public static final int GLOBAL_OFFSET = 4;
    public static final int INDEX_OFFSET = 2;


    public static String generateIndex(String baseFile)  {
        StringBuilder sb = new StringBuilder();
        String[] lines = baseFile.split("\n");
        List<IdentifiedOffset> offsets = new ArrayList<>();
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if(line.contains("<scan"))
                offsets.add(generateIdentifiedOffset(sb,line) );
            sb.append(line);
            sb.append("\n");
            if(line.contains("</msRun>"))
                break;

        }
        appendIndices(sb,offsets);
        appendFooter(sb);

        return sb.toString();
    }

    private static void appendIndices(StringBuilder sb, List<IdentifiedOffset> offsets) {
        int globalOffset = sb.length();
        sb.append(" <index name=\"scan\" >\n") ;
        for (IdentifiedOffset offset : offsets) {
            sb.append("    <offset id=\"");
            sb.append(Integer.toString(offset.id)) ;
            sb.append("\">") ;
            sb.append(Integer.toString(offset.offset)) ;
            sb.append("</offset>\n");
        }
        sb.append(" </index>\n");
        sb.append("  <indexOffset>");
        sb.append(Integer.toString(globalOffset)) ;
        sb.append("</indexOffset>\n") ;


    }

    private static void appendFooter(StringBuilder sb) {
        sb.append("<sha1>0</sha1>\n") ; // no one cares
        sb.append("</mzXML>\n") ; // no one cares
    }

    private static IdentifiedOffset generateIdentifiedOffset(StringBuilder sb, String line) {
        int offset = sb.length();
        int id = Integer.parseInt(XMLUtilities.extractTag("num",line));
        return new IdentifiedOffset(id,offset + GLOBAL_OFFSET);
    }

    public static class IdentifiedOffset  {
        public final int id;
        public final int  offset;

        public IdentifiedOffset(int id, int offset) {
            this.id = id;
            this.offset = offset;
        }
    }

}
