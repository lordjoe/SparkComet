package com.lordjoe.comet;

import com.lordjoe.distributed.XMLUtilities;
import com.lordjoe.distributed.input.MZXMLSplitter;
import com.lordjoe.utilities.FileUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * com.lordjoe.comet.MzXMLUtilities
 * User: Steve
 * Date: 12/20/2018
 */
public class MzXMLUtilities {


    public static Map<Integer,Integer>   getScanIndices(String fileWithIndices)
    {
        String[] lines = fileWithIndices.split("\n") ;
        Map<Integer,Integer> ret = new HashMap<>() ;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if(line.contains("<offset id="))  {
                String id = XMLUtilities.extractTag("id",line);
                String value = line.substring(line.indexOf("\">") + 2 , line.lastIndexOf("<")) ;
                ret.put(Integer.parseInt(id),Integer.parseInt(value));
            }
        }
        return ret;
    }


    public static int   getIndexOffset(String fileWithIndices)
    {
        int start = fileWithIndices.indexOf("<indexOffset>") + "<indexOffset>".length();
        int end = fileWithIndices.indexOf("</indexOffset>")  ;
        String value = fileWithIndices.substring(start,end) ;
        return Integer.parseInt(value);
    }


    public static String generateIndexedData(String fileWithoutIndices)  {
        fileWithoutIndices = fileWithoutIndices.replace("\r","") ;
        Set<Integer>   seenIDs = new HashSet<>();
        Map<Integer,Integer> indices = new HashMap<>() ;
        StringBuilder sb = new StringBuilder();
        String[] lines = fileWithoutIndices.split("\n") ;
        for (int i = 0; i < lines.length; i++) {
            int startOffset = sb.length();
            String line = lines[i];
              if(line.contains("<scan"))  {
                String id = XMLUtilities.extractTag("num",line);
                  int idNum = Integer.parseInt(id);
                  if(seenIDs.contains(idNum)) {
                    String oldId = id;
                     id = getNewID(seenIDs);
                     idNum = Integer.parseInt(id);
                    line = line.replace("num=\"" + oldId,"num=\"" +id) ;
                   }
                  sb.append(line);
                  sb.append("\n");
                  int offset =  startOffset + line.indexOf("<scan");
                       indices.put(idNum,offset);
            }
              else {
                  sb.append(line);
                  sb.append("\n");
                  if(line.contains("</msRun>")) {
                       break;
                  }
              }
        }
        appendScanIndices(sb,indices);

        return sb.toString();
    }

    private static String getNewID(Set<Integer> seenIDs) {
        int newId = 10000;
        while(seenIDs.contains(newId))
            newId++;
        return Integer.toString(newId);
    }

    public static final int INDEX_OFFSET = 2;
    public static void appendScanIndices(StringBuilder sb, Map<Integer, Integer> indices) {
        int indexOffset = sb.length() + INDEX_OFFSET;
        sb.append("    <index name=\"scan\">\n") ;
        List<Integer>  orderedScans = new ArrayList<>(indices.keySet()) ;
        Collections.sort(orderedScans);


        for (Integer orderedScan : orderedScans) {
            int value = indices.get(orderedScan);
            sb.append("        <offset id=\"") ;
            sb.append(orderedScan);
            sb.append("\">");
            sb.append(value);
            sb.append("</offset>\n");
        }

        sb.append("  </index>\n");
        sb.append("    <indexOffset>") ;
        sb.append(indexOffset);
        sb.append("</indexOffset>\n");

        sb.append("    <sha1>0</sha1>\n" + "</mzXML>\n");
    }

    /**
     * create a directory with splits of the mzxml file
     * Note I cannot see how to parallelize this step as mxZML files are notoriously hard to parse from the\middle
     * @param originalFile
     * @return
     */
    public static List<File> splitMzXMLFile(File originalFile,int numberScans)  {

        try {
            Path tempPath = Files.createTempDirectory(originalFile.getName() + "splits");
            File dir = tempPath.toFile();
            dir.deleteOnExit();
            List<File> files = MZXMLSplitter.splitMZXML(originalFile, dir, numberScans);
            for (File file : files) {
                 file.deleteOnExit();
            }
            return files;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public static void generateIndex(String file)
    {
        File inp = new File(file);
        String fileWithoutIndices = FileUtilities.readInFile(inp);
        String outName = inp.getName().replace(".mzXML", "Indexed.mzXML");
        File out = new File(outName);
        String outData = generateIndexedData(fileWithoutIndices) ;
        FileUtilities.writeFile(out,outData);
    }


    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            generateIndex(arg);
        }
    }



}
