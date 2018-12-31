package com.lordjoe.distributed;

import java.util.ArrayList;
import java.util.List;

/**
 * org.systemsbiology.xml.XMLUtilities
 *
 * @author Steve Lewis
 * @date Feb 1, 2011
 */
public class XMLUtilities
{



    public static List<String> extractXMLTags(String xml,String tag)  {
        List<String> ret = new ArrayList<>() ;
        extractXMLTagsInternal(ret,xml,tag,0);
         return ret;

    }

    private static void extractXMLTagsInternal(List<String> holder,String xml,String tag,int start)  {
        String tagStart = "<"  + tag;
        String tagEnd = "</"  + tag + ">";
        int startLoc = xml.indexOf(tagStart,start);
        if(startLoc == -1)
            return;
        int endLoc = xml.indexOf(tagEnd,start);
        if(endLoc == -1)
            throw new IllegalArgumentException("unclused tag " + tag);
        endLoc += tagEnd.length();
        holder.add(xml.substring(startLoc,endLoc));
        extractXMLTagsInternal( holder, xml, tag,endLoc);

    }

    public static String extractTag(String tagName, String xml) {
         String startStr = tagName + "=\"";
         int index = xml.indexOf(startStr);
         if (index == -1) {
             String xmlLc = xml.toLowerCase();
             if (xmlLc.equals(xml))
                 throw new IllegalArgumentException("cannot find tag " + tagName);
             String tagLc = tagName.toLowerCase();
               return  extractTag(tagLc,xmlLc); // rey without case
         }
         index += startStr.length();
         int endIndex = xml.indexOf("\"", index);
         if (endIndex == -1)
             throw new IllegalArgumentException("cannot terminate tag " + tagName);
         // new will uncouple old and long string
         return new String(xml.substring(index, endIndex));
     }

    public static void outputLine() {
        System.out.println();
    }


    public static void outputText(String text) {
        System.out.print(text);
    }

     /**
     * in string item find a single instance of   attribute and replace its value
     * so
     *     <tag number="x" > </tag>  called with  attribute number and value y
     *     will return
     *      <tag number="y" > </tag>
     * @param item
     * @param attribute
     * @return
     */
    public static String substituteAttributeValue(String item,String attribute,String newvalue) {
        String base = attribute + "=\"";
        int index1 = item.indexOf(base);
        if(index1 == -1)
            throw new IllegalArgumentException(item + " dose not contain " + base);
        index1 += base.length();
        int index2 = item.indexOf("\"",index1);
        if(index2 == -1)
            throw new IllegalArgumentException( base + " tag not closed");
        int index3 = item.indexOf(base,index2);
        if(index1 == -1)
            throw new IllegalArgumentException(item + " has multiple tags " + base);

        StringBuilder sb = new StringBuilder();
        sb.append(item.substring(0,index1));
        sb.append(newvalue);
        sb.append(item.substring(index2));

        return sb.toString();
    }
}
