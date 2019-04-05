package com.lordjoe.comet;

import com.lordjoe.distributed.XMLUtilities;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingPairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * com.lordjoe.comet.TagExtractionFunction
 * User: Steve
 * Date: 12/14/2018
 */
public class TagExtractionFunction  extends AbstractLoggingPairFlatMapFunction<String,String,String>  {
    public static final TagExtractionFunction[] EMPTY_ARRAY = {};

    public final String tag;
    public final String attribute;   // used as key

    public TagExtractionFunction(String spectrum_query,String keyAttribute) {
        tag = spectrum_query;
        attribute =  keyAttribute;
    }

    @Override
    public Iterator<Tuple2<String, String>> doCall(String s) throws Exception {
        List<Tuple2<String, String>> ret = new ArrayList<>();
        if(s == null)
            return ret.iterator();
        try {
            List<String> strings = XMLUtilities.extractXMLTags(s, tag);
            for (String value : strings) {
              String key =  XMLUtilities.extractTag(attribute,value);
              ret.add(new Tuple2<String, String>(key,value));
           }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret.iterator();
    }
}
