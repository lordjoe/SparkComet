package com.lordjoe.comet;

import com.lordjoe.distributed.SparkFileSaver;
import com.lordjoe.distributed.XMLUtilities;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.systemsbiology.hadoop.IParameterHolder;
import org.systemsbiology.xtandem.XTandemHadoopUtilities;
import org.systemsbiology.xtandem.XTandemMain;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * com.lordjoe.distributed.hydra.scoring.CometConsolidator     \
 * Responsible for writing an output file
 * User: Steve
 * Date: 10/20/2014
 */
public class CometConsolidator implements Serializable {
    public static final String FORCED_OUTPUT_NAME_PARAMETER = "org.systemsbiology.xtandem.outputfile";

    private final String header;
    private final String footer;
    private final XTandemMain application;

    public CometConsolidator(final String pHeader, final String pFooter, final XTandemMain pApplication) {
        footer = pFooter;
        application = pApplication;
        header = pHeader;
    }



    public static String buildDefaultFileName(IParameterHolder pParameters) {
        // force output name so it is known to hadoop caller
        String absoluteName = pParameters.getParameter(FORCED_OUTPUT_NAME_PARAMETER);
        if (absoluteName != null)
            return absoluteName;

        String name = pParameters.getParameter("output, path");
        // little hack to separate real tandem and hydra results
        if (name != null)
            name = name.replace(".tandem.xml", ".hydra.xml");
        if ("full_tandem_output_path".equals(name)) {
            return "xtandem_" + getDateHash() + ".xml"; // todo do better
        }

        //  System.err.println("No File name forced building file name");
        if ("yes".equals(pParameters.getParameter("output, path hashing"))) {
            assert name != null;
            final int index = name.lastIndexOf(".");
            String extension = name.substring(index);
            String dataHash = getDateHash();
            name = name.substring(0, index) + "." + dataHash + ".t" + extension;
        }
        return name;
    }

    // make sure that the output date is hashed only once
    private static String gDateHash;

    public static synchronized void clearDateHash() {
        gDateHash = null;
    }

    protected static synchronized String getDateHash() {
        if (gDateHash == null)
            gDateHash = buildDateHash();
        return gDateHash;
    }

    private static String buildDateHash() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy_MM_DD_HH_mm_ss");
        return df.format(new Date());
    }


    public XTandemMain getApplication() {
        return application;
    }

    /**
     * write scores into a file
     *
     * @param scans
     */
    public void writeScores(JavaRDD<String> textOut) {

        textOut = sortByIndex(textOut);
        String outputPath =  buildDefaultFileName(application);
        Path path = XTandemHadoopUtilities.getRelativePath(outputPath);

//        List<String> headerList = new ArrayList<>();
//        headerList.add(header);
//
//        List<String> footerList = new ArrayList<>();
//        footerList.add(footer);
//
//        JavaSparkContext sparkContext = SparkUtilities.getCurrentContext();
//        JavaRDD<String> headerRdd = sparkContext.parallelize(headerList);
//        JavaRDD<String> footerRdd = sparkContext.parallelize(footerList);
//
//        textOut = headerRdd.union(textOut);
//        textOut = textOut.union(footerRdd);


        SparkFileSaver.saveAsFile(path, textOut,header,footer);
     }


    public static   JavaRDD  sortByIndex(JavaRDD<String> bestScores) {
        JavaPairRDD<String, String> byIndex = bestScores.mapToPair(new TextToScanID());
        JavaPairRDD<String, String> sortedByIndex = byIndex.sortByKey();

        return sortedByIndex.values();
   
    }

     static class TextToScanID   implements PairFunction<String, String, String> {
        @Override
        public Tuple2<String, String> call(final String t) throws Exception {
            String id = XMLUtilities.extractTag("spectrum",t);
            return new Tuple2<String, String>(id, t);
        }
    }


}
