package com.lordjoe.comet;

import com.lordjoe.distributed.SparkSpectrumUtilities;
import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.hydra.SparkMapReduceScoringHandler;
import com.lordjoe.distributed.input.MultiMZXMLScanInputFormat;
import com.lordjoe.distributed.spark.IdentityFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction2;
import com.lordjoe.distributed.spark.accumulators.AccumulatorUtilities;
import com.lordjoe.utilities.ElapsedTimer;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.systemsbiology.xtandem.XTandemHadoopUtilities;
import org.systemsbiology.xtandem.XTandemMain;
import scala.Tuple2;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * com.lordjoe.comet.SparkCometCaller
 * uses calls to comet directly
 * Date: 10/7/2014
 */
public class SparkCometCaller  implements Serializable {

    public static final int PROTEINS_TO_HANDLE = 2000;
    public static final int SPECTRA_TO_HANDLE = 500;

    public static final boolean DO_DEBUGGING_COUNT = true;

    private static boolean debuggingCountMade = DO_DEBUGGING_COUNT;

    public static boolean isDebuggingCountMade() {
        return debuggingCountMade;
    }

    public static void setDebuggingCountMade(final boolean pIsDebuggingCountMade) {
        debuggingCountMade = pIsDebuggingCountMade;
    }

    private static int maxBinSpectra = 30; // todo make this configurable

    public static int getMaxBinSpectra() {
        return maxBinSpectra;
    }

    public static void setMaxBinSpectra(int maxBinSpectra) {
        SparkCometCaller.maxBinSpectra = maxBinSpectra;
    }

    public static final int SPARK_CONFIG_INDEX = 0;
    public static final int TANDEM_CONFIG_INDEX = 1;
    public static final int SPECTRA_INDEX = 2;
    public static final int SPECTRA_TO_SCORE = Integer.MAX_VALUE;
    public static final String MAX_PROTEINS_PROPERTY = "com.lordjoe.distributed.hydra.MaxProteins";
    @SuppressWarnings("UnusedDeclaration")
    public static final String MAX_SPECTRA_PROPERTY = "com.lordjoe.distributed.hydra.MaxSpectra";
    @SuppressWarnings("UnusedDeclaration")
    public static final String SKIP_SCORING_PROPERTY = "com.lordjoe.distributed.hydra.SkipScoring";
    public static final String SCORING_PARTITIONS_SCANS_NAME = "com.lordjoe.distributed.max_scoring_partition_scans";
    public static final long MAX_SPECTRA_TO_SCORE_IN_ONE_PASS = Long.MAX_VALUE;



//
//    public static class PairCounter implements Comparable<PairCounter> {
//        public final BinChargeKey key;
//        public final long v1;
//        public final long v2;
//        public final long product;
//
//        public PairCounter(BinChargeKey pkey, final long pV1, final long pV2) {
//            v1 = pV1;
//            v2 = pV2;
//            key = pkey;
//            product = v1 * v2;
//        }
//
//        @Override
//        public int compareTo(final PairCounter o) {
//            return Long.compare(o.product, product);
//        }
//
//        public String toString() {
//            return key.toString() + "spectra " + Long_Formatter.format(v1) + " peptides " + Long_Formatter.format(v2) +
//                    " product " + Long_Formatter.format(product);
//
//        }
//    }
//


    public static SparkMapReduceScoringHandler buildCometScoringHandler(String arg) {
        Properties sparkPropertiesX = SparkUtilities.getSparkProperties();

        String pathPrepend = sparkPropertiesX.getProperty(SparkUtilities.PATH_PREPEND_PROPERTY);
        if (pathPrepend != null)
            XTandemHadoopUtilities.setDefaultPath(pathPrepend);

        String maxScoringPartitionSize = sparkPropertiesX.getProperty(SCORING_PARTITIONS_SCANS_NAME);


        String configStr = SparkUtilities.buildPath(arg);

        //Configuration hadoopConfiguration = SparkUtilities.getHadoopConfiguration();
        //hadoopConfiguration.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, 64 * 1024L * 1024L);

        //Configuration hadoopConfiguration2 = SparkUtilities.getHadoopConfiguration();  // did we change the original or a copy
        SparkMapReduceScoringHandler handler = new SparkMapReduceScoringHandler(configStr, false);
        XTandemMain application = handler.getApplication();

        return handler;
    }




    /**
     * score with a join of a List of peptides
     *
     * @param args
     */
    public static void scoreWithFiles(String[] args) {
         long totalSpectra = 0;

        // Force PepXMLWriter to load
            ElapsedTimer timer = new ElapsedTimer();
        ElapsedTimer totalTime = new ElapsedTimer();

        if (args.length < TANDEM_CONFIG_INDEX + 1) {
            System.out.println("usage sparkconfig configFile");
            return;
        }

  //      buildDesiredScoring(args);

        SparkUtilities.readSparkProperties(args[SPARK_CONFIG_INDEX]);

        SparkMapReduceScoringHandler handler = buildCometScoringHandler(args[TANDEM_CONFIG_INDEX]);

        XTandemMain scoringApplication = handler.getApplication();


        setDebuggingCountMade(scoringApplication.getBooleanParameter(SparkUtilities.DO_DEBUGGING_CONFIG_PROPERTY, false));

        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        
        Properties sparkProperties = SparkUtilities.getSparkProperties();
         for (Object key : sparkProperties.keySet()) {
            scoringApplication.setParameter(key.toString(),sparkProperties.getProperty(key.toString()));
        }

        String spectrumPath = scoringApplication.getSpectrumPath();
        System.out.println("Spectrum " + spectrumPath);
        String databaseName = scoringApplication.getDatabaseName();
        System.out.println("Database " + databaseName);


        String addedFiles = scoringApplication.getParameter("com.lordjoe.distributed.files", "");
        if(addedFiles.length() > 0)  {
            String[] split = addedFiles.split(";");
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                currentContext.addFile(s);
             }
        }


        String propBinStr = SparkUtilities.getSparkProperties().getProperty("com.lordjoe.comet.fastaBin");
        int proteinsToHandle = PROTEINS_TO_HANDLE;
        if(propBinStr != null)
            proteinsToHandle = Integer.parseInt(propBinStr);
        String databasePath =  databaseName + ".fasta";
        JavaRDD<String> fastas = SparkSpectrumUtilities.partitionFastaFile(databasePath,currentContext,proteinsToHandle).values();

        String binStr = SparkUtilities.getSparkProperties().getProperty("com.lordjoe.comet.spectraBin");
        int spectraToHandle = SPECTRA_TO_HANDLE;
        if(binStr != null)
            spectraToHandle = Integer.parseInt(binStr);
        File spectra = new File(spectrumPath);

        File tempdir = new File(spectra.getParentFile(),UUID.randomUUID().toString());

        List<File> files = MzXMLUtilities.splitMzXMLFile(spectra,tempdir,spectraToHandle);

        String tempdirAbsolutePath = tempdir.getAbsolutePath();
        System.out.println("mzXML Split " + tempdirAbsolutePath);
        File[] files1 = tempdir.listFiles();
        for (int i = 0; i < files1.length; i++) {
            File file = files1[i];
            if(!file.exists()) {
                System.out.println(file.getAbsolutePath() + " does not exist!");
            }
            if(!file.canRead()) {
                System.out.println(file.getAbsolutePath() + " cannot be read!");
            }
            System.out.println(file.getAbsolutePath());
        }
        System.out.println("end mzXMLFiles");


        if(!tempdir.exists()) {
            System.out.println(tempdir.getAbsolutePath() + " does not exist!");
        }
        if(!tempdir.canRead()) {
            System.out.println(tempdir.getAbsolutePath() + " cannot be read!");
        }
          JavaRDD<String> spectraData = currentContext.wholeTextFiles(tempdirAbsolutePath).values();

                //    String header = getSpectrumHeader(spectrumPath);
    //    JavaRDD<String> spectraData =  SparkSpectrumUtilities.partitionAsMZXML(spectrumPath,currentContext,spectraToHandle,header).values();

   //     databasePath
   //     List<String> spectra = spectraData.collect();

        JavaRDD<Tuple2<String, String>> scoringPairs = buildScoringPairs(  fastas, spectraData) ;
     //   scoringPairs.persist(StorageLevel.MEMORY_AND_DISK()) ;
    //    List<Tuple2<String, String>> parirs = scoringPairs.collect();

        JavaRDD<String> pepXMLS = scoringPairs.map(new CometScoringFunction());


        pepXMLS.persist(StorageLevel.MEMORY_AND_DISK()) ;
        List<String> scored = fastas.collect();

        handleScores(pepXMLS, scoringApplication);

        System.out.println("Cleaning Up");
         for (File file : files1) {
              file.delete();
        }
        tempdir.delete();

     }

    private static String generateMZXMLIndex(String data) {
         return MZMLIndexGenerator.generateIndex(data); 
    }

    private static String getSpectrumHeader(String spectrumPath) {
        try {
            LineNumberReader rdr = new LineNumberReader((new FileReader(spectrumPath)));
            String line = rdr.readLine();
            StringBuilder sb = new StringBuilder();
            while(line != null)   {
                if(line.contains("<scan"))
                    break;
                sb.append(line) ;
                if(line.contains("</search_summary>"))
                    break;
                sb.append("\n");

                line = rdr.readLine();
            }
            rdr.close();
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }



    public static JavaPairRDD<String,String> getPartitionSpectra( final String pSpectra, XTandemMain application,int spectraToHandle) {
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        String fileHeader = MultiMZXMLScanInputFormat.readMZXMLHeader(new Path(pSpectra));

        // read spectra
        JavaPairRDD<String, String> scans = SparkSpectrumUtilities.partitionAsMZXML(pSpectra,currentContext,  spectraToHandle,fileHeader);
        // next line is for debugging
        // spectraToScore = SparkUtilities.realizeAndReturn(spectraToScore);
        return scans;
    }



    /**
     * extract the header of the file
     * @param exemplar
     * @return
     */
    private static String extractHeader(String exemplar) {
        return exemplar.substring(0,exemplar.indexOf("<spectrum_query "));
    }



    public static JavaRDD<Tuple2<String, String>> buildScoringPairs( JavaRDD<String> fastas,JavaRDD<String> spectraData)
    {
        JavaPairRDD<String, String> cartesian = fastas.cartesian(spectraData);
       return  cartesian.map( IdentityFunction.INSTANCE);

    }



    public static final String FOOTER_XML = " </msms_run_summary>\n" +
            "</msms_pipeline_analysis>\n";

    public static  void handleScores(JavaRDD<String>  pepXMLS, XTandemMain scoringApplication)
    {
        String header = extractHeader(pepXMLS);

        JavaPairRDD<String,String> scores = pepXMLS.flatMapToPair(new TagExtractionFunction("spectrum_query","spectrum"));

        JavaPairRDD<String,SpectrumQueryWithHits> results =  scores.combineByKey(
              new GenerateFirstScore(),
              new AddNewScore(),
              new CombineScoredScans()  );

        CometConsolidator consolidator = new  CometConsolidator(header,FOOTER_XML,scoringApplication);

        
        JavaRDD<String> outtext = results.map(new AbstractLoggingFunction<Tuple2<String, SpectrumQueryWithHits>, String>() {
                        @Override
                        public String doCall(Tuple2<String, SpectrumQueryWithHits> x) throws Exception {
                            SpectrumQueryWithHits outhits =  x._2;
                            return outhits.formatBestHits(5);
                        }
                    }) ;

          consolidator.writeScores(outtext);

                //       writeScores(  header,  results, "combinedresults.pep.xml");
    }

    private static String extractHeader(JavaRDD<String> pepXMLS) {
        // we need to use a few times
        pepXMLS.persist(StorageLevel.MEMORY_AND_DISK());
        String header = null;

        while(header == null)   {
            List<String> strings = pepXMLS.takeSample(true, 2);
            for (String exemplar : strings) {
                if(exemplar != null) {
                    header = extractHeader(exemplar);
                    if(header.contains("<msms_run_summary"))
                        break;
                }
            }
        }

           return header;
    }



    private static class GenerateFirstScore extends AbstractLoggingFunction<String, SpectrumQueryWithHits> {
        @Override
        public SpectrumQueryWithHits doCall(String v1) throws Exception {
            return new SpectrumQueryWithHits(v1);
        }
    }


    public static class AddNewScore extends AbstractLoggingFunction2<SpectrumQueryWithHits, String, SpectrumQueryWithHits> {
         @Override
        public SpectrumQueryWithHits doCall(SpectrumQueryWithHits v1, String addedString) throws Exception {
            SpectrumQueryWithHits v2 = new SpectrumQueryWithHits(addedString);
            return v1.addQuery(v2);
             
        }
    }


    private static class CombineScoredScans extends AbstractLoggingFunction2<SpectrumQueryWithHits, SpectrumQueryWithHits, SpectrumQueryWithHits> {
    

        @Override
        public SpectrumQueryWithHits doCall(SpectrumQueryWithHits v1, SpectrumQueryWithHits v2) throws Exception {
            return v1.addQuery(v2);
        }
    }


    public static JavaRDD<String> readFilesFromDirectory(String directory) {
        JavaPairRDD<String, String> fileAndContents = SparkUtilities.getCurrentContext().wholeTextFiles(directory);
        return  fileAndContents.values();
    }
        /**
         * call with args like or20080320_s_silac-lh_1-1_11short.mzxml in Sample2
         *
         * @param args
         */
    public static void main(String[] args) throws Exception {

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        AccumulatorUtilities.setFunctionsLoggedByDefault(false);
        scoreWithFiles(args);
      }
}
