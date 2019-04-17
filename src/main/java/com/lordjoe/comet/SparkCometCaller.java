package com.lordjoe.comet;

import com.lordjoe.distributed.MultiFastaInputFormat;
import com.lordjoe.distributed.MultiMGFInputFormat;
import com.lordjoe.distributed.SparkSpectrumUtilities;
import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.hydra.SparkMapReduceScoringHandler;
import com.lordjoe.distributed.input.FastaInputFormat;
import com.lordjoe.distributed.input.MGFInputFormat;
import com.lordjoe.distributed.input.MGFSpecialDelimitedText;
import com.lordjoe.distributed.input.MultiMZXMLScanInputFormat;
import com.lordjoe.distributed.spark.IdentityFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFlatMapFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction2;
import com.lordjoe.distributed.spark.accumulators.AccumulatorUtilities;
import com.lordjoe.utilities.ElapsedTimer;
import com.lordjoe.utilities.FileUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.HttpsFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.systemsbiology.hadoop.HadoopUtilities;
import org.systemsbiology.xtandem.XTandemHadoopUtilities;
import org.systemsbiology.xtandem.XTandemMain;
import scala.Tuple2;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;

/**
 * com.lordjoe.comet.SparkCometCaller
 * uses calls to comet directly
 * Date: 10/7/2014
 */
public class SparkCometCaller implements Serializable {

    public static final int PROTEINS_TO_HANDLE = 2000;
    public static final int SPECTRA_TO_HANDLE = 500;

    public static int numberAnalalyzedSpectra = 0;
    public static int numberAnlalyzedProteins = 0;
    public static int numberSpectraPartitions = 0;
    public static int numberProteinPartitions = 0;
    public static int scoringPartitions = 0;
    public static int proteinsToHandle = 0;
    public static int spectraToHandle = 0;

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
            scoringApplication.setParameter(key.toString(), sparkProperties.getProperty(key.toString()));
        }

        String spectrumPath = scoringApplication.getSpectrumPath();
        System.out.println("Spectrum " + spectrumPath);
        String databaseName = scoringApplication.getDatabaseName();
        System.out.println("Database " + databaseName);


        String addedFiles = scoringApplication.getParameter("com.lordjoe.distributed.files", "");
        if (addedFiles.length() > 0) {
            String[] split = addedFiles.split(";");
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                currentContext.addFile(s);
            }
        }

        int minimumPartitions = 100;
         scoringPartitions = minimumPartitions;
        String partitionBinStr = SparkUtilities.getSparkProperties().getProperty("com.lordjoe.comet.scoring_partitions");
        if (partitionBinStr != null)
            scoringPartitions = Integer.parseInt(partitionBinStr);
        System.out.println("Scoring partitions");

        String propBinStr = SparkUtilities.getSparkProperties().getProperty("com.lordjoe.comet.fastaBin");
        proteinsToHandle = PROTEINS_TO_HANDLE;
        if (propBinStr != null)
            proteinsToHandle = Integer.parseInt(propBinStr);
        String databasePath = databaseName + ".fasta";

        String binStr = SparkUtilities.getSparkProperties().getProperty("com.lordjoe.comet.spectraBin");
        spectraToHandle = SPECTRA_TO_HANDLE;
        if (binStr != null)
            spectraToHandle = Integer.parseInt(binStr);
        System.out.println("SpectraPath " + spectrumPath);

        File fasta = new File(databasePath);
      //  int splitsize = 50 * 1000 * 1000;
        JavaRDD<String> fastas = handleFastaFileNew(databasePath, proteinsToHandle);

      //  long[] countRef = new long[1];
    //    fastas = SparkUtilities.persistAndCount("Fasta Split", fastas, countRef);
     //   int fastaCount = (int) countRef[0];

       // List<String> collect = fastas.collect();

        File spectra = new File(spectrumPath);

        String extension = FileUtilities.getExtension(spectra);


        JavaRDD<String> spectraData;
        if (spectra.getName().toLowerCase().endsWith(".mzxml")) {
            spectraData = handleMzmlFile(spectra, spectraToHandle);
        } else {
            spectraData = handleMgfFile(spectrumPath, spectraToHandle);
        }



   //     List<String> collect = spectraData.collect();

        //    String header = getSpectrumHeader(spectrumPath);
        //    JavaRDD<String> spectraData =  SparkSpectrumUtilities.partitionAsMZXML(spectrumPath,currentContext,spectraToHandle,header).values();

        //     databasePath
        //     List<String> spectra = spectraData.collect();

        JavaRDD<Tuple2<String, String>> scoringPairs = buildScoringPairs(fastas, spectraData);
        //   scoringPairs.persist(StorageLevel.MEMORY_AND_DISK()) ;
        //    List<Tuple2<String, String>> parirs = scoringPairs.collect();

        if (scoringPairs.partitions().size() < minimumPartitions)
            scoringPairs = scoringPairs.repartition(scoringPartitions);

        // https://martin.atlassian.net/wiki/spaces/lestermartin/blog/2016/05/19/67043332/why+spark+s+mapPartitions+transformation+is+faster+than+map+calls+your+function+once+partition+not+once+element
        // trying mapPartition

        //   scoringPairs =   SparkUtilities.persistAndCount("Commet Calls " ,scoringPairs);

        JavaRDD<String> pepXMLS = scoringPairs.map(new CometScoringFunction(extension));
        //JavaRDD<String> pepXMLS = scoringPairs.mapPartitions(new CometScoringFunction());

        String header = null;

/*
          pepXMLS.persist(StorageLevel.MEMORY_AND_DISK());
        //     List<String> scored = fastas.collect();

        // try 1 if that fails try many
        String test = pepXMLS.first();

        if(test != null)
            header = extractHeader(test);
        if(header == null) {
            header = extractHeader(pepXMLS);
        }
*/
        handleScores(pepXMLS, scoringApplication, header);

        System.out.println("Cleaning Up");
        //   for (File file : files1) {
        //       file.delete();
        //    }
        //    tempdir.delete();

    }

    private static JavaRDD<String> handleMgfFile(String path, int spectraToHandle) {

        JavaSparkContext ctx = SparkUtilities.getCurrentContext();
        Configuration conf = new Configuration(ctx.hadoopConfiguration());
        conf.setInt("spectraPerGroup", spectraToHandle);
        conf.set("path", path);
        Class inputFormatClass = MGFInputFormat.class; //MultiMGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;


        JavaRDD<String>  spectraData =  ctx.newAPIHadoopFile(
                path,
                inputFormatClass,
                keyClass,
                valueClass,
                conf

        ).values();
        long[] countRef = new long[1];
        spectraData = SparkUtilities.persistAndCount("Spectra Split", spectraData, countRef);
        numberAnalalyzedSpectra = (int) countRef[0];

        numberSpectraPartitions = 1 + (int) numberAnalalyzedSpectra / (int) spectraToHandle;
        spectraData = spectraData.coalesce(numberSpectraPartitions);

        System.out.println("Spectra Partitions " + numberSpectraPartitions);

        // compine all recordsin partition to one string
        spectraData = spectraData.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                int nrecs = 0;
                StringBuilder sb = new StringBuilder();
                while(stringIterator.hasNext()) {
                    sb.append(stringIterator.next());
                    sb.append("\n");
                    nrecs++;
                }
                List<String> ret = new ArrayList<>();
                ret.add(sb.toString());
                return ret.iterator();
            }
        });
  //      spectraData = SparkUtilities.persistAndCount("Spectra to Process", spectraData, countRef);
        return spectraData;
    }

    private static JavaRDD<String> handleFastaFileNew(String path, int spectraToHandle) {

        JavaSparkContext ctx = SparkUtilities.getCurrentContext();
        Configuration conf = new Configuration(ctx.hadoopConfiguration());
        Class inputFormatClass = FastaInputFormat.class; //MultiMGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;


        JavaRDD<String>  spectraData =  ctx.newAPIHadoopFile(
                path,
                inputFormatClass,
                keyClass,
                valueClass,
                conf

        ).values();
        long[] countRef = new long[1];
        spectraData = SparkUtilities.persistAndCount("Protein Split", spectraData, countRef);
        numberAnlalyzedProteins = (int) countRef[0];

        numberProteinPartitions  = 1 + (int) numberAnlalyzedProteins / (int) spectraToHandle;
        spectraData = spectraData.repartition(numberProteinPartitions);

        System.out.println("Protein Partitions " + numberProteinPartitions);


        // compine all recordsin partition to one string
        spectraData = spectraData.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                int nrecs = 0;
                boolean pass1  = false; //  if true see first split
                StringBuilder sb = new StringBuilder();
                while(stringIterator.hasNext()) {
                    String next = stringIterator.next();
                    String str = appendSpectrum(next);
                    if(pass1) {
                        System.out.println(next);
                        System.out.println(str);
                        pass1 = false;
                    }
                    sb.append(str);
                     nrecs++;
                }
                List<String> ret = new ArrayList<>();
                ret.add(sb.toString());
                return ret.iterator();
            }
        });
        return spectraData;
    }

    public static String appendSpectrum(String raw) {
        StringBuilder sb = new StringBuilder(">");
        while(raw.length() > 80) {
            sb.append(raw.substring(0,80));
            sb.append("\n");
            if(raw.length() > 80)
                raw = raw.substring(80);
            else
                raw = "";
        }
        if(raw.length() > 0) {
            sb.append(raw );
            sb.append("\n");

        }
         return sb.toString();
    }

    private static JavaRDD<String> handleMzmlFile(File spectra, int spectraToHandle) {
        System.out.println("SpectraPath file  " + spectra.getAbsolutePath());
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();

        // split the file if needed
        File tempdir = new File(spectra.getParentFile(), spectra.getName() + "_split");
        if (!tempdir.exists() || (tempdir.isDirectory() && tempdir.listFiles() == null)) {
            System.out.println("Data is being split");
            tempdir.mkdirs();
            List<File> files = MzXMLUtilities.splitMzXMLFile(spectra, tempdir, spectraToHandle);
        } else {
            System.out.println("Data is presplit");
        }
        //      File tempdir = new File(spectra.getParentFile(),UUID.randomUUID().toString());


        String tempdirAbsolutePath = tempdir.getAbsolutePath();
        System.out.println("mzXML Split " + tempdirAbsolutePath);
        File[] files1 = tempdir.listFiles();
        for (int i = 0; i < files1.length; i++) {
            File file = files1[i];
            if (!file.exists()) {
                System.out.println(file.getAbsolutePath() + " does not exist!");
            }
            if (!file.canRead()) {
                System.out.println(file.getAbsolutePath() + " cannot be read!");
            }
            System.out.println(file.getAbsolutePath());
        }
        System.out.println("end mzXMLFiles");


        if (!tempdir.exists()) {
            System.out.println(tempdir.getAbsolutePath() + " does not exist!");
        }
        if (!tempdir.canRead()) {
            System.out.println(tempdir.getAbsolutePath() + " cannot be read!");
        }
        JavaRDD<String> spectraData = currentContext.wholeTextFiles(tempdirAbsolutePath, 100).values();
        return spectraData;
    }

    private static JavaRDD<String> handleFastaFile(File spectra, int splitsize) {
        System.out.println("fasta file  " + spectra.getAbsolutePath());
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();

        // split the file if needed
        File tempdir = new File(spectra.getParentFile(), spectra.getName() + "_split");
        if (!tempdir.exists() || (tempdir.isDirectory() && tempdir.listFiles() == null)) {
            System.out.println("Data is being split");
            long length = spectra.length();
            long numberSplits = length / splitsize;
            FileUtilities.expungeDirectory(tempdir);
            tempdir.mkdirs();
            List<File> files = splitFastaFile(spectra, tempdir, currentContext, splitsize);

        } else {
            System.out.println("Data is presplit");
        }
        //      File tempdir = new File(spectra.getParentFile(),UUID.randomUUID().toString());


        String tempdirAbsolutePath = tempdir.getAbsolutePath();
        System.out.println("fasta Split " + tempdirAbsolutePath);
        File[] files1 = tempdir.listFiles();
        for (int i = 0; i < files1.length; i++) {
            File file = files1[i];
            if (!file.exists()) {
                System.out.println(file.getAbsolutePath() + " does not exist!");
            }
            if (!file.canRead()) {
                System.out.println(file.getAbsolutePath() + " cannot be read!");
            }
            System.out.println(file.getAbsolutePath());
        }
        System.out.println("end fasta");


        if (!tempdir.exists()) {
            System.out.println(tempdir.getAbsolutePath() + " does not exist!");
        }
        if (!tempdir.canRead()) {
            System.out.println(tempdir.getAbsolutePath() + " cannot be read!");
        }
        JavaRDD<String> spectraData = currentContext.wholeTextFiles(tempdirAbsolutePath, 100).values();
        return spectraData;
    }

    public static List<File> splitFastaFile(File original, File tempdir, JavaSparkContext currentContext, int splitsize) {
        JavaPairRDD<String, String> parse = SparkSpectrumUtilities.partitionFastaFile(original.getAbsolutePath(),
                currentContext, splitsize);
        JavaRDD<String> data = parse.values();
        int numberPartitions = Math.max(1, (int) (original.length() / splitsize));
        final File targetDir = tempdir;
        data = data.repartition(numberPartitions);
        JavaRDD<String> ret = data.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                File f = new File(targetDir, UUID.randomUUID().toString() + ".fasta");
                PrintWriter pw = new PrintWriter(new FileWriter(f));
                while (stringIterator.hasNext()) {
                    pw.println(stringIterator.next());
                }
                pw.close();
                List<String> out = new ArrayList<>();
                out.add(f.getAbsolutePath());
                return out.iterator();
            }
        });
        List<String> collect = ret.collect();
        List<File> ret1 = new ArrayList<>();
        for (String s : collect) {
            ret1.add(new File(s));
        }
        return ret1;
    }

    private static String generateMZXMLIndex(String data) {
        return MZMLIndexGenerator.generateIndex(data);
    }

    private static String getSpectrumHeader(String spectrumPath) {
        try {
            LineNumberReader rdr = new LineNumberReader((new FileReader(spectrumPath)));
            String line = rdr.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                if (line.contains("<scan"))
                    break;
                sb.append(line);
                if (line.contains("</search_summary>"))
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


    public static JavaPairRDD<String, String> getPartitionSpectra(final String pSpectra, XTandemMain application, int spectraToHandle) {
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        String fileHeader = MultiMZXMLScanInputFormat.readMZXMLHeader(new Path(pSpectra));

        // read spectra
        JavaPairRDD<String, String> scans = SparkSpectrumUtilities.partitionAsMZXML(pSpectra, currentContext, spectraToHandle, fileHeader);
        // next line is for debugging
        // spectraToScore = SparkUtilities.realizeAndReturn(spectraToScore);
        return scans;
    }


    /**
     * extract the header of the file
     *
     * @param exemplar
     * @return
     */
    private static String extractHeader(String exemplar) {
        return exemplar.substring(0, exemplar.indexOf("<spectrum_query "));
    }


    public static JavaRDD<Tuple2<String, String>> buildScoringPairs(JavaRDD<String> fastas, JavaRDD<String> spectraData) {
        JavaPairRDD<String, String> cartesian = fastas.cartesian(spectraData);
        return cartesian.map(IdentityFunction.INSTANCE);

    }

    public static final String FOOTER_XML = " </msms_run_summary>\n" +
            "</msms_pipeline_analysis>\n";

    public static void handleScores(JavaRDD<String> pepXMLS, XTandemMain scoringApplication, String header) {

        header = STANDARD_HEADER;

        JavaPairRDD<String, String> scores = pepXMLS.flatMapToPair(new TagExtractionFunction("spectrum_query", "spectrum"));

        JavaPairRDD<String, SpectrumQueryWithHits> results = scores.combineByKey(
                new GenerateFirstScore(),
                new AddNewScore(),
                new CombineScoredScans());

        CometConsolidator consolidator = new CometConsolidator(header, FOOTER_XML, scoringApplication);


        JavaRDD<String> outtext = results.map(new AbstractLoggingFunction<Tuple2<String, SpectrumQueryWithHits>, String>() {
            @Override
            public String doCall(Tuple2<String, SpectrumQueryWithHits> x) throws Exception {
                SpectrumQueryWithHits outhits = x._2;
                return outhits.formatBestHits(5);
            }
        });

        showRunStatistics();
          consolidator.writeScores(outtext);

        //       writeScores(  header,  results, "combinedresults.pep.xml");
    }

    private static void showRunStatistics() {
        System.out.println("Protein Batch " + proteinsToHandle);
        System.out.println("Spectra Batch " + spectraToHandle);
        System.out.println("Number Spectra " + numberAnalalyzedSpectra);
        System.out.println("Number Proteins " + numberAnlalyzedProteins);
        System.out.println("Spectra Partitions" + numberSpectraPartitions);
        System.out.println("Proteins Partitions " + numberProteinPartitions);
        System.out.println("Comet calls " + (numberSpectraPartitions * numberProteinPartitions));
        System.out.println("Comparisons " + (numberAnalalyzedSpectra * numberAnlalyzedProteins) / 1000000 + "M");
     }

    private static String extractHeader(JavaRDD<String> pepXMLS) {
        // we need to use a few times
        pepXMLS.persist(StorageLevel.MEMORY_AND_DISK());
        String header = null;

        while (header == null) {
            List<String> strings = pepXMLS.takeSample(true, 2);
            for (String exemplar : strings) {
                if (exemplar != null) {
                    header = extractHeader(exemplar);
                    if (header.contains("<msms_run_summary"))
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
        return fileAndContents.values();
    }

    /**
     * call with args like or20080320_s_silac-lh_1-1_11short.mzxml in Sample2
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        rootLogger = Logger.getLogger("org");
        if (rootLogger != null)
            rootLogger.setLevel(Level.FATAL);
        rootLogger = Logger.getLogger("org.apache.spark.network");
        if (rootLogger != null)
            rootLogger.setLevel(Level.FATAL);
        rootLogger = Logger.getLogger("org.apache.spark.status.AppStatusListerner");
        if (rootLogger != null)
            rootLogger.setLevel(Level.FATAL);
        AccumulatorUtilities.setFunctionsLoggedByDefault(false);

        //runFirefox();

        scoreWithFiles(args);

        System.out.println("Done waiting to be killed");
        // hand so display workd
        //       while(!HadoopUtilities.isWindows()) {
        //           Thread.sleep(1000);
        //      }
    }

    private static void runFirefox() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    String[] commands = {"firefox", "localost:9080"};
                    Process exec = Runtime.getRuntime().exec(commands, null);
                    System.out.println("Firefox " + exec.exitValue());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public static final String STANDARD_HEADER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    " <msms_pipeline_analysis date=\"2018-12-24T10:25:21\" xmlns=\"http://regis-web.systemsbiology.net/pepXML\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://sashimi.sourceforge.net/schema_revision/pepXML/pepXML_v120.xsd\" summary_xml=\"E:\\SparkHydraV2\\comet\\Spectrum.pep.xml\">\n" +
                    " <msms_run_summary base_name=\"E:\\SparkHydraV2\\comet\\Spectrum\" msManufacturer=\"UNKNOWN\" msModel=\"UNKNOWN\" raw_data_type=\"raw\" raw_data=\".mzXML\">\n" +
                    " <sample_enzyme name=\"Trypsin\">\n" +
                    "  <specificity cut=\"KR\" no_cut=\"P\" sense=\"C\"/>\n" +
                    " </sample_enzyme>\n" +
                    " <search_summary base_name=\"E:\\SparkHydraV2\\comet\\Spectrum\" search_engine=\"Comet\" search_engine_version=\"2018.01 rev. 3\" precursor_mass_type=\"monoisotopic\" fragment_mass_type=\"monoisotopic\" search_id=\"1\">\n" +
                    "  <search_database local_path=\"E:\\SparkHydraV2\\comet\\d80885b0-6df5-4c3b-8f26-76442bfe388b.fasta\" type=\"AA\"/>\n" +
                    "  <enzymatic_search_constraint enzyme=\"Trypsin\" max_num_internal_cleavages=\"2\" min_number_termini=\"2\"/>\n" +
                    "  <aminoacid_modification aminoacid=\"M\" massdiff=\"15.994900\" mass=\"147.035385\" variable=\"Y\" symbol=\"*\"/>\n" +
                    "  <aminoacid_modification aminoacid=\"C\" massdiff=\"57.021464\" mass=\"160.030649\" variable=\"N\"/>\n" +
                    "  <parameter name=\"# comet_version \" value=\"2017.01 rev. 1\"/>\n" +
                    "  <parameter name=\"activation_method\" value=\"ALL\"/>\n" +
                    "  <parameter name=\"add_A_alanine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_B_user_amino_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_C_cysteine\" value=\"57.021464\"/>\n" +
                    "  <parameter name=\"add_Cterm_peptide\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Cterm_protein\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_D_aspartic_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_E_glutamic_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_F_phenylalanine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_G_glycine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_H_histidine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_I_isoleucine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_J_user_amino_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_K_lysine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_L_leucine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_M_methionine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_N_asparagine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Nterm_peptide\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Nterm_protein\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_O_ornithine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_P_proline\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Q_glutamine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_R_arginine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_S_serine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_T_threonine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_U_selenocysteine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_V_valine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_W_tryptophan\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_X_user_amino_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Y_tyrosine\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"add_Z_user_amino_acid\" value=\"0.000000\"/>\n" +
                    "  <parameter name=\"allowed_missed_cleavage\" value=\"2\"/>\n" +
                    "  <parameter name=\"clear_mz_range\" value=\"0.000000 0.000000\"/>\n" +
                    "  <parameter name=\"clip_nterm_methionine\" value=\"0\"/>\n" +
                    "  <parameter name=\"database_name\" value=\"E:\\SparkHydraV2\\comet\\d80885b0-6df5-4c3b-8f26-76442bfe388b.fasta\"/>\n" +
                    "  <parameter name=\"decoy_prefix\" value=\"DECOY_\"/>\n" +
                    "  <parameter name=\"decoy_search\" value=\"0\"/>\n" +
                    "  <parameter name=\"digest_mass_range\" value=\"600.000000 5000.000000\"/>\n" +
                    "  <parameter name=\"fragment_bin_offset\" value=\"0.400000\"/>\n" +
                    "  <parameter name=\"fragment_bin_tol\" value=\"1.000500\"/>\n" +
                    "  <parameter name=\"isotope_error\" value=\"0\"/>\n" +
                    "  <parameter name=\"mass_type_fragment\" value=\"1\"/>\n" +
                    "  <parameter name=\"mass_type_parent\" value=\"1\"/>\n" +
                    "  <parameter name=\"max_fragment_charge\" value=\"3\"/>\n" +
                    "  <parameter name=\"max_precursor_charge\" value=\"6\"/>\n" +
                    "  <parameter name=\"max_variable_mods_in_peptide\" value=\"5\"/>\n" +
                    "  <parameter name=\"minimum_intensity\" value=\"0\"/>\n" +
                    "  <parameter name=\"minimum_peaks\" value=\"10\"/>\n" +
                    "  <parameter name=\"ms_level\" value=\"2\"/>\n" +
                    "  <parameter name=\"nucleotide_reading_frame\" value=\"0\"/>\n" +
                    "  <parameter name=\"num_enzyme_termini\" value=\"2\"/>\n" +
                    "  <parameter name=\"num_output_lines\" value=\"5\"/>\n" +
                    "  <parameter name=\"num_results\" value=\"100\"/>\n" +
                    "  <parameter name=\"num_threads\" value=\"1\"/>\n" +
                    "  <parameter name=\"output_pepxmlfile\" value=\"1\"/>\n" +
                    "  <parameter name=\"output_percolatorfile\" value=\"0\"/>\n" +
                    "  <parameter name=\"output_sqtfile\" value=\"0\"/>\n" +
                    "  <parameter name=\"output_sqtstream\" value=\"0\"/>\n" +
                    "  <parameter name=\"output_suffix\" value=\"\"/>\n" +
                    "  <parameter name=\"output_txtfile\" value=\"0\"/>\n" +
                    "  <parameter name=\"override_charge\" value=\"0\"/>\n" +
                    "  <parameter name=\"peff_format\" value=\"0\"/>\n" +
                    "  <parameter name=\"peptide_mass_tolerance\" value=\"3.000000\"/>\n" +
                    "  <parameter name=\"peptide_mass_units\" value=\"0\"/>\n" +
                    "  <parameter name=\"precursor_charge\" value=\"0 0\"/>\n" +
                    "  <parameter name=\"precursor_tolerance_type\" value=\"0\"/>\n" +
                    "  <parameter name=\"print_expect_score\" value=\"1\"/>\n" +
                    "  <parameter name=\"remove_precursor_peak\" value=\"0\"/>\n" +
                    "  <parameter name=\"remove_precursor_tolerance\" value=\"1.500000\"/>\n" +
                    "  <parameter name=\"require_variable_mod\" value=\"0\"/>\n" +
                    "  <parameter name=\"sample_enzyme_number\" value=\"1\"/>\n" +
                    "  <parameter name=\"scan_range\" value=\"0 0\"/>\n" +
                    "  <parameter name=\"search_enzyme_number\" value=\"1\"/>\n" +
                    "  <parameter name=\"show_fragment_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"skip_researching\" value=\"1\"/>\n" +
                    "  <parameter name=\"spectrum_batch_size\" value=\"0\"/>\n" +
                    "  <parameter name=\"theoretical_fragment_ions\" value=\"1\"/>\n" +
                    "  <parameter name=\"use_A_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"use_B_ions\" value=\"1\"/>\n" +
                    "  <parameter name=\"use_C_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"use_NL_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"use_X_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"use_Y_ions\" value=\"1\"/>\n" +
                    "  <parameter name=\"use_Z_ions\" value=\"0\"/>\n" +
                    "  <parameter name=\"variable_mod01\" value=\"15.994900 M 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod02\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod03\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod04\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod05\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod06\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod07\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod08\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    "  <parameter name=\"variable_mod09\" value=\"0.000000 X 0 3 -1 0 0\"/>\n" +
                    " </search_summary>";
}