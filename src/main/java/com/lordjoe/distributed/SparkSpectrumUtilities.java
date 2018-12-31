package com.lordjoe.distributed;

import com.lordjoe.distributed.input.FastaInputFormat;
import com.lordjoe.distributed.input.MultiMZXMLScanInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.Nonnull;

/**
 * com.lordjoe.distributed.spectrum.SparkSpectrumUtilities
 * User: Steve
 * Date: 9/24/2014
 */
public class SparkSpectrumUtilities {

    public static final int MAX_MZXML_TAG_LENGTH = 500 * 1000;

    private static boolean gMSLevel1Dropped = true;

    public static boolean isMSLevel1Dropped() {
        return gMSLevel1Dropped;
    }

    public static void setMSLevel1Dropped(final boolean pMSLevel1Dropped) {
        gMSLevel1Dropped = pMSLevel1Dropped;
    }

    /**
     * parse a Faste File returning the comment > line as the key
     * and the rest as the value
     *
     * @param path
     * @param ctx
     * @return
     */
    @Nonnull
    public static JavaPairRDD<String, String> parseFastaFile(@Nonnull String path, @Nonnull JavaSparkContext ctx) {
        Class inputFormatClass = FastaInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        return ctx.newAPIHadoopFile(
                path,
                inputFormatClass,
                keyClass,
                valueClass,
                ctx.hadoopConfiguration()
        );

    }

    public static JavaPairRDD<String,String> partitionFastaFile(@Nonnull final String path, JavaSparkContext ctx, int proteinsPerGroup) {
        Configuration conf = new  Configuration(ctx.hadoopConfiguration());
        conf.setInt("proteinsPerGroup",proteinsPerGroup);
        conf.set("path",path);
        Class inputFormatClass = MultiFastaInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        return ctx.newAPIHadoopFile(
                path,
                inputFormatClass,
                keyClass,
                valueClass,
                conf

                );

    }


    @Nonnull
    public static JavaPairRDD<String, String> partitionAsMZXML(@Nonnull final String path, @Nonnull final JavaSparkContext ctx,
                                                               int spectraToHandle,
                                                               String fileHeader  ) {
        Configuration conf = new  Configuration(SparkUtilities.getHadoopConfiguration());
        conf.setInt("spectra",spectraToHandle);
        conf.set("fileHeader",fileHeader);
        Class inputFormatClass = MultiMZXMLScanInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<String, String> parsed = ctx.newAPIHadoopFile(
                path,
                inputFormatClass,
                keyClass,
                valueClass,
                conf
        );
        return parsed;
    }


//    @Nonnull
//     public static JavaPairRDD<String, IMeasuredSpectrum> parseAsMZXMLOLD_CODE(@Nonnull final String path, @Nonnull final JavaSparkContext ctx,XTandemMain application) {
//         Class inputFormatClass = MZXMLInputFormat.class;
//         Class keyClass = String.class;
//         Class valueClass = String.class;
//
//         JavaPairRDD<String, String> spectraAsStrings = ctx.newAPIHadoopFile(
//                 path,
//                 inputFormatClass,
//                 keyClass,
//                 valueClass,
//                 SparkUtilities.getHadoopConfiguration()
//         );
//
//         long[] countRef = new long[1];
//         if (SparkCometScanScorer.isDebuggingCountMade())
//             spectraAsStrings = SparkUtilities.persistAndCountPair("Raw spectra", spectraAsStrings, countRef);
//         // debug code
//         //spectraAsStrings = SparkUtilities.realizeAndReturn(spectraAsStrings);
//
//         if (isMSLevel1Dropped()) {
//             // filter out MS Level 1 spectra
//             spectraAsStrings = spectraAsStrings.filter(new Function<Tuple2<String, String>, Boolean>() {
//                                                            public Boolean call(Tuple2<String, String> s) {
//                                                                String s1 = s._2();
//                                                                if (s1.contains("msLevel=\"2\""))
//                                                                    return true;
//                                                                return false;
//                                                            }
//                                                        }
//             );
//             if (SparkCometScanScorer.isDebuggingCountMade())
//                 spectraAsStrings = SparkUtilities.persistAndCountPair("Filtered spectra", spectraAsStrings, countRef);
//         }
//
//         // debug code
//         //spectraAsStrings = SparkUtilities.realizeAndReturn(spectraAsStrings);
//
//         // parse scan tags as  IMeasuredSpectrum key is id
//         JavaPairRDD<String, IMeasuredSpectrum> parsed = spectraAsStrings.mapToPair(new MapSpectraStringToRawScan());
//         // TODO normalize
//
//         // kill duplicates todo why are there duplicated
//         parsed = SparkUtilities.chooseOneValue(parsed);
//
//         return parsed;
//     }


//    @Nonnull
//    public static JavaPairRDD<String, IMeasuredSpectrum> parseAsOldMGF(@Nonnull final String path, @Nonnull final JavaSparkContext ctx) {
//        Class inputFormatClass = MGFOldInputFormat.class;
//        Class keyClass = String.class;
//        Class valueClass = String.class;
//
//        JavaPairRDD<String, String> spectraAsStrings = ctx.hadoopFile(
//                path,
//                inputFormatClass,
//                keyClass,
//                valueClass
//        );
//        JavaPairRDD<String, IMeasuredSpectrum> spectra = spectraAsStrings.mapToPair(new MGFStringTupleToSpectrumTuple());
//        return spectra;
//    }

}
