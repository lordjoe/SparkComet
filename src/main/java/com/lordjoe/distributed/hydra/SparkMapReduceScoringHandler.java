package com.lordjoe.distributed.hydra;

import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.spark.HydraSparkUtilities;
import org.systemsbiology.xtandem.XTandemMain;

import java.io.InputStream;
import java.io.Serializable;

/**
 * com.lordjoe.distributed.hydra.scoring.SparkMapReduceScoringHandler
 * User: Steve
 * Date: 10/7/2014
 */
public class SparkMapReduceScoringHandler implements Serializable {

    private XTandemMain application;


    /**
     * constructor from file
     * @param congiguration
     * @param createDb
     */
    public SparkMapReduceScoringHandler(String congiguration, boolean createDb) {
        this(HydraSparkUtilities.readFrom(congiguration), congiguration,createDb);
    }

    /**
     * constructor suitable for resource read
     * @param is
     * @param congiguration
     * @param createDb
     */
    public SparkMapReduceScoringHandler(InputStream is, String congiguration, boolean createDb) {
        SparkUtilities.setAppName("SparkMapReduceScoringHandler");


        application = new  XTandemMain(is, congiguration);
      }



    public XTandemMain getApplication() {
        return application;
    }



    public static final double MIMIMUM_HYPERSCORE = 50;
    public static final int DEFAULT_MAX_SCORINGS_PER_PARTITION = 20000;


     static int[] hashes = new int[100];
    static int numberProcessed;


}
