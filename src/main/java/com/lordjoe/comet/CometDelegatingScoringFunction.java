package com.lordjoe.comet;

import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.utilities.ElapsedTimer;
import com.lordjoe.utilities.FileUtilities;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * com.lordjoe.comet.CometScoringFunction
 * User: Steve
 * Date: 11/11/2018
 */
public class CometDelegatingScoringFunction extends AbstractLoggingFunction<String, String> {

    private static final long MAX_ELAPSED = 90 * 1000; // 1 minutes
    private transient String baseParamters;
    private final String extension;
    private final String databaseBath;

    public CometDelegatingScoringFunction(String extension,String dbPath) {
        this.extension = extension;
        this.databaseBath = dbPath;
    }

    private String getCurrentParameters( ) {
         StringBuilder sb = new StringBuilder();
        String params = getBaseParams();
        String[] split = params.split("\n");
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
             if(s.contains("database_name"))
                 s = "database_name = "  + databaseBath;
             sb.append(s);
             sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public String doCall(String v1) throws Exception {
          String mzXML = v1;

         File xmlFile = createLocalFile(mzXML,extension);
        FileUtilities.writeFile(xmlFile,mzXML);

         String params = getCurrentParameters();
        File paramsFile = createLocalFile(params,"params");
        FileUtilities.writeFile(paramsFile,params);
        String outFilePath = xmlFile.getAbsolutePath().replace("." + extension, ".pep.xml").replace("\\", "/");
        File outFile = new File(outFilePath);
        String outfileName =  outFile.getName();
        outfileName = outfileName.replace(".pep.xml","");
        StringBuilder output = new StringBuilder();
        StringBuilder errors = new StringBuilder();
        String ret = doRunComet(xmlFile, paramsFile, outFile, outfileName, output, errors);
        return ret;

    }

    /**
     * synchronized insures only one instance per JVM
     * @param xmlFile
     * @param paramsFile
     * @param outFile
     * @param outfileName
     * @param output
     * @param errors
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private String  doRunComet(File xmlFile, File paramsFile, File outFile, String outfileName, StringBuilder output, StringBuilder errors) throws IOException, InterruptedException {
        synchronized (CometDelegatingScoringFunction.class) {
            boolean success = CommandLineExecutor.executeCommandLine(output, errors, "comet",
                    "-P" + paramsFile.getName(),
                    xmlFile.getAbsolutePath()
            );
            String ret = null;
            boolean cometSuccess = defineCometSuccess(output.toString(), errors.toString());
            if (success && cometSuccess) {
                ElapsedTimer timer = new ElapsedTimer();
                while (timer.getElapsedMillisec() < MAX_ELAPSED) {
                    if (outFile.exists() && outFile.canRead())
                        break;
                    System.out.println("Waiting for file " + outFile.getAbsolutePath() + " " + timer.getElapsedMillisec() / 60000 + " min");
                    Thread.sleep(60000);
                }
                if (!outFile.exists() || !outFile.canRead()) {
                    success = false;
                    System.err.println("File timeout after  " + timer.getElapsedMillisec() / 60000 + " min");
                }
                if (success) {
                    ret = FileUtilities.readInFile(outFile);
                    if (ret != null)
                        ret = ret.replace(outfileName, "Spectrum");
                    else
                        System.out.println(outFile);
                }
                xmlFile.delete();
                paramsFile.delete();
                outFile.delete();
            } else {
                System.out.println("handle error " + databaseBath +
                        " " + xmlFile.getAbsolutePath() +
                        " " + paramsFile.getAbsolutePath()

                );
                System.out.println("Stop and look");
            }
            return ret;
        }
    }

    private boolean defineCometSuccess(String output, String errors) {
        if(output.contains(" no spectra searched."))
            return false;
        if(errors.contains(" no spectra searched."))
            return false;
        return true;
    }

    private File createLocalFile(String fasta,String extension) {
           String  name = UUID.randomUUID().toString() + "." + extension;
           return new File(name);
    }

    private void buildParamsFile() {
        if (baseParamters == null)
            baseParamters = getBaseParams();

    }

    private static String getBaseParams() {
        String path = SparkFiles.get("comet.base.params");

        path = path.replace("\\", "/");
        String ret = FileUtilities.readInFile(path) ;
        return ret;

    }
}
