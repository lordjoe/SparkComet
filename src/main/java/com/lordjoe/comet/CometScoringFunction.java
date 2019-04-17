package com.lordjoe.comet;

import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.utilities.ElapsedTimer;
import com.lordjoe.utilities.FileUtilities;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

import java.io.File;
import java.util.UUID;

/**
 * com.lordjoe.comet.CometScoringFunction
 * User: Steve
 * Date: 11/11/2018
 */
public class CometScoringFunction extends AbstractLoggingFunction<Tuple2<String, String>, String> {

    private static final long MAX_ELAPSED = 90 * 1000; // 1 minutes
    private transient String baseParamters;
    private final String extension;

    public CometScoringFunction(String extension) {
        this.extension = extension;
    }

    private String getCurrentParameters(String fastaFile) {
         StringBuilder sb = new StringBuilder();
        String params = getBaseParams();
        String[] split = params.split("\n");
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
             if(s.contains("database_name"))
                 s = "database_name = "  + fastaFile;
             sb.append(s);
             sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public String doCall(Tuple2<String, String> v1) throws Exception {
        String fasta = v1._1;
        String mzXML = v1._2;

        System.out.println( "Fasta size " + fasta.length() +" mzXML " +  mzXML.length() );
        File fastaFile = createLocalFile(fasta,extension);
        FileUtilities.writeFile(fastaFile,fasta);
        File xmlFile = createLocalFile(mzXML,extension);
        FileUtilities.writeFile(xmlFile,mzXML);

        String absolutePath = fastaFile.getAbsolutePath();
        String params = getCurrentParameters(absolutePath);
        File paramsFile = createLocalFile(params,"params");
        FileUtilities.writeFile(paramsFile,params);
        String outFilePath = xmlFile.getAbsolutePath().replace("." + extension, ".pep.xml").replace("\\", "/");
        File outFile = new File(outFilePath);
        String outfileName =  outFile.getName();
        outfileName = outfileName.replace(".pep.xml","");
        StringBuilder output = new StringBuilder();
        StringBuilder errors = new StringBuilder();
        boolean success = CommandLineExecutor.executeCommandLine(output,errors,"comet",
                "-P" + paramsFile.getName(),
                xmlFile.getAbsolutePath()
        );
        String ret = null;
        boolean cometSuccess = defineCometSuccess(output.toString(),errors.toString());
        if(success && cometSuccess) {
            ElapsedTimer timer = new ElapsedTimer();
            while (timer.getElapsedMillisec() < MAX_ELAPSED) {
                if (outFile.exists() && outFile.canRead())
                    break;
                System.out.println("Waiting for file " + outFile.getAbsolutePath() + " " + timer.getElapsedMillisec() / 60000  +" min");
                  Thread.sleep(60000);
            }
            if (!outFile.exists() || !outFile.canRead()) {
                success = false;
                System.err.println("File timeout after  " + timer.getElapsedMillisec() / 60000 +" min");
            }
            if (success) {
                ret = FileUtilities.readInFile(outFile);
                if (ret != null)
                    ret = ret.replace(outfileName, "Spectrum");
                else
                    System.out.println(outFile);
            }
            fastaFile.delete();
            xmlFile.delete();
            paramsFile.delete();
             outFile.delete();
        }
        else {
            System.out.println("handle error " + fastaFile.getAbsolutePath() +
                    " " +  xmlFile.getAbsolutePath() +
                    " " +  paramsFile.getAbsolutePath()

            );
            System.out.println("Stop and look");
        }
        return ret;

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
