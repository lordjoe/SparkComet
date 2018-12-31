package com.lordjoe.comet;

import com.lordjoe.utilities.FileUtilities;
import org.apache.hadoop.fs.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * com.lordjoe.comet.CometRunner
 * User: Steve
 * Date: 11/10/2018
 */
public class CometRunner implements Serializable {

    private static boolean runComet(File spectrum, File fasta, File out, int index) throws IOException {

        File params = buildParams(fasta, index);
        boolean success = CommandLineExecutor.executeCommandLine("comet",
                "-P" + params.getName(),
                spectrum.getAbsolutePath()
        );
        if (success) {
            moveoutFile(spectrum, out, index);
        }
        return success;
    }

    private static void moveoutFile(File spectrum, File out, int index) {

        File src = new File(spectrum.getAbsolutePath().replace(".mzXML", ".pep.xml"));
        File target = new File(out, "out" + index + ".pep.xml");
        boolean success = src.renameTo(target);
        if (!success)
            FileUtilities.copyFile(src,target) ;

    }
    


    private static File buildParams(File fasta, int index) {
        String s = FileUtilities.readInFile("comet.base.params");
        s.replace("XXXSET_FAST_NAME_HEREXXX", fasta.getAbsolutePath());
        File ret = new File("Comet" + index + ".params");
        FileUtilities.writeFile(ret, s);
        return ret;
    }


    public static void main(String[] args) throws Exception {
        File fastaFiles = new File(args[0]);
        File mzXMLFiles = new File(args[1]);
        File outFiles = new File(args[2]);
        outFiles.mkdirs();
        File[] fastas = fastaFiles.listFiles();
        File[] spectra = mzXMLFiles.listFiles();
        int index = 0;
        if (fastas != null && spectra != null) {
            for (int i = 0; i < spectra.length; i++) {
                File spectrum = spectra[i];
                for (int j = 0; j < fastas.length; j++) {
                    File fasta = fastas[j];
                    if (!runComet(spectrum, fasta, outFiles, index++)) {
                        System.out.println("Failure");
                        break;
                    }

                }

            }
        }
    }
}
