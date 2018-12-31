package org.systemsbiology.xtandem;

import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.spark.HydraSparkUtilities;
import org.systemsbiology.hadoop.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

;

/**
 * org.systemsbiology.xtandem.XTandemMain
 * User: steven
 * Date: Jan 5, 2011
 * Singleton representing a JXTandem job -
 * This has the program main
 */
public class XTandemMain extends AbstractParameterHolder implements IParameterHolder {
     public static final String HARDCODED_MODIFICATIONS_PROPERTY = "org.systemsbiology.xtandem.HardCodeModifications";
    public static final String NUMBER_REMEMBERED_MATCHES = "org.systemsbiology.numberRememberedMatches";

    private static boolean gShowParameters = true;

    public static boolean isShowParameters() {
        return gShowParameters;
    }

    public static void setShowParameters(final boolean pGShowParameters) {
        gShowParameters = pGShowParameters;
    }

    private static final List<IStreamOpener> gPreLoadOpeners =
            new ArrayList<IStreamOpener>();

    public static void addPreLoadOpener(IStreamOpener opener) {
        gPreLoadOpeners.add(opener);
    }

    public static IStreamOpener[] getPreloadOpeners() {
        return gPreLoadOpeners.toArray(new IStreamOpener[gPreLoadOpeners.size()]);
    }

    public static final int MAX_SCANS = Integer.MAX_VALUE;

    public static int getMaxScans() {
        return MAX_SCANS;
    }


    private static String gRequiredPathPrefix;

    public static String getRequiredPathPrefix() {
        return gRequiredPathPrefix;
    }

    public static void setRequiredPathPrefix(final String pRequiredPathPrefix) {
        gRequiredPathPrefix = pRequiredPathPrefix;
    }


     private boolean m_SemiTryptic;
    private String m_DefaultParameters;
    private String m_TaxonomyInfo;
    private String m_SpectrumPath;
    private String m_OutputPath;
    private String m_OutputResults;
    private String m_TaxonomyName;
    private final StringBuffer m_Log = new StringBuffer();

       private final Map<String, String> m_PerformanceParameters = new HashMap<String, String>();
    private final DelegatingFileStreamOpener m_Openers = new DelegatingFileStreamOpener();

    // used by Map Reduce

    protected XTandemMain() {

    }


    public XTandemMain(final File pTaskFile) {
        String m_TaskFile = pTaskFile.getAbsolutePath();
        //      Protein.resetNextId();

        Properties predefined = XTandemHadoopUtilities.getHadoopProperties();
        for (String key : predefined.stringPropertyNames()) {
            setPredefinedParameter(key, predefined.getProperty(key));
        }
        try {
            InputStream is = new FileInputStream(m_TaskFile);
            handleInputs(is, pTaskFile.getName());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);

        }
        //       if (gInstance != null)
        //           throw new IllegalStateException("Only one XTandemMain allowed");
    }


    public static final String DO_DEBUGGING_KEY = "org.systemsbiology.xtandem.XTandemMain.DO_DEBUGGING"; // fix this

    public XTandemMain(final InputStream is, String url) {
        //     Protein.resetNextId();
          Properties predefined = XTandemHadoopUtilities.getHadoopProperties();
        for (String key : predefined.stringPropertyNames()) {
            setPredefinedParameter(key, predefined.getProperty(key));
        }


        handleInputs(is, url);


        //     if (gInstance != null)
        //        throw new IllegalStateException("Only one XTandemMain allowed");
    }


    private void setPredefinedParameter(String key, String value) {
        setParameter(key, value);
      }

    public void appendLog(String added) {
        m_Log.append(added);
    }

    public void clearLog() {
        m_Log.setLength(0);
    }

    public String getLog() {
        return m_Log.toString();
    }


    public void setPerformanceParameter(String key, String value) {
        m_PerformanceParameters.put(key, value);
    }

    /**
     * return a parameter configured in  default parameters
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public String getPerformanceParameter(String key) {
        return m_PerformanceParameters.get(key);
    }

    @SuppressWarnings("UnusedDeclaration")
    public String[] getPerformanceKeys() {
        String[] ret = m_PerformanceParameters.keySet().toArray(new String[0]);
        Arrays.sort(ret);
        return ret;
    }


    /**
     * open a file from a string
     *
     * @param fileName  string representing the file
     * @param otherData any other required data
     * @return possibly null stream
     */
    @Override
    public InputStream open(String fileName, Object... otherData) {
        return m_Openers.open(fileName, otherData);
    }

    public void addOpener(IStreamOpener opener) {
        m_Openers.addOpener(opener);
    }



    public boolean isSemiTryptic() {
        return m_SemiTryptic;
    }

    public void setSemiTryptic(final boolean pSemiTryptic) {
        m_SemiTryptic = pSemiTryptic;
    }



       /**
     * what do we call the database or output directory
     *
     * @return !null name
     */
     public String getDatabaseName() {
        String ret = m_TaxonomyName; //getParameter("protein, taxon");
        //  System.err.println("database name = " + m_TaxonomyName);
        return conditionDatabaseName(ret);
    }

    protected String conditionDatabaseName(String s) {
        if (s == null)
            return "database";
        s = s.replace(".fasta", "");
        s = s.replace(":", "");
        s = s.replace(".;", "");
        return s;
    }


    /**
     * parse the initial file and get run parameters
     *
     * @param is
     */
    public void handleInputs(final InputStream is, String url) {
        Map<String, String> notes = XTandemUtilities.readNotes(is, url);

        for (String key : notes.keySet()) {
             setParameter(key, notes.get(key));
          }

        if (isShowParameters()) {
            for (String key : notes.keySet()) {
                System.err.println(key + " = " + notes.get(key));
            }
        }
        m_DefaultParameters = notes.get(
                "list path, default parameters"); //, "default_input.xml");
        m_TaxonomyInfo = notes.get(
                "list path, taxonomy information"); //, "taxonomy.xml");
        m_TaxonomyName = notes.get("protein, taxon");
        m_SpectrumPath = notes.get("spectrum, path"); //, "test_spectra.mgf");
        m_OutputPath = notes.get("output, path"); //, "output.xml");
        // little hack to separate real tandem and hydra results
        if (m_OutputPath != null)
            m_OutputPath = m_OutputPath.replace(".tandem.xml", ".hydra.xml");

        m_OutputResults = notes.get("output, results");

        String requiredPrefix = getRequiredPathPrefix();
        //System.err.println("requiredPrefix " + requiredPrefix);

        if (requiredPrefix != null) {
            if (m_DefaultParameters != null && !m_DefaultParameters.startsWith(requiredPrefix))
                m_DefaultParameters = requiredPrefix + m_DefaultParameters;
            if (m_TaxonomyInfo != null && !m_TaxonomyInfo.startsWith(requiredPrefix))
                m_TaxonomyInfo = requiredPrefix + m_TaxonomyInfo;
            if (m_OutputPath != null && !m_OutputPath.startsWith(requiredPrefix))
                m_OutputPath = requiredPrefix + m_OutputPath;
            if (m_SpectrumPath != null && !m_SpectrumPath.startsWith(requiredPrefix))
                m_SpectrumPath = requiredPrefix + m_SpectrumPath;
        }

        try {
            readDefaultParameters(notes);
        } catch (Exception e) {
           // e.printStackTrace();
            // forgive
            System.err.println("Cannot find file " + m_DefaultParameters);
        }

        XTandemUtilities.validateParameters(this);



        String digesterSpec = getParameter("protein, cleavage site", "trypsin");
        int missedCleavages = getIntParameter("scoring, maximum missed cleavage sites", 0);


            boolean bval = getBooleanParameter("protein, cleavage semi", false);
        setSemiTryptic(bval);

//        String parameter = getParameter(JXTandemLauncher.ALGORITHMS_PROPERTY);
//        if (parameter != null)
//            addAlternateParameters(parameter);



    }



    protected static File getInputFile(Map<String, String> notes, String key) {
        File ret = new File(notes.get(key));
        if (!ret.exists() || !ret.canRead())
            throw new IllegalArgumentException("cannot access file " + ret.getName());
        return ret;
    }

    protected static File getOutputFile(Map<String, String> notes, String key) {
        String athname = notes.get(key);
        File ret = new File(athname);
        File parentFile = ret.getParentFile();
        if ((parentFile != null && (!parentFile.exists() || parentFile.canWrite())))
            throw new IllegalArgumentException("cannot access file " + ret.getName());
        if (ret.exists() && !ret.canWrite())
            throw new IllegalArgumentException("cannot rewrite file file " + ret.getName());

        return ret;
    }



    public String getDefaultParameters() {
        return m_DefaultParameters;
    }

    public String getTaxonomyInfo() {
        return m_TaxonomyInfo;
    }

    public String getSpectrumPath() {
        return m_SpectrumPath;
    }

    public String getOutputPath() {
        return m_OutputPath;
    }

    public String getOutputResults() {
        return m_OutputResults;
    }

    public String getTaxonomyName() {
        return m_TaxonomyName;
    }

         /*
 * modify checks the input parameters for known parameters that are use to modify
 * a protein sequence. these parameters are stored in the m_pScore member object's
 * msequenceutilities member object
 */

    protected String[] readModifications() {
        List<String> holder = new ArrayList<String>();
        String value;

        String strKey = "residue, modification mass";

        value = getParameter(strKey);
        if (value != null)
            holder.add(value);

        String strKeyBase = "residue, modification mass ";
        int a = 1;
        value = getParameter(strKeyBase + (a++));
        while (value != null) {
            holder.add(value);
            value = getParameter(strKeyBase + (a++));
        }

        strKeyBase = "residue, potential modification mass";
        value = getParameter(strKeyBase + (a++));
        value = getParameter(strKey);
        if (true)
            throw new UnsupportedOperationException("Fix This"); // ToDo
//        if (m_xmlValues.get(strKey, strValue)) {
//           m_pScore - > m_seqUtil.modify_maybe(strValue);
//           m_pScore - > m_seqUtilAvg.modify_maybe(strValue);
//       }
//        strKey = "residue, potential modification motif";
//        if (m_xmlValues.get(strKey, strValue)) {
//            m_pScore - > m_seqUtil.modify_motif(strValue);
//            m_pScore - > m_seqUtilAvg.modify_motif(strValue);
//        }
//        strKey = "protein, N-terminal residue modification mass";
//        if (m_xmlValues.get(strKey, strValue)) {
//            m_pScore - > m_seqUtil.modify_n((float) atof(strValue.c_str()));
//            m_pScore - > m_seqUtilAvg.modify_n((float) atof(strValue.c_str()));
//        }
//        strKey = "protein, C-terminal residue modification mass";
//        if (m_xmlValues.get(strKey, strValue)) {
//            m_pScore - > m_seqUtil.modify_c((float) atof(strValue.c_str()));
//            m_pScore - > m_seqUtilAvg.modify_c((float) atof(strValue.c_str()));
//        }
//        strKey = "protein, cleavage N-terminal mass change";
//        if (m_xmlValues.get(strKey, strValue)) {
//            m_pScore - > m_seqUtil.m_dCleaveN = atof(strValue.c_str());
//            m_pScore - > m_seqUtilAvg.m_dCleaveN = atof(strValue.c_str());
//        }
//        strKey = "protein, cleavage C-terminal mass change";
//        if (m_xmlValues.get(strKey, strValue)) {
//            m_pScore - > m_seqUtil.m_dCleaveC = atof(strValue.c_str());
//            m_pScore - > m_seqUtilAvg.m_dCleaveC = atof(strValue.c_str());
//        }
        String[] ret = new String[holder.size()];
        holder.toArray(ret);
        return ret;
    }


    public final String DEFAULT_SCORING_CLASS = "org.systemsbiology.xtandem.TandemKScoringAlgorithm";



    /**
     * read the parameters dscribed in the bioml file
     * listed in "list path, default parameters"
     * These look like
     * <note>spectrum parameters</note>
     * <note type="input" label="spectrum, fragment monoisotopic mass error">0.4</note>
     * <note type="input" label="spectrum, parent monoisotopic mass error plus">100</note>
     * <note type="input" label="spectrum, parent monoisotopic mass error minus">100</note>
     * <note type="input" label="spectrum, parent monoisotopic mass isotope error">yes</note>
     */
    protected void readDefaultParameters(Map<String, String> inputParameters) {
        Map<String, String> parametersMap = getParametersMap();
        if (m_DefaultParameters != null) {
            String paramName = m_DefaultParameters;
            InputStream is;
            final String path = SparkUtilities.buildPath(m_DefaultParameters);
            if (m_DefaultParameters.startsWith("res://")) {
                is = XTandemUtilities.getDescribedStream(m_DefaultParameters);
                paramName = m_DefaultParameters;
            } else {
                is = HydraSparkUtilities.readFrom(path);
//                        File f = new File(defaults);
//                if (f.exists() && f.isFile() && f.canRead()) {
//                    try {
//                        is = new FileInputStream(f);
//                    } catch (FileNotFoundException e) {
//                        throw new RuntimeException(e);
//
//                    }
//                    paramName = f.getName();
//                } else {
//                    paramName = XMLUtilities.asLocalFile(m_DefaultParameters);
//                    is = open(paramName);
//                }
            }
            if (is == null) {
                // maybe this is a resource
                is = XTandemMain.class.getResourceAsStream(m_DefaultParameters);
                if (is == null) {
                   }
            }

            Map<String, String> map = XTandemUtilities.readNotes(is, paramName);
            for (String key : map.keySet()) {
                if (!parametersMap.containsKey(key)) {
                    String value = map.get(key);
                    parametersMap.put(key, value);
                }
            }
        }
        // parameters in the input file override parameters in the default file
        parametersMap.putAll(inputParameters);
        inputParameters.putAll(parametersMap);
    }



}
