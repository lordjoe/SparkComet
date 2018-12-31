package org.systemsbiology.xtandem;


//import com.lordjoe.utilities.*;

import com.lordjoe.utilities.Base64Float;
import com.lordjoe.utilities.FileUtilities;
import org.systemsbiology.boiml.sax.AbstractXTandemElementSaxHandler;
import org.systemsbiology.hadoop.IParameterHolder;
import org.systemsbiology.hadoop.IStreamOpener;
import org.systemsbiology.sax.DelegatingSaxHandler;
import org.systemsbiology.sax.ITopLevelSaxHandler;
import org.systemsbiology.xtandem.sax.BiomlHandler;

import java.io.*;
import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;

//import org.systemsbiology.xtandem.taxonomy.*;
//import org.springframework.jdbc.core.simple.*;


/**
 * org.systemsbiology.xtandem.XTandemUtilities
 *
 * @author Steve Lewis
 * @date Dec 29, 2010
 */
public class XTandemUtilities {
    public static XTandemUtilities[] EMPTY_ARRAY = {};
    public static Class THIS_CLASS = XTandemUtilities.class;



    public static final String WRITING_PEPXML_PROPERTY = "org.systemsbiology.xtandem.hadoop.WritePepXML";
    public static final String WRITING_MGF_PROPERTY = "org.systemsbiology.xtandem.hadoop.WriteMGFSpectraWithHyperscoreGreaterThan";
    public static final String WRITING_MGF_PROPERTY_2 = "org.systemsbiology.xtandem.hadoop.WriteMGFSpectraWithExpectValueLowerThan";
    public static final String CREATE_DECOY_PEPTIDES_PROPERTY = "org.systemsbiology.xtandem.CreateDecoyPeptides";
    public static final String CREATE_DECOY_FOR_MODIFIED_PEPTIDES_PROPERTY = "org.systemsbiology.xtandem.CreateDecoyPeptidesForModifiedPeptides";


    public static final String EMAIL_ADDRESS_PROPERTY = "org.systemsbiology.xtandem.sender";
    //# password to send emmail
    public static final String EMAIL_PASSWORD_PROPERTY = "org.systemsbiology.xtandem.encryptedEmailPassword";
    //# email recipient
    public static final String EMAIL_RECIPIENT_PROPERTY = "org.systemsbiology.xtandem.emailrecipient";


    // do not plan to deal with larger charges
    public static final int MAX_CHARGE = 4;

    public static final int INTEGER_SIZE = 4; // bytes per integer
    public static final int FLOAT_SIZE = 4; // bytes per float
    public static final int FLOAT64_SIZE = 8; // bytes per float 64
    public static final int MINIMUM_SEQUENCE_PEPTIDES = 4; // ignore peptides smaller than this
    public static final int MAXIMUM_SEQUENCE_PEPTIDES = 40; // ignore peptides larger than this
    public static final int MAX_SCORED_MASS = 5000;   // maximum MZ in daltons - XTandem uses 5000



    public static final Comparator OBJECT_STRING_COMPARATOR = new ObjectStringComparator();


    public static String findPeptide(File dbDirectory, String peptide) {
        if (!dbDirectory.exists() || !dbDirectory.isDirectory())
            throw new IllegalStateException("bad directory");
        File[] files = dbDirectory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.getName().endsWith(".peptide")) {
                String ret = findPeptideInFile(file, peptide);
                if (ret != null)
                    return ret;
            }
        }
        return null;
    }

    public static String findPeptideInFile(final File pFile, final String peptide) {
        String[] itens = FileUtilities.readInLines(pFile);
        for (int i = 0; i < itens.length; i++) {
            String iten = itens[i];
            if (iten.startsWith(peptide))
                return pFile.getName();
        }
        return null; // not found
    }


    private static class ObjectStringComparator implements Comparator {
        private ObjectStringComparator() {
        }

        @Override
        public int compare(final Object o1, final Object o2) {
            if (o1 == o2)
                return 0;
            String s1 = o1.toString();
            String s2 = o2.toString();
            int value = s1.compareTo(s2);
            if (value != 0)
                return value;
            return System.identityHashCode(o1) > System.identityHashCode(o2) ? 1 : -1;
        }
    }


    private static double gKScoreBinningFactor = 0.05;

    public static double getKScoreBinningFactor() {
        return gKScoreBinningFactor;
    }

    public static void setKScoreBinningFactor(final double pKScoreBinningFactor) {
        gKScoreBinningFactor = pKScoreBinningFactor;
    }

    /**
     * make protein labels a little friendlier to store and parse , or ; delimited
     *
     * @param s
     * @return
     */
    public static String conditionProteinLabel(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 32 || c >= 127)
                continue; // drop non-printing and unicode
            if (Character.isWhitespace(c)) {
                sb.append(" ");
                continue;
            }
            switch (c) {
                case '|':
                    sb.append("_");
                    break;
                // so it can be parsed
                case '\"':
                case '\'':
                case '!':
                case ';':
                case ':':
                case ',':
                    sb.append(" ");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    public static String xTandemNow() {
        return xTandemDate(new Date());
    }

    public static String xTandemDate(Date time) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
        return df.format(time);
    }

      /**
     * true if the sequence has no ambiguous amino acids
     *
     * @param sequence !null sequence
     * @return
     */
    public static boolean isSequenceAmbiguous(String sequence) {
        for (int i = 0; i < sequence.length(); i++) {
            char c = sequence.charAt(i);
            if (isCharacterAmbiguousAminoAcid(c)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCharacterAmbiguousAminoAcid(char c) {
        switch (Character.toUpperCase(c)) {
            case 'A': //   ("alanine"),  1
            case 'C': //   ("cystine"),  2
            case 'D': //   ("aspartate"),  3
            case 'E': //   ("glutamate"),  4
            case 'F': //   ("phenylalanine"),5
            case 'G': //   ("glycine"),      6
            case 'H': //   ("histidine"),    7
            case 'I': //   ("isoleucine"),     8
            case 'K': //   ("lysine"),       9
            case 'L': //   ("leucine"),      10
            case 'M': //   ("methionine"),   11
            case 'N': //   ("asparagine"),    12
            case 'P': //   ("proline"),      13
            case 'Q': //   ("glutamine"),    14
            case 'R': //   ("arginine"),     15
            case 'S': //   ("serine"),       16
            case 'T': //   ("threonine"),    17
            case 'V': //   ("valine"),       18
            case 'W': //   ("tryptophan"),   19
            case 'Y': //   ("tyrosine"),     20
                return false;
            default:
                return true;
        }
    }


    public static int compareTo(int i1, int i2) {
        if (i1 == i2)
            return 0;
        return i1 < i2 ? -1 : 1;
    }


    public static final double DEFAULT_TOLERANCE = 0.0001;

    /**
     * useful for equivalence
     *
     * @param d1
     * @param d2
     * @return
     */
    public static boolean equivalentDouble(double d1, double d2) {
        return Math.abs(d1 - d2) < DEFAULT_TOLERANCE;
    }

    /**
     * if two strings can be numbers than compare as numbers
     *
     * @param s1 !null string
     * @param s2 !null string
     * @return as above
     */
    public static int compareAsNumbers(String s1, String s2) {
        if (s1.equals(s2))
            return 0;
        Integer I1 = asInteger(s1);
        if (I1 == null)
            return s1.compareTo(s2);
        Integer I2 = asInteger(s2);
        if (I2 == null)
            return s1.compareTo(s2);
        return I1.compareTo(I2);
    }

    /**
     * useful for equivalence
     *
     * @param d1
     * @param d2
     * @return
     */
    public static Integer asInteger(String s) {
        int i1 = s.length();
        if (i1 == 0)
            return null;
        for (int i = 0; i < i1; i++) {
            if (Character.isDigit(s.charAt(i)))
                return null;
        }
        return new Integer(s);
    }

    /**
     * useful for equivalence
     *
     * @param d1
     * @param d2
     * @return
     */
    public static boolean equivalentDouble(double d1, double d2, double allowedDifferense) {
        return Math.abs(d1 - d2) < allowedDifferense;
    }


    private static boolean gDataCachedForTesting;

    /**
     * if true some algorithms will save data to allow tests of functionality
     *
     * @return as above
     */
    public static boolean isDataCachedForTesting() {
        return gDataCachedForTesting;
    }

    /**
     * usually this is set true during some unit tests
     *
     * @param pDataCachedForTesting
     */
    public static void setDataCachedForTesting(boolean pDataCachedForTesting) {
        gDataCachedForTesting = pDataCachedForTesting;
    }

    private static double gProtonMass = 1.00727646688; // 1.007276;
    private static double gCleaveNMass = gProtonMass;
    private static double gCleaveCMass = 19.0178; //17.002735;

    public static final boolean SHOW_MASS_CALCULATION = false;

    /**
     * debug only do not normally call
     *
     * @param mass
     * @param added
     * @param msg
     */
    public static void mayBeShowAddedMassX(double mass, double added, String msg) {
//         if(true)
//             throw new UnsupportedOperationException("Fix This"); // ToDo
        if (SHOW_MASS_CALCULATION)
            System.out.println(String.format("%10.2f", mass) + " " + String.format("%10.2f", added) + " " + msg);
    }

    public static double calculateMatchingMass(double mass) {
        double added = XTandemUtilities.getCleaveCMass();
        //      mayBeShowAddedMassX(  mass,  added,"getCleaveCMass");
        mass += added;
// changed to match comet  8-apr-2015
//        added = XTandemUtilities.getCleaveNMass();
//        //     mayBeShowAddedMassX(  mass,  added,"getCleaveNMass");
//        mass += added;
//        added = XTandemUtilities.getProtonMass();
//        //    mayBeShowAddedMassX(  mass,  added,"getProtonMass");
//        mass += added;
        return mass;
    }

    public static double calculateMassFromMatchingMass(double mass) {
        mass -= XTandemUtilities.getCleaveCMass();
// changed to match comet  8-apr-2015
//        mass -= XTandemUtilities.getCleaveNMass();
//        mass -= XTandemUtilities.getProtonMass();
        return mass;
    }

    public static double getProtonMass() {
        return gProtonMass;
    }

    public static void setProtonMass(double pProtonMass) {
        gProtonMass = pProtonMass;
    }

    public static double getCleaveNMass() {
        return gCleaveNMass;
    }

    public static void setCleaveNMass(double pCleaveNMass) {
        gCleaveNMass = pCleaveNMass;
    }

    public static double getCleaveCMass() {
        return gCleaveCMass;
    }

    public static void setCleaveCMass(double pCleaveCMass) {
        gCleaveCMass = pCleaveCMass;
    }

    /**
     * documents that I have no clue why I am doing
     * the next operation
     *
     * @return
     */
    public static boolean isDoneForUnknownReasaon() {
        return true;
    }

    /**
     * note we want to break - usually used in debugging
     */
    public static void breakHere() {

    }


    /**
     * convert a String to something which sorts well ablpabeticalli - if it is a number append
     * 00 else return the string
     *
     * @param s
     * @return
     */
    public static String asAlphabeticalId(String s) {
        // can we trreat as number
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i)))
                return s.trim(); // not a number
        }
        // ok it is a number
        return String.format("%08d", Integer.parseInt(s));    // return padded with 0 so it alphabetizes well

    }

    /**
     * throw an exception after the date use for testing patched to the coed when you do not wsnt to forget them
     *
     * @param year  like 2012 - 1900 internally subtacted
     * @param month j1-12 1 internally subtracted
     * @param day   1-31
     */
    public static void workUntil(int year, int month, int day) {
        GregorianCalendar now = new GregorianCalendar();
        GregorianCalendar stopWorking = new GregorianCalendar(year - 1900, month - 1, day);
        if (now.after(stopWorking))
            throw new IllegalStateException("Temporary patch has expired");
    }

    /**
     * quick and dirty test when hunting for doubles
     *
     * @param test
     * @param answer
     * @return
     */
    public static boolean isCloseTo(double test, double answer) {
        double range = Math.max(Math.max(Math.abs(test), Math.abs(answer)), 0.01);
        return Math.abs(test - answer) < (range / 300);
    }

    public static InputStream getResourceStream(String resourceStr) {
        String resource = resourceStr.replace("res://", "");
        final InputStream stream = THIS_CLASS.getResourceAsStream(resource);

        if (stream == null)
            throw new IllegalArgumentException("Cannot open resource " + resourceStr);
        return stream;
    }


    public static InputStream getResourceStream(Class theClass, String resourceStr) {
        String resource = resourceStr.replace("res://", "");
        final InputStream stream = theClass.getResourceAsStream(resource);

        if (stream == null)
            throw new IllegalArgumentException("Cannot open resource " + resourceStr);
        return stream;
    }


    public static InputStream getDescribedStream(String name) {
        if (name.startsWith("res://"))
            return getResourceStream(name);
        try {
            File test = new File(name);
            if (!test.exists())
                return null;
            if (name.endsWith(".gz"))
                return new GZIPInputStream(new FileInputStream(name));
            return new FileInputStream(name);
        }
        catch (IOException e) {

            throw new RuntimeException("the file " + name + " was not found", e);
        }
    }



    public static final String[] RIGHT_OF_DECIMAL_FORMATS = {
            "###################",
            "##################.#",
            "#################.##",
            "################.###",
            "###############.####",
            "##############.#####",
            "#############.######",
            "############.#######",
            "###########.########",
            "##########.#########",
            "#########.##########",
    };

    public static final String[] SCIENTIFIC_FORMATS = {
            "0E+000",
            "0.0E000",
            "0.00E000",
            "0.000E000",
            "0.0000E+000",
            "0.00000E+000",
            "0.000000E+000",
            "0.0000000E+000",
            "0.00000000E+000",
            "0.00000000E+000",
            "0.000000000E+000",
    };

    protected static DecimalFormat getRightOfDecimal(int rightOfDecimal) {
        if (rightOfDecimal < 0 || rightOfDecimal >= RIGHT_OF_DECIMAL_FORMATS.length)
            throw new IllegalArgumentException(
                    "0 .. " + RIGHT_OF_DECIMAL_FORMATS.length + " are supported"); // ToDo change
        return new DecimalFormat(RIGHT_OF_DECIMAL_FORMATS[rightOfDecimal]);
    }


    protected static DecimalFormat getScientific(int rightOfDecimal) {
        if (rightOfDecimal < 0 || rightOfDecimal >= SCIENTIFIC_FORMATS.length)
            throw new IllegalArgumentException(
                    "0 .. " + SCIENTIFIC_FORMATS.length + " are supported"); // ToDo change
        return new DecimalFormat(SCIENTIFIC_FORMATS[rightOfDecimal]);
    }


    public static String formatScientific(double f, int rightOfDecimal) {
        if(f == 0)
            return "0.0";
        return getScientific(rightOfDecimal).format(f);
    }


    public static String formatFloat(float f, int rightOfDecimal) {
        return getRightOfDecimal(rightOfDecimal).format(f);
    }

    public static String formatDouble(double f, int rightOfDecimal) {
        return getRightOfDecimal(rightOfDecimal).format(f);
    }

    public static <K, T> void insertIntoArrayMap(Map<K, T[]> map, K key, T value) {
        T[] item = map.get(key);
        if (item == null) {
            T[] newValue = (T[]) Array.newInstance(value.getClass(), 1);
            newValue[0] = value;
            map.put(key, newValue);
        }
        else {  // something is there
            T[] newValue = (T[]) Array.newInstance(value.getClass(), item.length + 1);
            System.arraycopy(item, 0, newValue, 0, item.length);
            newValue[item.length] = value;
            map.put(key, newValue);

        }
    }


    public static String encodeData64(float[] data) {
        byte[] bytes = new byte[data.length * FLOAT_SIZE];
        int index = 0;
        for (int i = 0; i < data.length; i++) {
            float v = data[i];
            Base64Float.floatToBytes(v, bytes, index);
            index += FLOAT_SIZE;

        }
        final String s = Base64Float.encodeBytesAsString(bytes);
        return s;
    }


    public static double[] convertToValueType(Double[] inp) {
        double[] ret = new double[inp.length];
        for (int i = 0; i < inp.length; i++) {
            ret[i] = inp[i];

        }
        return ret;
    }


    public static int[] convertToValueType(Integer[] inp) {
        int[] ret = new int[inp.length];
        for (int i = 0; i < inp.length; i++) {
            ret[i] = inp[i];

        }
        return ret;
    }


    public static float[] convertToValueType(Float[] inp) {
        float[] ret = new float[inp.length];
        for (int i = 0; i < inp.length; i++) {
            ret[i] = inp[i];

        }
        return ret;
    }


    /**
     * parse an xml file using a specific handler
     *
     * @param is !null stream
     * @return !null key value set
     */
    public static Map<String, String> readNotes(String str) {
        final InputStream is = getDescribedStream(str);
        return readNotes(is, str);
    }


    /**
     * parse an xml file using a specific handler
     *
     * @param is !null stream
     * @return !null key value set
     */
    public static <T> T parseFileString(String str, AbstractXTandemElementSaxHandler<T> handler1) {
        final InputStream is = getDescribedStream(str);
        try {
            return parseFile(is, handler1, str);
        }
        finally {
            try {
                is.close();
            }
            catch (IOException e) {

            }
        }
    }


    /**
     * parse an xml file using a specific handler
     *
     * @param is !null stream
     * @return !null key value set
     */
    public static <T> T parseFile(InputStream is, AbstractXTandemElementSaxHandler<T> handler1, String url) {
        DelegatingSaxHandler handler = new DelegatingSaxHandler();
        if (handler1 instanceof ITopLevelSaxHandler) {
            handler1.setHandler(handler);
            handler.pushCurrentHandler(handler1);
            handler.parseDocument(is);
            T ret = handler1.getElementObject();
            return ret;

        }
        else {
            final BiomlHandler<T> bh = new BiomlHandler(handler, handler1, url);
            handler.pushCurrentHandler(bh);
            handler.parseDocument(is);

            T ret = bh.getFileObject();
            return ret;
        }
    }


    /**
     * parse an xml file using a specific handler
     *
     * @param is !null stream
     * @return !null key value set
     */
    public static Map<String, String> readNotes(String str, IStreamOpener opener, Object... otherData) {
        final InputStream is = opener.open(str, otherData);
        return readNotes(is, str);
    }

    public static final int[] ADDED_MASSES = {443, 467,/* m_ParentStream mass 663, */ 762};




    /**
     * parse a bioml file holding nothing but note tags
     *
     * @param is !null stream
     * @return !null key value set
     */
    public static Map<String, String> readNotes(InputStream is, String url) {
        if(url.endsWith(".xml")) {
            DelegatingSaxHandler handler = new DelegatingSaxHandler();
            final BiomlHandler handler1 = new BiomlHandler(handler, url);
            handler.pushCurrentHandler(handler1);
            handler.parseDocument(is);

            if (handler1 instanceof AbstractXTandemElementSaxHandler) {
                AbstractXTandemElementSaxHandler handlerx = (AbstractXTandemElementSaxHandler) handler1;
                Map<String, String> notes = handlerx.getNotes();
                return notes;

            }
        }
        if(url.endsWith(".params")) {
            Properties pps = new Properties();
            try {
                pps.load(is);
               return remapProperties(pps);
             } catch (IOException e) {
                throw new RuntimeException(e);

            }
        }

         throw new UnsupportedOperationException("Fix This"); // ToDo

    }

    private static Map<String, String> remapProperties(Properties pps) {
        Map<String, String> ret = new HashMap<String, String>();
        for (Object k : pps.keySet()) {
            String k1 = (String) k;
            String property = pps.getProperty(k1);
            String name = remapNames(k1,property,ret);

            ret.put(name, property) ;
        }
        return ret;
    }

    private static String remapNames(String name,String value,Map<String, String> properties) {
        if("fragment_bin_tol".equals(name))  return "comet.fragment_bin_tol";
        if("fragment_bin_offset".equals(name)) {
            properties.put("protein, cleavage N-terminal mass change","1.007276466");   // todo FIX
            return "comet.fragment_bin_offset";
        }
        if("peptide_mass_tolerance".equals(name))  return "comet.mass_tolerance";
        if("max_fragment_charge".equals(name))  return "comet.max_fragment_charge";
        if("use_B_ions".equals(name))  {
            properties.put("scoring, b ions" ,"yes") ;
            return name;
        }
        if("use_Y_ions".equals(name))  {
            properties.put("scoring, y ions" ,"yes") ;
            return name;
        }
        return name;
    }

//
//    /**
//     * parse a bioml file holding nothing but note tags
//     *
//     * @param is !null stream
//     * @return !null key value set
//     */
//    public static MultiScorer readMultiScore(String text, IMainData god) {
//        DelegatingSaxHandler handler = new DelegatingSaxHandler();
//        final MultiScoreHandler handler1 = new MultiScoreHandler(god, handler);
//        handler.pushCurrentHandler(handler1);
//        InputStream is = XMLUtilities.stringToInputStream(text);
//        handler.parseDocument(is);
//
//        MultiScorer notes = handler1.getElementObject();
//        return notes;
//    }



    /**
     * parse a bioml file holding nothing but note tags
     *
     * @param is !null existing readible file
     * @return !null key value set
     */
    public static Map<String, String> readNotes(File file) {
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return readNotes(is, file.getName());
    }


    /**
     * *******************************
     * Error handling code for MGF parse failuer
     * *******************************
     */
    public static final int MAX_NUMBER_BAD_MGF_LINES = 2000;
    private static int gNumberBadMGFLines = 0;

    /**
     * we cannot parse a line of the form mass peak i.e.  370.2438965 3.906023979 in an
     * mgf file - the first  MAX_NUMBER_BAD_MGF_LINES output a message on stderr than
     * exceptions are thrown
     *
     * @param line !null line we cannot handle
     * @throws IllegalStateException after  MAX_NUMBER_BAD_MGF_LINES are seen
     */
    protected static void handleBadMGFData(String line) throws IllegalStateException {
        if (gNumberBadMGFLines++ > MAX_NUMBER_BAD_MGF_LINES)
            throw new IllegalStateException("cannot read MGF data line " + line +
                    " failing after " + gNumberBadMGFLines + " errors");
        System.err.println("Cannot parse mgf line " + line);

    }



    /**
     * comvert   PEPMASS=459.17000000000002 8795.7734375   into  459.17
     *
     * @param pLine line as above
     * @return indicasted mass
     */
    public static double parsePepMassLine(final String pLine) {
        final double mass;
        String numeric = pLine.substring("PEPMASS=".length());
        String massStr;
        // at least once we have seen \t as a separator
        if(numeric.contains("\t"))  {
            massStr = numeric.split("\t")[0].trim();
        }
        else {
            massStr = numeric.split(" ")[0].trim();
        }
         try {
            mass = Double.parseDouble(massStr);
            return mass;
        }
        catch (NumberFormatException e) {
            throw e;

        }
    }


    protected static String buildMGFTitle(String line) {
        String[] items = line.split(",");
        String label = line.substring("TITLE=".length());
        String spot_id = label;
        if (items.length > 1)
            spot_id = items[1].trim().substring("Spot_Id: ".length());
        return spot_id;
    }

    protected static Map<String, String> commentLineToProperties(String line) {
        line = line.substring("Comment: ".length());
        Map<String, String> ret = new HashMap<String, String>();
        int position = nextItemPosition(line);
        while (position < line.length()) {
            String itemEquals = line.substring(0, position).trim();
            addMapEntry(itemEquals, ret);
            line = line.substring(position);
            position = nextItemPosition(line);
        }
        return ret;
    }

    private static void addMapEntry(String itemEquals, Map<String, String> ret) {
        int index = itemEquals.indexOf("=");
        String key = itemEquals.substring(0, index);
        String value = itemEquals.substring(index + 1);
        ret.put(key, value);
    }

    private static int nextItemPosition(String line) {
        String test = "";
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (' ' == c) {
                return Math.min(i + 1, line.length());
            }
            if ('\"' == c) {
                i = skipToEndOfQuote(line, i + 1);
                test = line.substring(0, i);
            }
        }
        return line.length();
    }

    private static int skipToEndOfQuote(String line, int i) {
        for (; i < line.length(); i++) {
            char c = line.charAt(i);
            if ('\\' == c) {
                i++;
                continue;
            }
            if ('\"' == c) {
                return i + 1;
            }
        }
        throw new UnsupportedOperationException("never get here");
    }


    public static final int MAXIMUM_INFLATION_FACTOR = 10;

    public static byte[] decompressBytes(final byte[] pDecoded) {
        try {
            Inflater inf = new Inflater();
            inf.setInput(pDecoded);
            byte[] holder = new byte[MAXIMUM_INFLATION_FACTOR * pDecoded.length];
            int number = inf.inflate(holder);
            byte[] ret = new byte[number];
            System.arraycopy(holder, 0, ret, 0, number);
            return ret;
        }
        catch (DataFormatException e) {
            throw new RuntimeException(e);

        }
    }


    /**
     * documents that I do not understand why a need a specific
     * line of code - in production code this should never be called
     *
     * @return always returns true but in production might throw an exception
     */
    public static boolean isForNotUnderstoodReason() {
        return true;
    }


    /**
     * create an object given a class name
     *
     * @param cls       !null expected type
     * @param className !null className
     * @param <T>       the type of return
     * @return !null return
     */
    public static <T> T buildObject(Class<? extends T> cls, String className) {
        try {
            Class target = Class.forName(className);
            T ret = (T) target.newInstance();
            return ret;
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    /*
GROUP: list path,

default parameters - path to default parameter file.
taxonomy information - path to sequence taxonomy file.
GROUP: output,

histogram column width - width of columns in output file.
histograms - display histograms in output file.
log path - sets logging file location.
maximum valid expectation value - highest value for recorded peptides.
message - sets console output processing message.
one sequence copy - sets the mode for writing protein sequences.
parameters - controls output of input parameters
path - output file path.
path hashing - hash file name with date and time of record.
performance - controls output of performance parameters.
proteins - controls output of protein sequences.
results - controls the types of results recorded.
sequence path - output the refinement protein sequence list.
sort results by - controls how spectrum results are sorted.
sequences - controls output of sequence information.
spectra - controls output of spectrum information.
xsl path - sets path for the XSLT style sheet used to view the output XML.
GROUP: protein,

cleavage C-terminal mass change - moiety added to peptide C-terminus by cleavage.
cleavage N-terminal mass change - moiety added to peptide N-terminus by cleavage.
cleavage semi - use semi-enzymatic cleavage rules
cleavage site - specification of specific protein cleavage sites
C-terminal residue modification mass - moiety added to the C-terminus of protein.
N-terminal residue modification mass - moiety added to the N-terminus of protein.
modified residue mass file - modify the default residue masses for any or all amino acids.
quick acetyl - protein N-terminal modification detection.
quick pyrolidone - peptide N-terminus cyclization detection.
stP bias - interpretation of peptide phosphorylation models.
taxon - specification of the taxonomy keyword.
use annotations - use the annotation file specified in the taxonomy file.
GROUP: refine,

cleavage semi - use semi-enzymatic cleavage rules.
maximum valid expectation value - highest value allowed as a refinement result.
modification mass - alter the list of complete modifications for refinement.
point mutations - test for point mutations.
potential modification mass - potential modifications to test.
potential modification motif -potential modification motifs to test.
potential N-terminus modifications - potential modifications to the N-terminus of a peptide.
refine - controls the use of the refinement modules.
single amino acid polymorphisms - test for known annotated SAPS.
sequence path - input protein sequence list prior to refinement.
spectrum synthesis - controls the use of spectrum synthesis scoring.
tic percent - alter the frequency of output tics during refinement.
unanticipated cleavage - controls the use of cleavage at every residue.
use annotations - use the annotation file specified in the taxonomy file.
use potential modifications for full refinement - controls the use of refinement modifications in all refinement modules.
GROUP: residue,

modification mass - specification of modifications of residues.
potential modification mass - specificiation of potential modifications of residues.
potential modification motif - specification of potential modification motifs.
GROUP: scoring,

a ions - allows the use of a-ions in scoring.
b ions - allows the use of b-ions in scoring.
c ions - allows the use of c-ions in scoring.
cyclic permutation - compensate for very small sequence list files.
include reverse - automatically perform "reversed database" search.
maximum missed cleavage sites - sets the number of missed cleavage sites.
minimum ion count - sets the minimum number of ions required for a peptide to be scored.
x ions - allows the use of x-ions in scoring.
y ions - allows the use of y-ions in scoring.
z ions - allows the use of z-ions in scoring.
GROUP: spectrum,

contrast angle - sets contrast angle for removing duplicate spectra.
dynamic range - sets the dynamic range for scoring spectra.
fragment mass error - fragment ion mass tolerance (chemical average mass).
fragment mass error units - units for fragment ion mass tolerance (chemical average mass).
fragment mass type - use chemical average or monoisotopic mass for fragment ions.
fragment monoisotopic mass error - fragment ion mass tolerance (monoisotopic mass).
fragment monoisotopic mass error units - units for fragment ion mass tolerance (monoisotopic mass).
minimum fragment mz - sets minimum fragment m/z to be considered.
minimum peaks - sets the minimum number of peaks required for a spectrum to be considered.
minimum m_ParentStream m+h -sets the minimum m_ParentStream M+H required for a spectrum to be considered.
neutral loss mass - sets the centre of the window for ignoring neutral molecule losses.
neutral loss window - sets the width of the window for ignoring neutral molecule losses.
m_ParentStream monoisotopic mass error minus - parent ion M+H mass tolerance lower window.
m_ParentStream monoisotopic mass error plus - parent ion M+H mass tolerance upper window.
m_ParentStream monoisotopic mass error units - parent ion M+H mass tolerance window units.
m_ParentStream monoisotopic mass isotope error - anticipate carbon isotope m_ParentStream ion assignment errors.
path - path for input spectrum file.
path type - type of input spectrum file.
sequence batch size - alter how protein sequences are retrieved from a FASTA file.
threads - worker threads to be used for calculation.
total peaks - maximum number of peaks to be used from a spectrum.
use neutral loss window - controls the use of the neutral loss window.
use noise suppression - controls the use of noise suppression routines.
use contrast angle - controls the use of contrast angle duplicate spectrum deletion.

     */

    public static void validateParameters(IParameterHolder params) {
        //GROUP: list path
        //    GROUP: output,

        String key = "k-score, histogram scale";
        Double dp = params.getDoubleParameter(key);
        if (dp != null)
            setKScoreBinningFactor(dp);

        //  histogram column width - width of columns in output file.
        //   histograms - display histograms in output file.
        validateParameterNotSet(params, "log path");
        validateParameterNotSet(params, "maximum valid expectation value");
        validateParameterNotSet(params, "message");
        validateParameterNotSet(params, "one sequence copy");
        //       parameters - controls output of input parameters
        //     path - output file path.
        //     path hashing - hash file name with date and time of record.
        //     performance - controls output of performance parameters.
        //     proteins - controls output of protein sequences.
        //     results - controls the types of results recorded.
        //     sequence path - output the refinement protein sequence list.
        validateParameterNotSet(params, "sort results by");
        //      sort results by - controls how spectrum results are sorted.
        //     sequences - controls output of sequence information.
        //      spectra - controls output of spectrum information.
        validateParameterNotSet(params, "xsl path");
        //      xsl path - sets path for the XSLT style sheet used to view the output XML.

        // GROUP: protein,
        validateParameterNotSet(params, "cleavage C-terminal mass change");
        validateParameterNotSet(params, "cleavage N-terminal mass change");
        validateParameterNotSetToValuer(params, "cleavage semi", "yes");
        validateParameterNotSet(params, "cleavage site");

        //       C-terminal residue modification mass - moiety added to the C-terminus of protein.
        //     N-terminal residue modification mass - moiety added to the N-terminus of protein.
        validateParameterNotSet(params, "modified residue mass file");
        validateParameterNotSet(params, "quick acetyl");
        validateParameterNotSet(params, "quick pyrolidone");
        validateParameterNotSet(params, "stP bias");
        validateParameterNotSet(params, "taxon");
        validateParameterNotSet(params, "use annotations");

        // GROUP: refine,
        validateParameterNotSetToValuer(params, "cleavage semi", "yes");
        validateParameterNotSet(params, "maximum valid expectation value");
        validateParameterNotSet(params, "modification mass");
        validateParameterNotSet(params, "point mutations");
        validateParameterNotSet(params, "potential modification mass");
        validateParameterNotSet(params, "potential modification motif");
        validateParameterNotSet(params, "potential N-terminus modifications");
        validateParameterNotSet(params, "single amino acid polymorphisms");
        validateParameterNotSet(params, "sequence path");
        validateParameterNotSet(params, "spectrum synthesis");
        validateParameterNotSet(params, "tic percent");
        validateParameterNotSet(params, "unanticipated cleavage");
        validateParameterNotSet(params, "use annotations");
        validateParameterNotSet(params, "use potential modifications for full refinement");

        // GROUP: residue,
        validateParameterNotSet(params, "modification mass");
        validateParameterNotSet(params, "potential modification mass");
        validateParameterNotSet(params, "potential modification motif");

        // GROUP: scoring,
        validateParameterNotSet(params, "cyclic permutation");
        validateParameterNotSet(params, "minimum ion count");

        // GROUP: spectrum,
        validateParameterNotSet(params, "use contrast angle");
        validateParameterNotSet(params, "contrast angle");
        validateParameterNotSet(params, "sequence batch size");
        validateParameterNotSet(params, "neutral loss window");
    }

    /**
     * throw an exception if  a parameter yiou cannot handle has any value
     *
     * @param parameter !null parameter
     */
    protected static void validateParameterNotSet(IParameterHolder params, String parameter) {
        final String s = params.getParameter(parameter);
        if (s == null)
            return;
        if ("".equals(s))
            return;
        throw new IllegalStateException("Unhandled parameter value parameter " + parameter + " set to " + s);
    }

    /**
     * throw an exception if  a parameter yiou cannot handle has a specified value
     *
     * @param parameter !null parameter
     * @param badValue  !null bad values
     */
    protected static void validateParameterNotSetToValuer(IParameterHolder params, String parameter, String badValue) {
        final String s = params.getParameter(parameter);
        if (s == null)
            return;
        if ("".equals(s))
            return;
        if (!badValue.equalsIgnoreCase(s))
            return;   // not the case we care about
        throw new IllegalStateException("Unhandled parameter value parameter " + parameter + " set to " + s);

    }

    /**
     * get all values from a map sorted by key value  assumed key impleents comparable
     *
     * @param map !null map
     * @param cls class of return array
     * @param <T> same as cls
     * @return !null array
     */
    public static <T> T[] getSortedValues(Map<? extends Object, T> map, Class<T> cls) {
        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys);
        List<T> holder = new ArrayList<T>();
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            holder.add(map.get(key));
        }
        T[] ret = (T[]) Array.newInstance(cls, holder.size());
        holder.toArray(ret);
        return ret;

    }

    public static final double INTEGER_TOLERANCE = 0.00001;

    public static boolean isInteger(double mz) {
        return Math.abs(mz - (int) mz) < INTEGER_TOLERANCE;
    }

    public static final double DEFAULT_MAX_DIFFERENCE = 0.0001;


    public static boolean equivalentFloat(float d1, float d2) {
        return equivalentFloat(d1, d2, DEFAULT_MAX_DIFFERENCE);

    }

    public static boolean equivalentFloat(float d1, float d2, double MaxDifference) {
        return Math.abs(d1 - d2) < MaxDifference;

    }

    /**
     * make the scan tags balance on a mzxml file
     *
     * @param inFile
     */
    public static void fixScanTagsSafely(File inFile) {
        if (!inFile.exists())
            return;
        if (inFile.isDirectory()) {
            File[] files = inFile.listFiles();
            if (files == null)
                return;
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                fixScanTagsSafely(file);
            }
            return;
        }
        String path = inFile.getPath();
        File tmpFile = new File(path + ".tmp");
        tmpFile.delete(); // we need to make sure this is not there so we can rename
        if (!inFile.renameTo(tmpFile))
            throw new IllegalStateException("cannot make temp file from " + path);

        File outFile = new File(path);
        fixScanTags(tmpFile, outFile);
        tmpFile.delete();
    }

    public static void fixScanTags(File inFile, File outFile) {
        if (!inFile.exists() || !inFile.canRead())
            return;
        try {
            LineNumberReader inp = new LineNumberReader(new FileReader(inFile));
            PrintWriter out = new PrintWriter(new FileWriter(outFile));
            fixScanTags(inp, out);
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }

    }
     public static final String SCHEMA_LOCATION_TAG = "xsi:schemaLocation=\"";

    /**
     * sometimes scan tags are not closed and are nested - this unnests and makes sure the
     * scan tags are properly closed
     *
     * @param inp
     * @param out
     * @throws IOException
     */
    public static void fixScanTags(LineNumberReader inp, PrintWriter out) throws IOException {
        int numberScans = 0;
        try {
            boolean scanEndSeen = false;
            boolean scanSeen = false;
            boolean seenSchemaLocation = false;
            String line = inp.readLine();
            while (line != null) {
                /// Schema location is sometimes a malformed url
                if (!seenSchemaLocation) {
                    int schemaLocIndex = line.indexOf(SCHEMA_LOCATION_TAG);
                    // patch out  xsi:schemaLocation so we will not see a malformed url tag
                    if (schemaLocIndex != -1) {
                        seenSchemaLocation = true; // only deal with this once
                        int schemaEndIndex = line.indexOf("\"", SCHEMA_LOCATION_TAG.length() + schemaLocIndex);
                        if (schemaEndIndex != -1) {
                            String newLine = line.substring(0, schemaLocIndex);
                            if (schemaEndIndex < line.length() - 2)
                                newLine += line.substring(schemaEndIndex + 1);
                            line = newLine;
                        }
                    }
                }
                if (line.contains("</scan>")) {
                    // skip this ie a repeat
                    if (scanEndSeen) {        // skip
                        line = inp.readLine();
                        continue;
                    }
                    else {
                        scanEndSeen = true;
                    }
                }
                if (line.contains("<scan ")) {
                    numberScans++;
                    // skip this ie a repeat
                    if (scanSeen && !scanEndSeen) {
                        out.println("</scan>");
                    }
                    scanSeen = true; // we have seen the first scan
                    scanEndSeen = false;
                }
                out.println(line);
                line = inp.readLine();
            }
        }
        finally {
            out.close();
            //       m_Notes.outputLine("Scans Seen " + numberScans);
        }

    }


    /**
     * write a string representing Now
     *
     * @return non-null String
     */
    public static String nowTimeString() {
        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm");
        return (fmt.format(new Date()));
    }

    /**
     * true if except for spaces the strings are the same -
     * this is useful for testing xml
     *
     * @param sx1
     * @param sx2
     * @return
     */
    public static boolean equivalentExceptSpace(String sx1, String sx2) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();

        String s1 = printingOnly(sx1);
        String s2 = printingOnly(sx2);
        for (int i = 0; i < s1.length(); i++) {
            if (i >= s2.length())
                return false;
            char c1 = s1.charAt(i);
            char c2 = s2.charAt(i);
            if (c1 != c2)
                return false;
            sb1.append(c1); // the equal part
            sb2.append(c2); // the equal part
        }
        if (s1.length() != s2.length())
            return false;
        return true;
    }

    public static String printingOnly(String s1) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < s1.length(); i++) {
            char c = s1.charAt(i);
            if (Character.isWhitespace(c))
                continue;
            if (Character.isISOControl(c))
                continue;
            sb.append(c);

        }
        return sb.toString();

    }


    /**
     * build map containing all values in theirs where there is no identical key in pMine
     *
     * @param pMine   !null map
     * @param pTheirs !null map
     * @param <K>     key type both maps
     * @param <T>     value type both maps
     * @return !null map with differences
     */
    public static <K, T> Map<K, T> buildDifferenceMap(final Map<K, T> pMine, final Map<K, T> pTheirs) {
        Set<K> diff = new HashSet<K>(pTheirs.keySet());
        diff.removeAll(pMine.keySet());
        Map<K, T> ret = new HashMap<K, T>();
        for (K key : diff)
            ret.put(key, pTheirs.get(key));
        return ret;
    }

    /**
     * make sure that the two maps have the same keys
     *
     * @param map1 !null map
     * @param map2 !null map
     * @param <K>  key type
     * @param <V>  value type
     */
    public static <K, V> void toCommonKeySet(final Map<K, V> map1, final Map<K, V> map2) {
        List<K> holder = new ArrayList<K>();
        // find all keys of map1 not in map2
        for (K key : map1.keySet()) {
            if (!map2.containsKey(key))
                holder.add(key);
        }
        // and drop them
        for (K key : holder)
            map1.remove(key);
        holder.clear();
        // find all keys of map2 not in map1
        for (K key : map2.keySet()) {
            if (!map1.containsKey(key))
                holder.add(key);
        }
        // and drop them
        for (K key : holder)
            map2.remove(key);


    }




    /**
     * print lines in a tandem output showing sequences scored
     *
     * @param fileName
     */
    public static void handleXTandemListing(String fileName) {
        Set<String> holder = new HashSet<String>();
        String[] strings = FileUtilities.readInLines(fileName);
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i].trim();
            if (string.startsWith("add Sequence ")) {
                string = string.substring("add Sequence ".length());
                if (string.length() > 4)
                    holder.add(string);
            }
            if (string.startsWith("set Sequence ")) {
                string = string.substring("set Sequence ".length());
                if (string.length() > 4)
                    holder.add(string);
            }
        }
        String[] ret = new String[holder.size()];
        holder.toArray(ret);
        Arrays.sort(ret);
        for (int i = 0; i < ret.length; i++) {
            String s = ret[i];
            System.out.println(s);
        }
    }


    /**
     * look for a fragment in the database
     *
     * @param fragment !null fragment
     * @param database !null database
     */
    public static void showFragmentMass(String fragment, String database) {
        fragment += ",";
        Set<String> holder = new HashSet<String>();
        int index = 200;
        File f = new File(database);
        if (!f.exists() || !f.isDirectory())
            throw new IllegalArgumentException("database does not exist " + database);

        while (index < 5000) {
            if (showFragmentInIndex(fragment, index++, database))
                break;
        }
    }

    /**
     * look for a fragment in the database
     *
     * @param pFragment
     * @param mass
     * @param pDatabase
     * @return
     */
    private static boolean showFragmentInIndex(final String pFragment, final int mass, final String pDatabase) {
        String fileName = pDatabase + "/" + XTandemHadoopUtilities.buildFileNameFromMass(mass);
        File f = new File(fileName);
        if (!f.exists())
            return false;
        String[] strings = FileUtilities.readInLines(f);
        if (strings == null)
            return false;
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            if (string.startsWith(pFragment)) {
                System.out.println(string);
                return true;
            }
        }
        return false;
    }

}
