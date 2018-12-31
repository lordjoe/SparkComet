package com.lordjoe.distributed.input;

import com.lordjoe.distributed.SparkUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.systemsbiology.hadoop.HadoopUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * com.lordjoe.distributed.input.MultiMZXMLScanInputFormat
 * User: Steve
 * Date: 9/24/2014
 */
public class MultiMZXMLScanInputFormat extends FileInputFormat<String, String> implements Serializable {
    public static final String MZMLFOOTER =
            "   </msRun>\n" +
            "    <indexOffset>0</indexOffset>\n" +
            "  <sha1>0</sha1>\n" +
            "</mzXML>\n";

    public static final String END_SCANS = "</msRun>";
    public static final int DEFAULTS_PECTRA_SPLIT = 1000;

    /**
     * read header of mzXML file until first scan
     *
     * @param p path of the file
     * @return non-null header
     */
    public static String readMZXMLHeader(Path p) {
        try {
            FSDataInputStream open = SparkUtilities.getHadoopFileSystem().open(p);
            LineNumberReader rdr = new LineNumberReader(new InputStreamReader(open.getWrappedStream()));
            StringBuilder sb = new StringBuilder();
            String line = rdr.readLine();
            sb.append(line);
            sb.append("\n");
            line = rdr.readLine();
            sb.append(line);
            sb.append("\n");
            if (!sb.toString().contains("<mzXML"))
                throw new IllegalArgumentException("path is not an mzXML file " + p.toString());

            line = rdr.readLine();
            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = rdr.readLine();
                if (line.contains("<scan"))
                    break;
            }
            rdr.close();

            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }


    public static final boolean FORCE_ONE_MAPPER = false;
    // todo run off a parameter
    // setting this small forces many mappers
    @SuppressWarnings("UnusedDeclaration")
    public static final int SPLIT_BLOCK_SIZE = 10 * 1024 * 1024;
    public static final int MIN_BLOCK_SIZE = 10 * 1024;


    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    private String m_Extension = "mzXML";

    public MultiMZXMLScanInputFormat() {
    }


    public String getExtension() {
        return m_Extension;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setExtension(final String pExtension) {
        m_Extension = pExtension;
    }

    public boolean isSplitReadable(InputSplit split) {
        if (!(split instanceof FileSplit))
            return true;
        FileSplit fsplit = (FileSplit) split;
        Path path1 = fsplit.getPath();
        return isPathAcceptable(path1);
    }

    protected boolean isPathAcceptable(final Path pPath1) {
        String path = pPath1.toString().toLowerCase();
        if (path.startsWith("part-r-"))
            return true;
        String extension = getExtension();
        if (extension != null && path.endsWith(extension.toLowerCase()))
            return true;
        if (extension != null && path.endsWith(extension.toLowerCase() + ".gz"))
            return true;
        //noinspection RedundantIfStatement
        if (extension == null)
            return true;
        return false;
    }

    @Override
    public RecordReader<String, String> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) {
        if (isSplitReadable(split)) {
            Configuration configuration = context.getConfiguration();
            return new MultiMZXMLFileReader(configuration.get("fileHeader"), configuration.getInt("spectra", DEFAULTS_PECTRA_SPLIT));
        } else
            return NullRecordReader.INSTANCE; // do not read
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        String fname = file.getName().toLowerCase();
        if (fname.endsWith(".gz"))
            return false;

        //noinspection RedundantIfStatement
        if (FORCE_ONE_MAPPER)
            return false;
        return true;
    }

    /**
     * Generate the list of files and make them into FileSplits.
     * This needs to be copied to insert a filter on acceptable data
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);
        long desiredMappers = job.getConfiguration().getLong("org.systemsbiology.jxtandem.DesiredDatabaseMappers", 0);


        //   maxSize = SPLIT_BLOCK_SIZE; // force more mappers
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();

        Path[] paths = getInputPaths(job);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < paths.length; i++) {
            Path path = paths[i];
            System.err.println("Input path " + path.toString());
        }
        List<FileStatus> fileStatuses = listStatus(job);
        // if there is only one file we may force more than the default mappers
        boolean forceNumberMappers = fileStatuses.size() == 1;

        for (FileStatus file : fileStatuses) {
            Path path = file.getPath();
            if (!isPathAcceptable(path))   // filter acceptable data
                continue;
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                // use desired mappers to force more splits
                if (forceNumberMappers && desiredMappers > 0) {
                    final long ms1 = length / desiredMappers;
                    final long ms2 = Math.max(MIN_BLOCK_SIZE, ms1);
                    maxSize = Math.min(maxSize, ms2);
                }
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                    splits.add(new FileSplit(path, length - bytesRemaining, splitSize,
                            blkLocations[blkIndex].getHosts()));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts()));
                }
            } else if (length != 0) {
                splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
            } else {
                //Create empty hosts array for zero length files
                splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }

        HadoopUtilities.validateSplits(splits);
        //   LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }

    /**
     * Value is the mzXML record  minus the comment line
     * Key is the comment line
     */
    public class MultiMZXMLFileReader extends RecordReader<String, String> {

        private CompressionCodecFactory compressionCodecs = null;
        private long m_Start;  // start this split
        private long m_End;   // end this split
        private long m_Current;  // current position
        private String m_Key;
        private StringBuffer m_Valuex = new StringBuffer();
        private int m_NumberFields = 1000;
        private final Text m_Line = new Text(); // use to read current line
        private int m_MaxLineLength;
        private StringBuilder m_Data = new StringBuilder();
        private String m_CurrentLine;
        private FSDataInputStream m_FileIn; // input stream needed for position
        private LineReader m_Input; // current reader
        private final String header;
        private final int nSpectra;

        public MultiMZXMLFileReader(String header, int numberSpectra) {
            this.header = header;
            nSpectra = numberSpectra;
        }

        public void initialize(InputSplit genericSplit,
                               TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) genericSplit;
            Configuration job = context.getConfiguration();
            m_MaxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                    Integer.MAX_VALUE);
            Text buffer = new Text();
            m_Data.setLength(0);
            m_Start = split.getStart();
            m_End = m_Start + split.getLength();
            final Path file = split.getPath();
            compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(file);
            boolean skipFirstLine = false;

            // open the file and seek to the m_Start of the split
            FileSystem fs = file.getFileSystem(job);
            m_FileIn = fs.open(split.getPath());
            if (codec != null) {
                m_Input = new LineReader(codec.createInputStream(m_FileIn), job);
                m_End = Long.MAX_VALUE;
            } else {
                if (m_Start != 0) {
                    skipFirstLine = true;
                    --m_Start;
                    m_FileIn.seek(m_Start);
                }
                m_Input = new LineReader(m_FileIn, job);
            }
            // not at the beginning so go to first line
            if (skipFirstLine) {  // skip first line and re-establish "start".
                m_Start += m_Input.readLine(buffer, 0,
                        (int) Math.min((long) Integer.MAX_VALUE, m_End - m_Start));
            }
            m_Current = m_Start;
            m_Key = split.getPath().getName();
        }

        /**
         * look for a line starting with > and read until it closes
         *
         * @return true if there is data
         * @throws IOException
         */
        public boolean nextKeyValue() throws IOException {

            m_Valuex.setLength(0);
            if (m_Current > m_End) {  // we are the the end of the split
                m_Key = null;
                return false;
            }


            // read more data
            if (m_CurrentLine == null) {
                m_CurrentLine = readNextLine();
                if (m_CurrentLine != null && m_CurrentLine.contains("</msRun>")) {
                    m_CurrentLine = null;
                }
                if (m_CurrentLine == null) { // end of file
                    if (m_Valuex.length() == 0)
                        return false;
                    return true;
                }
            }

            // lines starting with > are a new field in mzXML files
            while (m_FileIn.getPos() < m_End && m_CurrentLine != null && !m_CurrentLine.contains("<scan ")) {
                m_CurrentLine = readNextLine();
            }


            if (m_CurrentLine == null || !m_CurrentLine.contains("<scan ")) {  // we are the the end of data
                return false;
            }

            // starting new read
            m_Key = UUID.randomUUID().toString(); //
            m_Valuex.setLength(0);

            int nFields = 0;
            boolean readOK = true;
            while (readOK && nFields++ < m_NumberFields) {
                readOK = readNextField();
            }

            m_Current = m_FileIn.getPos();
            return true;
        }

        private boolean readNextField() throws IOException {
            if (m_CurrentLine != null) {
                m_Data.setLength(0); // clear the buffer
                m_Data.append(m_CurrentLine);
                m_Data.append("\n");
            }
            m_CurrentLine = readNextLine();

            // keep reading
            while (m_CurrentLine != null && !m_CurrentLine.contains("<scan ")) {
                m_Data.append(m_CurrentLine);
                m_Data.append("\n");
                m_CurrentLine = readNextLine();
            }

            if (m_Data.length() == 0) {  // cannot read
                return false;
            }

            String str = m_Data.toString();
            m_Valuex.append(str);
            m_Data.setLength(0); // clear the buffer
            return true; // one field read
        }


        protected String readNextLine() throws IOException {
            int newSize = m_Input.readLine(m_Line, m_MaxLineLength,
                    Math.max(Math.min(Integer.MAX_VALUE, (int) (m_End - m_Current)),
                            m_MaxLineLength));
            if (newSize == 0)
                return null;
            m_Current += newSize; // new position
            String ret = m_Line.toString();
            if (ret.contains("</msRun>") || ret.contains("<index") || ret.contains("<offset"))
                return null;
            return ret;
        }

        @Override
        public String getCurrentKey() {
            return m_Key;
        }

        @Override
        public String getCurrentValue() {
            if (m_Valuex.length() == 0)
                return null;
            String text = header + m_Valuex.toString() + MZMLFOOTER;

            return text;
        }

        /**
         * Get the progress within the split
         */
        public float getProgress() {
            long totalBytes = m_End - m_Start;
            long totalhandled = m_Current - m_Start;
            return ((float) totalhandled) / totalBytes;
        }

        public synchronized void close() throws IOException {
            if (m_Input != null) {
                m_Input.close();
                m_FileIn = null;
            }
        }
    }
}
