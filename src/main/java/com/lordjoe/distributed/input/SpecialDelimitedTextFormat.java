package com.lordjoe.distributed.input;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.apache.hadoop.mapred.*;

import com.google.common.base.Charsets;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

@Public
@Stable
public abstract class SpecialDelimitedTextFormat   extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable,org.apache.hadoop.mapred.InputFormat<LongWritable, Text>
{
    private CompressionCodecFactory compressionCodecs = null;

    public SpecialDelimitedTextFormat() {
    }

    public abstract String getDelimiterText();

    public void configure(JobConf conf) {
        this.compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        return null == codec ? true : codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = getDelimiterText();
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }

        return new LineRecordReader(job, (FileSplit)genericSplit, recordDelimiterBytes);
    }
}

