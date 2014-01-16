package eu.scape_project.pt.mapred.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PtInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) {
        return new PtRecordReader();
    }
}


