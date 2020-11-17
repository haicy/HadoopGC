package cn.hpc.fileInputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MyFileInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        RecordReader recordReader = new MyRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
