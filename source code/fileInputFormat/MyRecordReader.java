package cn.hpc.fileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<Text, Text> {
    FileSplit split;
    Configuration configuration;
    Text key = new Text();
    Text value = new Text();
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    /**
     * key -> identifier
     * value -> sequence
     */
    @Override
    public boolean nextKeyValue() throws IOException {
        if (isProgress) {
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);

            int startIndex = 1;
            String firstLine = "";
            char c;

            c = (char) inputStream.read();
            while (c != '\n') {
                startIndex++;
                firstLine += c;
                c = (char) inputStream.read();
            }

            byte[] buf = new byte[(int) split.getLength()];
            IOUtils.readFully(inputStream, buf, 0, buf.length - startIndex);

            key.set(firstLine);
            value.set(buf, 0, buf.length - startIndex);

            IOUtils.closeStream(inputStream);

            isProgress = false;
            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {

    }
}
