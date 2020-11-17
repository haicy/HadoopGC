package cn.hpc;

import cn.hpc.compress.CompressMapper;
import cn.hpc.compress.CompressReducer;
import cn.hpc.fileInputFormat.MyFileInputFormat;
import cn.hpc.pojo.Sequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Driver {

    /*
     * argument: the to-be-compressed sequence ID -> chr1、chr2、chr3. . .
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String tarChr = args[0];
        int reduceTasks = 0;    // equals the number of the to-be-compressed sequences
        long inputSize = 0; // the total file size of the to-be-compressed sequences
        String currentPath = System.getProperty("user.dir");

        // 1 get the configuration
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.minsize", "268435456"); //set the split minsize as 256M
        Job job = Job.getInstance(configuration);
        FileSystem fs = FileSystem.get(configuration);

        job.setJarByClass(Driver.class);
        job.setMapperClass(CompressMapper.class);
        job.setReducerClass(CompressReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Sequence.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(MyFileInputFormat.class);

        //the reference sequence, get from the /reference directory of HDFS
        job.addCacheFile(new Path("hdfs://master:9000/reference/" + tarChr + ".fa").toUri());

        //the to-be-compressed sequences file paths, get from the /path directory of HDFS
        job.addCacheFile(new Path("hdfs://master:9000/path/" + tarChr + ".txt").toUri());

        // read the to-be-compressed sequence file paths from local file system, and get the file name
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(currentPath + "/" + tarChr + ".txt"));
            String hdfsPath;
            String[] splits1;
            String[] splits2;
            while ((hdfsPath = br.readLine()) != null) {
                reduceTasks++;
                inputSize += fs.getContentSummary(new Path(hdfsPath)).getLength();

                // set the output file name
                MyFileInputFormat.addInputPaths(job, hdfsPath);
                if (hdfsPath.contains("genotypes")) {
                    splits1 = hdfsPath.split("\\.");
                    MultipleOutputs.addNamedOutput(job, splits1[7] + splits1[8], TextOutputFormat.class, Text.class, NullWritable.class);
                } else {
                    splits1 = hdfsPath.split("/");
                    splits2 = splits1[splits1.length - 1].split("_");
                    MultipleOutputs.addNamedOutput(job, splits2[0], TextOutputFormat.class, Text.class, NullWritable.class);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("fail to open the file that records the path of all genome files");
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        job.setNumReduceTasks(reduceTasks);

        // set the output directory of Reduce task
        Path outPath = new Path("/output/" + tarChr);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // avoid generating empty file like: part-r-00000, the default is 'job.setOutputFormatClass(TextOutputFormat.class)'
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // execute MapReduce tasks
        job.waitForCompletion(true);

        // delete the temp directory
        Path tempPath = new Path("/temp");
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }

        // download the output to local file system and do BSC compression
        String path = currentPath + "/result/" + tarChr;
        Path localPath = new Path(path);
        deleteUnusedResult(path);
        downloadResult(configuration, outPath, localPath);
        compressWithBSC(path);

        // calculate the compression ration, compression time and compression speed
        long outSize = fs.getContentSummary(outPath).getLength();
        System.out.println("The compression ratio is " + inputSize / outSize);
        outSize = new File(path + ".bsc").length();
        System.out.println("The compression ratio with bsc is " + inputSize / outSize);
        long time = (System.currentTimeMillis() - startTime) / 1000;
        long speed = inputSize / 1024 / 1024 / time;
        System.out.println("The compression time is " + time + "s; the compression speed is " + speed + "MB/s");
    }

    /**
     * delete the intermediate file
     */
    private static void deleteUnusedResult(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            if (dir.listFiles() != null) {
                for (File file : dir.listFiles()) {
                    if (file.exists()) {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }

        File tarFile = new File(path + ".tar");
        if (tarFile.exists()) {
            tarFile.delete();
        }
        File bscFile = new File(path + ".bsc");
        if (bscFile.exists()) {
            bscFile.delete();
        }
    }

    /**
     * download result to the local file system
     */
    public static void downloadResult(Configuration configuration, Path hdfsPath, Path localPath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://master:9000"), configuration);
            fs.copyToLocalFile(false, hdfsPath, localPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * BSC compression, put the executable file of BSC in current directory
     */
    public static void compressWithBSC(String path) {
        try {
            String cmd = "tar -cPf " + path + ".tar " + path;
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            cmd = "./bsc e " + path + ".tar " + path + ".bsc -e2";
            process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            cmd = "rm -rf " + path + ".tar " + path;
            process = Runtime.getRuntime().exec(cmd);
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
