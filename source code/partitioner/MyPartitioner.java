package cn.hpc.partitioner;

import cn.hpc.pojo.Sequence;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Sequence> {
    @Override
    public int getPartition(Text text, Sequence sequence, int numPartitions) {
        return 0;
    }
}
