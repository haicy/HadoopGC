package cn.hpc.compress;

import cn.hpc.constant.MyConstant;
import cn.hpc.pojo.MatchEntry;
import cn.hpc.pojo.Sequence;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompressReducer extends Reducer<Text, Sequence, Text, NullWritable> {
    MultipleOutputs<Text, NullWritable> multipleOutputs;
    Text k;
    StringBuilder result;

    private int secondSeqNum;   // the number of the second-level references
    public static int seqBucketLen; // seqBucketLen is the minimum prime of the VEC_SIZE

    private List<List<MatchEntry>> matchResultVec;    // the reference vector of the second-level matching

    public static List<int[]> seqBucketVec;
    public static List<List<Integer>> seqLocVec;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        multipleOutputs = new MultipleOutputs<>(context);

        seqBucketLen = getNextPrime(MyConstant.VEC_SIZE);

        FileSystem fs;
        FSDataInputStream fis;
        ByteArrayOutputStream bos;
        try {
            fs = FileSystem.get(new URI("hdfs://master:9000"), configuration);
            byte[] bytes = new byte[8];

            // (1) read the secondSeqNum from HDFS
            secondSeqNum = 0;
            fis = fs.open(new Path("/temp/secondSeqNum.txt"));
            fis.read(bytes);
            for (byte b : bytes) {
                if (b != 0) {
                    secondSeqNum = secondSeqNum * 10 + (b - '0');
                } else {
                    break;
                }
            }
            matchResultVec = new ArrayList<>(this.secondSeqNum);
            seqBucketVec = new ArrayList<>(this.secondSeqNum);
            seqLocVec = new ArrayList<>(this.secondSeqNum);
            System.out.println("reducer get the secondSeqNum. It is " + secondSeqNum);

            // (2) read the matchResult vector from HDFS
            for (int i = 1; i <= secondSeqNum; i++) {
                fis = fs.open(new Path("/temp/matchResult" + i + ".txt"));
                bos = new ByteArrayOutputStream();
                int length = -1;
                while ((length = fis.read(bytes)) != -1) {
                    bos.write(bytes, 0, length);
                }
                List<MatchEntry> matchResult = JSON.parseObject(bos.toString(), new TypeReference<List<MatchEntry>>() {
                });
                System.out.println("reducer reads the " + i + "-th matched result, size is " + matchResult.size());
                matchResultVec.add(matchResult);
                matchResultHashConstruct(matchResult);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Sequence> values, Context context) throws IOException, InterruptedException {
        k = new Text();
        result = new StringBuilder();

        String fileName = context.getCurrentKey().toString();
        for (Sequence sequence : values) {
            int seqId = sequence.getId();
            saveOtherData(sequence);

            if (seqId == 0 || sequence.getMatchResult().size() < 2) {
                saveFirstMatchResult(sequence.getMatchResult());
            } else {
                codeSecondMatch(seqId, sequence.getMatchResult());
            }
            k.set(result.toString());

            if (fileName.contains("genotypes")) {
                String[] ss = fileName.split("\\.");
                multipleOutputs.write(ss[7] + ss[8], k, NullWritable.get());
            } else {
                String[] ss = fileName.split("_");
                multipleOutputs.write(ss[0], k, NullWritable.get());
            }

            multipleOutputs.close();
        }
    }

    private int getNextPrime(int number) {
        int cur = number + 1;
        Boolean prime = false;
        while (!prime) {
            prime = true;
            for (int i = 2; i < Math.sqrt(number) + 1; i++) {
                if (cur % i == 0) {
                    prime = false;
                    break;
                }
            }

            if (!prime) {
                cur++;
            }
        }

        return cur;
    }

    private void matchResultHashConstruct(List<MatchEntry> matchResult) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(MyConstant.VEC_SIZE);
        int[] seqBucket = new int[seqBucketLen];
        Arrays.fill(seqBucket, -1);

        hashValue1 = getHashValue(matchResult.get(0));
        if (matchResult.size() < 2) {
            hashValue2 = 0;
        } else {
            hashValue2 = getHashValue(matchResult.get(1));
        }
        hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < matchResult.size() - 1; i++) {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(matchResult.get(i + 1));
            hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }
        seqLocVec.add(seqLoc);
        seqBucketVec.add(seqBucket);
    }

    private int getHashValue(MatchEntry me) {
        int result = 0;
        for (int i = 0; i < me.getMisStr().length(); i++) {
            result += me.getMisStr().charAt(i) * 92083;
        }
        result += me.getPos() * 69061 + me.getLength() * 51787;
        result %= seqBucketLen;
        return result;
    }

    private void saveOtherData(Sequence sequence) {
        int seqLowVecLen = sequence.getLowVecLen();
        int seqNVecLen = sequence.getNVecLen();
        int seqSpeChaLen = sequence.getSpeChaLen();

        //save the identifier
        result.append(sequence.getIdentifier() + "\n");

        //save the lineWidth
        result.append(sequence.getLineWidth() + "\n");

        //save the lowercase information
        result.append(1 + " ");
        runLengthCodingForLowVecMatched(sequence.getLowVecMatched(), seqLowVecLen);
        savePositionRangeData(sequence.getDiffLowVecLen(), sequence.getDiffLowVecBegin(), sequence.getDiffLowVecLength());

        //save the 'N' character
        savePositionRangeData(seqNVecLen, sequence.getNVecBegin(), sequence.getNVecLength());

        //save the special characters
        savePositionRangeData(seqSpeChaLen, sequence.getSpeChaPos(), sequence.getSpeChaCh());

        result.append("\n");
    }

    private void runLengthCodingForLowVecMatched(int[] vec, int length) {
        List<Integer> code = new ArrayList<>(MyConstant.VEC_SIZE);

        if (length > 0) {
            code.add(vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i - 1] == 1) {
                    cnt++;
                } else {
                    code.add(cnt);
                    code.add(vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);
        }

        int code_len = code.size();
        result.append(code_len + " ");
        for (int i = 0; i < code_len; i++) {
            result.append(code.get(i) + " ");
        }
    }

    private void savePositionRangeData(int vecLen, int[] vecBegin, int[] vecLength) {
        result.append(vecLen + " ");
        for (int i = 0; i < vecLen; i++) {
            result.append(vecBegin[i] + " " + vecLength[i] + " ");
        }
    }

    private void saveFirstMatchResult(List<MatchEntry> mes) {
        for (int i = 0; i < mes.size(); i++) {
            saveMatchEntry(mes.get(i));
        }
    }

    private void saveMatchEntry(MatchEntry matchEntry) {
        result.append(matchEntry.getMisStr() + "\n" + matchEntry.getPos() + " " + matchEntry.getLength() + "\n");
    }

    private void codeSecondMatch(int seqID, List<MatchEntry> matchResult) {
        int hashValue;
        int preSeqId = 1;
        int maxPos = 0, prePos = 0, deltaPos, length, maxLength, deltaLength, seqId = 0, deltaSeqId;
        int id, pos;
        int i;

        List<MatchEntry> misMatchEntry = new ArrayList<>();

        misMatchEntry.add(matchResult.get(0));
        for (i = 1; i < matchResult.size() - 1; i++) {
            hashValue = Math.abs(getHashValue(matchResult.get(i)) + getHashValue(matchResult.get(i + 1))) % seqBucketLen;
            maxLength = 0;
            for (int j = 0; j < Math.min(seqID, secondSeqNum); j++) {
                id = seqBucketVec.get(j)[hashValue];
                if (id != -1) {
                    for (pos = id; pos != -1; pos = seqLocVec.get(j).get(pos)) {
                        length = getMatchLength(matchResultVec.get(j), pos, matchResult, i);
                        if (length > 1 && length > maxLength) {
                            seqId = j + 1;  //the j-th in the seqBucket, but actually the j+1-th in the to-be-compressed sequences
                            maxPos = pos;
                            maxLength = length;
                        }
                    }
                }
            }

            if (maxLength != 0) {
                deltaSeqId = seqId - preSeqId;
                deltaLength = maxLength - 2;
                deltaPos = maxPos - prePos;
                preSeqId = seqId;
                prePos = maxPos + maxLength;

                for (int j = 0; j < misMatchEntry.size(); j++) {
                    saveMatchEntry(misMatchEntry.get(j));
                }

                misMatchEntry = new ArrayList<>();
                result.append(deltaSeqId + " " + deltaPos + " " + deltaLength + "\n");

                i += maxLength - 1;
            } else {
                misMatchEntry.add(matchResult.get(i));
            }
        }

        if (i == matchResult.size() - 1) {
            misMatchEntry.add(matchResult.get(i));
        }
        for (int j = 0; j < misMatchEntry.size(); j++) {
            saveMatchEntry(misMatchEntry.get(j));
        }
    }

    private int getMatchLength(List<MatchEntry> ref_matchResult, int ref_idx, List<MatchEntry> tar_matchResult, int tar_idx) {
        int length = 0;
        while (ref_idx < ref_matchResult.size() && tar_idx < tar_matchResult.size() && compareMatchEntry(ref_matchResult.get(ref_idx++), tar_matchResult.get(tar_idx++))) {
            length++;
        }
        return length;
    }

    private boolean compareMatchEntry(MatchEntry ref, MatchEntry tar) {
        if (ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr())) {
            return true;
        } else {
            return false;
        }
    }
}
