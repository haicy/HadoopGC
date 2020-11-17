package cn.hpc.compress;

import cn.hpc.constant.MyConstant;
import cn.hpc.pojo.MatchEntry;
import cn.hpc.pojo.Sequence;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class CompressMapper extends Mapper<Text, Text, Text, Sequence> {
    Text k;
    Sequence v;

    private int refCodeLen;
    private char[] refCode;

    private int refLowVecLen;
    private int[] refLowVecBegin;
    private int[] refLowVecLength;

    private int[] refLoc;
    private int[] refBucket;

    private int seqNumber;   // the number of the to-be-compressed sequences
    private int secondSeqNum;   // the number of the second-level references

    private List<String> tarSeqName;   // to-be-compressed sequences file name
    private Map<String, Integer> tarSeqIdMap;    // map of the sequence file name and sequence id: key -> fileName value -> id

    // map of the sequence file name and its first matched result: key -> fileName value -> matchResult
    public HashMap<String, List<MatchEntry>> matchResultMap = new HashMap<>(MyConstant.MAX_SEQ_NUM);

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        refCodeLen = 0;
        refCode = new char[MyConstant.MAX_CHA_NUM];

        // record the lowercase from refLow[1], diff_lowercase_loc[i]=0 means mismatch
        refLowVecLen = 1;
        refLowVecBegin = new int[MyConstant.VEC_SIZE];
        refLowVecLength = new int[MyConstant.VEC_SIZE];

        seqNumber = 0;

        tarSeqName = new ArrayList<>(MyConstant.MAX_SEQ_NUM);
        tarSeqIdMap = new HashMap<>(MyConstant.MAX_SEQ_NUM);

        // read file from the distributed cache
        URI[] cacheFile = context.getCacheFiles();
        Path filePath = new Path(cacheFile[0].toString());
        FSDataInputStream inputStream = FileSystem.get(cacheFile[0], conf).open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

        int lettersLen = 0;
        String info;
        boolean flag = true;

        br.readLine();
        while ((info = br.readLine()) != null) {
            for (char ch : info.toCharArray()) {
                if (Character.isLowerCase(ch)) {
                    if (flag) {
                        flag = false;
                        refLowVecBegin[refLowVecLen] = lettersLen;
                        lettersLen = 0;
                    }
                    ch = Character.toUpperCase(ch);
                } else {
                    if (!flag) {
                        flag = true;
                        refLowVecLength[refLowVecLen++] = lettersLen;
                        lettersLen = 0;
                    }
                }

                if (ch == 'A' || ch == 'C' || ch == 'G' || ch == 'T') {
                    refCode[refCodeLen++] = ch;
                }

                lettersLen++;
            }
        }

        if (!flag) {
            refLowVecLength[refLowVecLen++] = lettersLen;
        }

        // construct hash table for reference sequence
        kMerHashingConstruct();

        // read the file name of the to-be-compressed sequences
        String[] splits;
        filePath = new Path(cacheFile[1].toString());
        inputStream = FileSystem.get(cacheFile[1], conf).open(filePath);
        br = new BufferedReader(new InputStreamReader(inputStream));
        while ((info = br.readLine()) != null) {
            splits = info.split("/");
            String fileName = splits[splits.length - 1];
            tarSeqName.add(fileName);
            tarSeqIdMap.put(fileName, seqNumber++);
        }
        secondSeqNum = (int) Math.ceil((double) (MyConstant.PERCENT * seqNumber) / 100);
        br.close();
        inputStream.close();
    }

    /**
     * extract the to-be-compressed sequence and the first-level matching
     *
     * @param key   the filename
     * @param value all the first-level compressed result
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        k = new Text(fileName);
        v = new Sequence();
        v.setIdentifier(key.toString());

        int seqCodeLen = 0;
        char[] seqCode = new char[MyConstant.MAX_CHA_NUM];

        int seqLowVecLen = 0;
        int[] seqLowVecBegin = new int[MyConstant.VEC_SIZE];
        int[] seqLowVecLength = new int[MyConstant.VEC_SIZE];

        int seqNVecLen = 0;
        int[] seqNVecBegin = new int[MyConstant.VEC_SIZE];
        int[] seqNVecLength = new int[MyConstant.VEC_SIZE];

        int speChaLen = 0;
        int[] speChaPos = new int[MyConstant.VEC_SIZE];
        int[] speChaCh = new int[MyConstant.VEC_SIZE];

        String data = value.toString();

        int lineWidth = 0;
        for (int i = 0; i < data.length() && data.charAt(i) != '\n'; i++) {
            lineWidth++;
        }
        v.setLineWidth(lineWidth);

        int lettersLen = 0, nLettersLen = 0;
        boolean flag = true, nFlag = false;
        for (char ch : data.toCharArray()) {
            if (ch != '\n') {
                if (Character.isLowerCase(ch)) {
                    if (flag) {
                        flag = false;
                        seqLowVecBegin[seqLowVecLen] = lettersLen;
                        lettersLen = 0;
                    }
                    ch = Character.toUpperCase(ch);
                } else {
                    if (!flag) {
                        flag = true;
                        seqLowVecLength[seqLowVecLen++] = lettersLen;
                        lettersLen = 0;
                    }
                }
                lettersLen++;

                if (ch == 'A' || ch == 'C' || ch == 'G' || ch == 'T') {
                    seqCode[seqCodeLen++] = ch;
                } else if (ch != 'N') {
                    speChaPos[speChaLen] = seqCodeLen;
                    speChaCh[speChaLen++] = ch - 'A';
                }

                if (!nFlag) {
                    if (ch == 'N') {
                        seqNVecBegin[seqNVecLen] = nLettersLen;
                        nLettersLen = 0;
                        nFlag = true;
                    }
                } else {
                    if (ch != 'N') {
                        seqNVecLength[seqNVecLen++] = nLettersLen;
                        nLettersLen = 0;
                        nFlag = false;
                    }
                }
                nLettersLen++;
            }
        }

        if (!flag) {
            seqLowVecLength[seqLowVecLen++] = lettersLen;
        }

        if (nFlag) {
            seqNVecLength[seqNVecLen++] = nLettersLen;
        }

        v.setId(tarSeqIdMap.get(fileName));
        v.setFileName(fileName);

        v.setLowVecLen(seqLowVecLen);

        v.setNVecLen(seqNVecLen);
        v.setNVecBegin(seqNVecBegin);
        v.setNVecLength(seqNVecLength);

        v.setSpeChaLen(speChaLen);
        v.setSpeChaPos(speChaPos);
        v.setSpeChaCh(speChaCh);

        if (v.getLowVecLen() > 0 && refLowVecLen > 0) {
            seqLowercaseMatching(v, seqLowVecBegin, seqLowVecLength);
        }
        List<MatchEntry> matchResult = codeFirstMatch(seqCodeLen, seqCode);

        v.setMatchResult(matchResult);
        matchResultMap.put(fileName, matchResult);

        context.write(k, v);
    }

    /**
     * (1) write the  secondSeqNum
     * (2) write the first-level matched result of the sequence id before secondSeqNum
     * note: cannot close the stream, because every map needs to cleanup. Otherwise, other maps will have no stream to use
     */
    @Override
    protected void cleanup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        FileSystem fs;
        InputStream is;
        FSDataOutputStream fos;
        try {
            fs = FileSystem.get(new URI("hdfs://master:9000"), configuration);

            // (1) write secondSeqNum
            Path path = new Path("/temp/secondSeqNum.txt");
            if (!fs.exists(path)) {
                is = new BufferedInputStream(new ByteArrayInputStream(String.valueOf(secondSeqNum).getBytes()));
                fos = fs.create(path);
                IOUtils.copyBytes(is, fos, configuration);
            }

            // (2) write the first-level matched result of the sequence id before secondSeqNum
            for (int i = 1; i <= secondSeqNum; i++) {
                path = new Path("/temp/matchResult" + i + ".txt");
                List<MatchEntry> matchResult = matchResultMap.get(tarSeqName.get(i));
                if (matchResult != null) {
                    if (!fs.exists(path)) {
                        byte[] bytes = JSON.toJSONBytes(matchResult);
                        is = new BufferedInputStream(new ByteArrayInputStream(bytes));
                        fos = fs.create(path);
                        IOUtils.copyBytes(is, fos, configuration);
                    }
                }
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * construct the hash index for reference sequence
     */
    private void kMerHashingConstruct() {
        refLoc = new int[MyConstant.MAX_CHA_NUM];
        refBucket = new int[MyConstant.hashTableLen];
        Arrays.fill(refBucket, -1);

        int value = 0;
        int len = refCodeLen - MyConstant.kMerLen + 1;

        for (int i = MyConstant.kMerLen - 1; i >= 0; i--) {
            value <<= 2;
            value += integerCoding(refCode[i]);
        }
        refLoc[0] = refBucket[value];
        refBucket[value] = 0;

        int shiftBitNum = (MyConstant.kMerLen * 2 - 2);
        for (int i = 1; i < len; i++) {
            value >>= 2;
            value += (integerCoding(refCode[i + MyConstant.kMerLen - 1])) << shiftBitNum;
            refLoc[i] = refBucket[value];
            refBucket[value] = i;
        }
    }

    /**
     * encode 'A C G T'
     */
    private int integerCoding(char ch) {
        switch (ch) {
            case 'A':
                return 0;
            case 'C':
                return 1;
            case 'G':
                return 2;
            case 'T':
                return 3;
            default:
                return -1;
        }
    }

    private void seqLowercaseMatching(Sequence sequence, int[] seqLowVecBegin, int[] seqLowVecLength) {
        //matched lowVec：if low_loc = 0, means mismatch；if low_loc = j, means match the j-th element of the reference lowercase vector
        int[] seqLowVecMatched = new int[MyConstant.VEC_SIZE];

        int seqDiffLowVecLen = 0;
        int[] seqDiffLowVecBegin = new int[MyConstant.VEC_SIZE];
        int[] seqDiffLowVecLength = new int[MyConstant.VEC_SIZE];

        int startPosition = 1;

        for (int i = 0; i < sequence.getLowVecLen(); i++) {
            for (int j = startPosition; j < refLowVecLen; j++) {
                if ((seqLowVecBegin[i] == refLowVecBegin[j]) && (seqLowVecLength[i] == refLowVecLength[j])) {
                    seqLowVecMatched[i] = j;
                    startPosition = j + 1;
                    break;
                }
            }
            if (seqLowVecMatched[i] == 0) {
                for (int j = startPosition - 1; j > 0; j--) {
                    if ((seqLowVecBegin[i] == refLowVecBegin[j]) && (seqLowVecLength[i] == refLowVecLength[j])) {
                        seqLowVecMatched[i] = j;
                        startPosition = j + 1;
                        break;
                    }
                }
            }

            if (seqLowVecMatched[i] == 0) {
                seqDiffLowVecBegin[seqDiffLowVecLen] = seqLowVecBegin[i];
                seqDiffLowVecLength[seqDiffLowVecLen++] = seqLowVecLength[i];
            }
        }

        sequence.setDiffLowVecLen(seqDiffLowVecLen);
        sequence.setDiffLowVecBegin(seqDiffLowVecBegin);
        sequence.setDiffLowVecLength(seqDiffLowVecLength);

        sequence.setLowVecMatched(seqLowVecMatched);
    }

    public List<MatchEntry> codeFirstMatch(int seqCodeLen, char[] seqCode) {
        int prePos = 0;
        int step_len = seqCodeLen - MyConstant.kMerLen + 1;
        int i, j, index, k, refIndex, tarIndex, length, tarValue, maxLength, maxK;
        MatchEntry matchEntry = new MatchEntry();
        List<MatchEntry> matchResult = new ArrayList<>(MyConstant.VEC_SIZE);

        StringBuilder mismatchedStr = new StringBuilder(integerCoding(seqCode[0])); // make the first code as mismatch to generate the first triple
        for (i = 1; i < step_len; i++) {
            tarValue = 0;
            for (j = MyConstant.kMerLen - 1; j >= 0; j--) {
                tarValue <<= 2;
                tarValue += integerCoding(seqCode[i + j]);
            }
            index = refBucket[tarValue];

            if (index > -1) {
                maxLength = -1;
                maxK = -1;

                for (k = index; k != -1; k = refLoc[k]) {
                    refIndex = k + MyConstant.kMerLen;
                    tarIndex = i + MyConstant.kMerLen;
                    length = MyConstant.kMerLen;

                    while (refIndex < refCodeLen && tarIndex < seqCodeLen && refCode[refIndex++] == seqCode[tarIndex++]) {
                        length++;
                    }

                    if (length >= MyConstant.min_rep_len && length > maxLength) {
                        maxLength = length;
                        maxK = k;
                    }
                }

                if (maxLength > -1) {
                    matchEntry.setMisStr(mismatchedStr.toString());
                    matchEntry.setPos(maxK - prePos);   //delta encoding of position
                    matchEntry.setLength(maxLength - MyConstant.min_rep_len);
                    matchResult.add(matchEntry);
                    matchEntry = new MatchEntry();

                    i += maxLength;
                    prePos = maxK + maxLength;
                    mismatchedStr = new StringBuilder();
                    if (i < seqCodeLen) {
                        mismatchedStr.append(integerCoding(seqCode[i]));
                    }
                    continue;
                }
            }
            mismatchedStr.append(integerCoding(seqCode[i]));
        }

        if (i < seqCodeLen) {
            for (; i < seqCodeLen; i++) {
                mismatchedStr.append(integerCoding(seqCode[i]));
            }
            matchEntry.setPos(0);
            matchEntry.setLength(-MyConstant.min_rep_len);
            matchEntry.setMisStr(mismatchedStr.toString());
            matchResult.add(matchEntry);
        }

        return matchResult;
    }
}
