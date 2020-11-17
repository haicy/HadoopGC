package cn.hpc.pojo;

import cn.hpc.constant.MyConstant;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Sequence implements Writable {
    private int id;
    private String fileName;
    private String identifier;
    private int lineWidth;
    private int lowVecLen;
    private int nVecLen;
    private int speChaLen;
    private int diffLowVecLen;
    private int[] nVecBegin;
    private int[] nVecLength;
    private int[] speChaPos;
    private int[] speChaCh;
    private int[] lowVecMatched;
    private int[] diffLowVecBegin;
    private int[] diffLowVecLength;
    private List<MatchEntry> matchResult;

    public Sequence() {
        super();

        int VEC_SIZE = 1 << 20; // length of other data
        id = -1;
        fileName = "";
        identifier = "";   // SequenceExtraction
        lineWidth = 0; // SequenceExtraction
        lowVecLen = 0;    // SequenceExtraction
        nVecLen = 0;   // SequenceExtraction
        speChaLen = 0; // SequenceExtraction
        diffLowVecLen = 0;    // seqLowercaseMatching
        nVecBegin = new int[VEC_SIZE]; // SequenceExtraction
        nVecLength = new int[VEC_SIZE];    // SequenceExtraction
        speChaPos = new int[VEC_SIZE]; // SequenceExtraction
        speChaCh = new int[VEC_SIZE];  // SequenceExtraction
        lowVecMatched = new int[VEC_SIZE];    // seqLowercaseMatching
        diffLowVecBegin = new int[VEC_SIZE];  // seqLowercaseMatching
        diffLowVecLength = new int[VEC_SIZE]; // seqLowercaseMatching
        matchResult = new ArrayList<>(VEC_SIZE);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(fileName);
        out.writeUTF(identifier);
        out.writeInt(lineWidth);
        out.writeInt(lowVecLen);
        out.writeInt(nVecLen);
        out.writeInt(speChaLen);
        out.writeInt(diffLowVecLen);

        out.writeInt(nVecBegin.length);
        for (int i = 0; i < nVecBegin.length; i++) {
            out.writeInt(nVecBegin[i]);
        }

        out.writeInt(nVecLength.length);
        for (int i = 0; i < nVecLength.length; i++) {
            out.writeInt(nVecLength[i]);
        }

        out.writeInt(speChaPos.length);
        for (int i = 0; i < speChaPos.length; i++) {
            out.writeInt(speChaPos[i]);
        }

        out.writeInt(speChaCh.length);
        for (int i = 0; i < speChaCh.length; i++) {
            out.writeInt(speChaCh[i]);
        }

        out.writeInt(lowVecMatched.length);
        for (int i = 0; i < lowVecMatched.length; i++) {
            out.writeInt(lowVecMatched[i]);
        }

        out.writeInt(diffLowVecBegin.length);
        for (int i = 0; i < diffLowVecBegin.length; i++) {
            out.writeInt(diffLowVecBegin[i]);
        }

        out.writeInt(diffLowVecLength.length);
        for (int i = 0; i < diffLowVecLength.length; i++) {
            out.writeInt(diffLowVecLength[i]);
        }

        out.writeInt(matchResult.size());
        for (int i = 0; i < matchResult.size(); i++) {
            out.writeInt(matchResult.get(i).getPos());
            out.writeInt(matchResult.get(i).getLength());
            out.writeUTF(matchResult.get(i).getMisStr());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        fileName = in.readUTF();
        identifier = in.readUTF();
        lineWidth = in.readInt();
        lowVecLen = in.readInt();
        nVecLen = in.readInt();
        speChaLen = in.readInt();
        diffLowVecLen = in.readInt();

        nVecBegin = new int[in.readInt()];
        for (int i = 0; i < nVecBegin.length; i++) {
            nVecBegin[i] = in.readInt();
        }

        nVecLength = new int[in.readInt()];
        for (int i = 0; i < nVecLength.length; i++) {
            nVecLength[i] = in.readInt();
        }

        speChaPos = new int[in.readInt()];
        for (int i = 0; i < speChaPos.length; i++) {
            speChaPos[i] = in.readInt();
        }

        speChaCh = new int[in.readInt()];
        for (int i = 0; i < speChaCh.length; i++) {
            speChaCh[i] = in.readInt();
        }

        lowVecMatched = new int[in.readInt()];
        for (int i = 0; i < lowVecMatched.length; i++) {
            lowVecMatched[i] = in.readInt();
        }

        diffLowVecBegin = new int[in.readInt()];
        for (int i = 0; i < diffLowVecBegin.length; i++) {
            diffLowVecBegin[i] = in.readInt();
        }

        diffLowVecLength = new int[in.readInt()];
        for (int i = 0; i < diffLowVecLength.length; i++) {
            diffLowVecLength[i] = in.readInt();
        }

        int length = in.readInt();
        MatchEntry matchEntry = new MatchEntry();
        matchResult = new ArrayList<>(length << 1);
        for (int i = 0; i < length; i++) {
            matchEntry.setPos(in.readInt());
            matchEntry.setLength(in.readInt());
            matchEntry.setMisStr(in.readUTF());
            matchResult.add(matchEntry);
            matchEntry = new MatchEntry();
        }
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public int getLineWidth() {
        return lineWidth;
    }

    public void setLineWidth(int lineWidth) {
        this.lineWidth = lineWidth;
    }

    public int getLowVecLen() {
        return lowVecLen;
    }

    public void setLowVecLen(int lowVecLen) {
        this.lowVecLen = lowVecLen;
    }

    public int getNVecLen() {
        return nVecLen;
    }

    public void setNVecLen(int nVecLen) {
        this.nVecLen = nVecLen;
    }

    public int getSpeChaLen() {
        return speChaLen;
    }

    public void setSpeChaLen(int speChaLen) {
        this.speChaLen = speChaLen;
    }

    public int getDiffLowVecLen() {
        return diffLowVecLen;
    }

    public void setDiffLowVecLen(int diffLowVecLen) {
        this.diffLowVecLen = diffLowVecLen;
    }

    public int[] getNVecBegin() {
        return nVecBegin;
    }

    public void setNVecBegin(int[] nVecBegin) {
        this.nVecBegin = nVecBegin;
    }

    public int[] getNVecLength() {
        return nVecLength;
    }

    public void setNVecLength(int[] nVecLength) {
        this.nVecLength = nVecLength;
    }

    public int[] getSpeChaPos() {
        return speChaPos;
    }

    public void setSpeChaPos(int[] speChaPos) {
        this.speChaPos = speChaPos;
    }

    public int[] getSpeChaCh() {
        return speChaCh;
    }

    public void setSpeChaCh(int[] speChaCh) {
        this.speChaCh = speChaCh;
    }

    public int[] getLowVecMatched() {
        return lowVecMatched;
    }

    public void setLowVecMatched(int[] lowVecMatched) {
        this.lowVecMatched = lowVecMatched;
    }

    public int[] getDiffLowVecBegin() {
        return diffLowVecBegin;
    }

    public void setDiffLowVecBegin(int[] diffLowVecBegin) {
        this.diffLowVecBegin = diffLowVecBegin;
    }

    public int[] getDiffLowVecLength() {
        return diffLowVecLength;
    }

    public void setDiffLowVecLength(int[] diffLowVecLength) {
        this.diffLowVecLength = diffLowVecLength;
    }

    public List<MatchEntry> getMatchResult() {
        return matchResult;
    }

    public void setMatchResult(List<MatchEntry> matchResult) {
        this.matchResult = matchResult;
    }
}
