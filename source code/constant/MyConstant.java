package cn.hpc.constant;

public class MyConstant {
    public static final int MAX_SEQ_NUM = 2000; // maximum sequence number
    public static final int MAX_CHA_NUM = 1 << 28;  // maximum length of a chromosome
    public static final int PERCENT = 10;   // the percentage of the second-level references
    public static final int kMerLen = 14;   // the length of k-mer
    public static final int kMer_bit_num = 2 * kMerLen; // bit numbers of k-mer
    public static final int hashTableLen = 1 << kMer_bit_num;   // length of hash table
    public static final int VEC_SIZE = 1 << 20; // length for other character arrays
    public static final int min_rep_len = 15;   // minimum replace length, matched string length exceeds min_rep_len, saved as matched information
}
