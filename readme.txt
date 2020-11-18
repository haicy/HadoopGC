****************************************************************************                             
	           HadoopHRCM
  (Hadoop based Hybrid Referential Compression Method )

          Copyright (C) 2020                  
****************************************************************************

1. Introduction

1.1 HadoopHRCM is implemented with Java. It is used to compress large collections of genomes stored in HDFS.

1.2 BSC executable file should be put in the same directory of the executable jar package.

1.3 Grant BSC file executable permission (e.g. chmod 777 bsc)

****************************************************************************

2. Use

2.1 Compress

//Command:
hadoop jar HadoopHRCM.jar cn/hpc/Driver {chromosome}
   -HadoopHRCM.jar is the compression executable file, required.
   -cn/hpc/Driver is the main class name of the compression method, required.
   -{chromosome} is the to-be-compressed chromosome, required.
   
//Notice:
(1) lib directory should be put in the HadoopHRCM.jar
(2) the reference sequence should be put in the hdfs://master:9000/reference/ directory and the filename should be set as {chromosome}.fa
(3) the to-be-compressed file paths text file should be put in the ./path/ directory and hdfs://master:9000/path directory and the filename should be set as {chromosome}.txt.

//Output:
(1) compressed file named {chromosome}.bsc in current directory
(2) decompressed file named filename.fa in the result directory

2.2 Decompress

//Command:
java -jar Decompress.jar {reference sequence} {compressed file}  {file name directory}  {output directory}
    -Decompress.jar is the decompression executable file,required.
	-{reference sequence} is the reference sequence of the compression
	-{compressed file} is the compressed file of the compression
	-{file name directory} is the directory of the original file, get the file names from the directory
	-{output directory} is the directory storing the decompressed sequences

//Output:
    decompressed file in the {output directory} 

****************************************************************************

3. Example

3.1 You can download the executable jar package and the test datasets in the test directory.

3.2 compress and decompress hg17_chr22.fa and hg18_chr22.fa, using hg13_chr22.fa as reference. Put hg13_chr22.fa in the hdfs://master:9000/reference, put hg17_chr22.fa and hg18_chr22.fa in the hdfs://master:9000/chr22. The paths of to-be-compressed files are written in chr22.txt in turn. Put chr22.txt in the same directory of HadoopHRCM.jar and hdfs://master:9000/path directory respectively.

    hadoop jar HadoopHRCM.jar cn/hpc/Driver chr22.txt
    output: chr22.bsc


    java -jar Decompress.jar hg13_chr22.fa chr22.bsc  hdfs://master:9000/chr22 decompressed/
    output: decomressed/hg17_chr22.fa 
	        decomressed/hg18_chr22.fa
***************************************************************************
