//PageRank.java
package PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class PageRank {
	//private static String FIRST_INPUT = "./buildgraph/part-r-00000";
    private static NumberFormat nf = new DecimalFormat("00");
    private static int titleNum = 0;
    private static double danglingRankSum = 0.0;
 	public static int main(String[] args) throws Exception {

	double[] intArray = new double[100];
        int runs;
        double error = 0.0;
        double pageRankSum = 0.0;
        boolean isCompleted = false;
        int danglingNode = 0;
        FileSystem fsIn = FileSystem.get(new Configuration());
        Path in = new Path("./buildgraph/part-r-00000");
        FSDataInputStream fstreamIn = fsIn.open(in);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstreamIn)); 
        String tmp = new String();
        while(br.ready()){
            tmp = br.readLine();
            String[] strArr = tmp.split("\t");
            if(strArr.length == 2){
                danglingNode++;
            }
            titleNum++; 
        }
        System.out.println("**************");
        System.out.println("danglingNode: " + danglingNode);
        System.out.println("titleNum: " + titleNum);
        System.out.println("**************");
        
        String inPath = new String(); 
        String lastResultPath = new String();
        for(runs = 0;;runs++){

            if(runs == 0){
                inPath = "./buildgraph";
                lastResultPath = "./ranking/iter" + nf.format(runs);
                isCompleted = runPageRank1(inPath, lastResultPath);
            }
            else{

                danglingRankSum = 0.0;
                danglingNode = 0;
                pageRankSum = 0;
                fsIn = FileSystem.get(new Configuration());
                in = new Path("./ranking/iter" + nf.format(runs-1) + "/part-r-00000");
                fstreamIn = fsIn.open(in);
                br = new BufferedReader(new InputStreamReader(fstreamIn));  
                tmp = "";
                while(br.ready()){
                    tmp = br.readLine();
                    String[] strArr = tmp.split("\t");
                    pageRankSum += Double.parseDouble(strArr[1]);
                    if(strArr.length == 3){
                        danglingRankSum += Double.parseDouble(strArr[1]);
                        danglingNode++;
                    }

                }
                System.out.println("**************");
                System.out.println("runs: " + runs + "start");
                System.out.println("danglingNode: " + danglingNode);                
                System.out.println("danglingRankSum: " + danglingRankSum);
                System.out.println("pageRankSum: " + pageRankSum);
                System.out.println("**************");
                

                inPath = "./ranking/iter" + nf.format(runs-1);
                lastResultPath = "./ranking/iter" + nf.format(runs);
                isCompleted = runPageRank(inPath, lastResultPath);

                error = calculateDistance(runs);
                System.out.println("**************");
                System.out.println("runs: " + runs + "finish");
		intArray[runs-1] = error;
                System.out.println("error: " + intArray[runs-1]);
                System.out.println("**************");
                if(error < 0.001)
                    break;

            }
            
        }

        inPath = "./ranking/iter" + nf.format(runs-1);
        lastResultPath = "./ranking/result";
        isCompleted = sortResult(inPath, lastResultPath);


        System.out.println("**************");
        System.out.println("totle runs: " + runs);
        System.out.println("final error: " + error);
	System.out.println("error list: ");
	int k = 0;	
	for(k = 0;k < runs;k++)
		System.out.println(intArray[k]+" => ");
        System.out.println("Top 10:");
        fsIn = FileSystem.get(new Configuration());
        in = new Path("./ranking/result" + "/part-r-00000");
        fstreamIn = fsIn.open(in);
        br = new BufferedReader(new InputStreamReader(fstreamIn));  
        tmp = "";
        int counter = 1;
        while(br.ready()){
            tmp = br.readLine();
            System.out.println(counter + ":\t" + tmp);
            counter++;
            if(counter>11)
                break;
        }

        System.out.println("**************");

        fsIn.close();
        return 0;
    }

    private static boolean runPageRank1(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        System.out.println("titleNum: " + titleNum);

        conf.set("titleNum",String.valueOf(titleNum));

        Job job1=Job.getInstance(conf);
        job1.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job1.setMapperClass(PageRank1Mapper.class);
        job1.setReducerClass(PageRank1Reducer.class);
   
        System.out.println("Set Mapper class and Reducer class success!");

        // 設置 Map 輸出類型  
        job1.setMapOutputKeyClass(Text.class);  
        job1.setMapOutputValueClass(Text.class);  

        System.out.println("Set Mapper class output success!");

        // 設置 Reduce 輸出類型  
        job1.setOutputKeyClass(Text.class);  
        job1.setOutputValueClass(InfoList.class);

        System.out.println("Set Reducer class output success!"); 

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));
    
        return job1.waitForCompletion(true);
    }
    private static boolean runPageRank(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();

        conf.set("titleNum",String.valueOf(titleNum));
        conf.set("danglingRankSum",String.valueOf(danglingRankSum));

        Job job1=Job.getInstance(conf);
        job1.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job1.setMapperClass(PageRankMapper.class);
        job1.setReducerClass(PageRankReducer.class);
   
        
        // 設置 Map 輸出類型  
        job1.setMapOutputKeyClass(Text.class);  
        job1.setMapOutputValueClass(Text.class);  
          
        // 設置 Reduce 輸出類型  
        job1.setOutputKeyClass(Text.class);  
        job1.setOutputValueClass(Text.class); 

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));
    
        return job1.waitForCompletion(true);
    }

    private static boolean sortResult(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
    
        Job job1=Job.getInstance(conf);
        job1.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job1.setMapperClass(SortMapper.class);
        job1.setReducerClass(SortReducer.class);
   
        
        // 設置 Map 輸出類型  
        job1.setMapOutputKeyClass(PageRankResult.class);  
        job1.setMapOutputValueClass(NullWritable.class);  
        
        // 設置 Reduce 輸出類型  
        job1.setOutputKeyClass(PageRankResult.class);  
        job1.setOutputValueClass(NullWritable.class); 

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));
    
        return job1.waitForCompletion(true);
    }

    private static double calculateDistance(int runs) throws IOException {
        double sum = 0.0;

        FileSystem fsIn = FileSystem.get(new Configuration());
        Path in = new Path("./ranking/iter" + nf.format(runs) + "/part-r-00000");
        FSDataInputStream fstreamIn = fsIn.open(in);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstreamIn)); 
        String tmp = new String();
        while(br.ready()){
            tmp = br.readLine();
            StringTokenizer tokenizer = new StringTokenizer(tmp, "\t");
            tokenizer.nextToken();
            tokenizer.nextToken();
            sum += Double.parseDouble(tokenizer.nextToken()); 
        }

        fsIn.close();
        return sum;
    }

}

