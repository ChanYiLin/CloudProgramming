//Retrieval.java
package Retrieval;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Retrieval {
	static ArrayList<String> keyVocabs;
    static String   INPUT_DIR  = "./input";
	static String   OUTPUT_DIR = "./scoreOutput/part-r-00000";

	public static void main(String[] args) throws Exception {
		//List<String> paramsKeys = new ArrayList<String>(Arrays.asList("keyVocabs1","keyVocabs2","keyVocabs3","keyVocabs4","keyVocabs5","keyVocabs6","keyVocabs7","keyVocabs8"));
		String[] paramsKeys = {"keyVocabs1","keyVocabs2","keyVocabs3","keyVocabs4","keyVocabs5","keyVocabs6","keyVocabs7","keyVocabs8"};




		System.out.println("************************");
		System.out.println("Search:");

	    Scanner input      = new Scanner(System.in);
	    String  inputStr   = input.nextLine();
	    String  patternStr = "([A-Za-z]+)\\s*\"(.*)\"";
	    Pattern pattern    = Pattern.compile(patternStr);
	    Matcher matcher    = pattern.matcher(inputStr);
	    input.close();
	    
	    // Split str
	    matcher.find();
        String action = matcher.group(1); 
        String params = matcher.group(2);

        keyVocabs = new ArrayList<String>();
        ArrayList<String> andVocabs = new ArrayList<String>();
        ArrayList<String> orVocabs = new ArrayList<String>();
        ArrayList<String> notVocabs = new ArrayList<String>();
        ArrayList<String>    logics = new ArrayList<String>();
        ArrayList<String> searchVocabs = new ArrayList<String>();
    	// Params might be
        // Basic: cat, Advanced: cat or dog

        String[] paramsArr = params.split("\\s");

        andVocabs.add(paramsArr[0]);
        keyVocabs.add(paramsArr[0]);
        searchVocabs.add(paramsArr[0]);
        	
        if (paramsArr.length > 1) {   	
        	for (int i = 1; i < paramsArr.length; i+= 2) {
        		if (paramsArr[i].equals("and")) {
	        		andVocabs.add(paramsArr[i+1]);
                    searchVocabs.add(paramsArr[i+1]);
	        	} else if (paramsArr[i].equals("or")){
                    orVocabs.add(paramsArr[i+1]);
                    searchVocabs.add(paramsArr[i+1]);
                }else{
	        		notVocabs.add(paramsArr[i+1]);
	        	}

	        	keyVocabs.add(paramsArr[i+1]);
        		logics.add(paramsArr[i]);
        	}
        }
        int orVocabsLen = orVocabs.size();
        int andVocabsLen = andVocabs.size();
        int notVocabsLen = notVocabs.size();
	    int    logicsLen =    logics.size();
		System.out.println("************************");
		for(int i = 0;i < keyVocabs.size();i++){
			System.out.println("search:" + keyVocabs.get(i));
		}
		System.out.println("************************");
		System.out.println("pre processing....");

		String keyVocabsNum = String.valueOf(keyVocabs.size());
		Configuration conf = new Configuration();

		conf.set("keyVocabsNum",keyVocabsNum);
		for(int i = 0;i < keyVocabs.size();i++){
			conf.set(paramsKeys[i],keyVocabs.get(i));
		}




        int fileNum = 0;
        FileSystem fss = FileSystem.get(conf);
        FileStatus[] status_list = fss.listStatus(new Path(INPUT_DIR));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                fileNum++;
            }
        }

        conf.set("fileNum", String.valueOf(fileNum));

		Job job1 = Job.getInstance(conf, "Retrieval");
		job1.setJarByClass(Retrieval.class);
		
		// set the class of each stage in mapreduce
 		job1.setMapperClass(RetrievalMapper.class);
 		//job1.setCombinerClass(InvertedIndexCombiner.class);
		//job.setSortComparatorClass(InvertedIndexKeyComparator.class);
		//job1.setPartitionerClass(InvertedIndexPartitioner.class);	
		job1.setReducerClass(RetrievalReducer.class);
		
		// 設置 Map 輸出類型  
        job1.setMapOutputKeyClass(TableKey.class);  
        job1.setMapOutputValueClass(TableValue.class);  
          
        // 設置 Reduce 輸出類型  
        job1.setOutputKeyClass(TableKey.class);  
        job1.setOutputValueClass(Scoreboard.class);  

		FileInputFormat.addInputPath(job1, new Path(args[0]));				
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);


        /********** pre processing over ********/

        //Read the result file
		HashMap<String, Scoreboard> scoreMap = new HashMap<String, Scoreboard>();
		HashMap<String, Scoreboard> term = readFileCreateHash(OUTPUT_DIR);
        scoreMap.putAll(term);



        HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
        
        for (int i = 0; i < orVocabsLen; i++) { 
            Scoreboard tValue = scoreMap.get(orVocabs.get(i)); 

            for (TermFileInfo TermFileInfo : tValue.getTermFileInfos()) {
                
                if (tempMap.get(TermFileInfo.getFileName()) == null) {
                    tempMap.put(TermFileInfo.getFileName(), 1);
                } else {
                    tempMap.replace(TermFileInfo.getFileName(), tempMap.get(TermFileInfo.getFileName())+1);
                }
            }
        }


        for (int i = 0; i < andVocabsLen; i++) { 
            Scoreboard tValue = scoreMap.get(andVocabs.get(i)); 

            for (TermFileInfo TermFileInfo : tValue.getTermFileInfos()) {
                
                if (tempMap.get(TermFileInfo.getFileName()) == null) {
                    tempMap.put(TermFileInfo.getFileName(), 1);
                } else {
                    tempMap.replace(TermFileInfo.getFileName(), tempMap.get(TermFileInfo.getFileName())+1);
                }
            }
        }
        for (int i = 0; i < notVocabsLen; i++) {
            Scoreboard tValue = scoreMap.get(notVocabs.get(i));
                
            for (TermFileInfo TermFileInfo : tValue.getTermFileInfos()) {       
                if (tempMap.get(TermFileInfo.getFileName()) != null) {
                    tempMap.replace(TermFileInfo.getFileName(), 0);
                }
            }
        }


        
        
        HashMap<String, Integer> FinalMap = new HashMap<String, Integer>();
        for (String key : tempMap.keySet()) {
            if (tempMap.get(key) != 0) {   //collect the file that has been referenced
                FinalMap.put(key, tempMap.get(key));
            }
        }
        System.out.println(FinalMap);
        
        
        HashMap<String, TermFileInfo> tValueMap = new HashMap<String, TermFileInfo>();
        HashMap<String, Double>     weights = new HashMap<String, Double>();

        for (String vocab : searchVocabs) {
            for (TermFileInfo TermFileInfo : scoreMap.get(vocab).getTermFileInfos()) {

                for (String key : FinalMap.keySet()) {
                
                    if (TermFileInfo.getFileName().equals(key)) {         
                        if (tValueMap.get(key) == null) { 
                            tValueMap.put(key, TermFileInfo);
                        } else {
                            TermFileInfo tmp = tValueMap.get(key);
                            tmp.setScore(tmp.getScore() + TermFileInfo.getScore());
                            tmp.appendOffset(TermFileInfo.getOffset());
                            tValueMap.replace(key, tmp);
                        }
                        
                        if (weights.get(key) == null) { //update weights 裡關於這個file的分數
                            weights.put(key, TermFileInfo.getScore());
                        } else {
                            weights.replace(key, weights.get(key) + TermFileInfo.getScore());
                        }
                    }
                    
                }
            }
        }
        
        TreeMap<Double, TermFileInfo> pairMap = createPairMap(weights, tValueMap);
        NavigableMap<Double, TermFileInfo> nmap = pairMap.descendingMap();
        
        // Results
        int Len = (nmap.size() < 10) ? nmap.size() : 10;
        for (int i = 0; i < Len; i++) {

            Entry<Double, TermFileInfo> entry    = nmap.pollFirstEntry();
            String                    fileName = entry.getValue().getFileName();
            Double                    score    = entry.getKey();


            //Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            
            Path inFile = new Path(INPUT_DIR + "/" + fileName);
            if (!fs.exists(inFile))
                System.out.println("Input file not found");
            FSDataInputStream in = fs.open(inFile);

            System.out.println("Rank " + Integer.toString(i+1) + "\t" + fileName + "score = " + score);
            System.out.println("************************");
            
            int len = entry.getValue().getOffset().size();
            for (int j = 0; j < len; j++) {
                // Jump to that position in O(1);
                //Text line = new Text();
                System.out.println("[offest = " + entry.getValue().getOffset().get(j) + "]:");
                System.out.print("=>  ");
                byte[] buffer = new byte[30];
                //in.seek(entry.getValue().getOffset().get(j)-10);
                //LineReader reader = new LineReader(in); 

                in.read(entry.getValue().getOffset().get(j)-10,buffer,0,30);
                String s1 = new String(buffer);
                System.out.println(s1);
            }
            
            System.out.println("************************");
        }        
      	System.exit(job1.waitForCompletion(true)?0:1);

  }
  public static HashMap<String, Scoreboard> readFileCreateHash(String dir) throws NumberFormatException, IOException {
        HashMap<String, Scoreboard> scoreMap = new HashMap<String, Scoreboard>();
        
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(dir);
		if (!fs.exists(inFile))
  			System.out.println("Input file not found");
  		FSDataInputStream in = fs.open(inFile);


  		Text line = new Text();
		LineReader reader = new LineReader(in); //read line by line

        
        while (reader.readLine(line) > 0) { 

        	String str = line.toString();
            String[] strArr = str.split(";");


            String[] strPart   = strArr[0].split("[\\s]+");
            String   word      = strPart[0];
            
            // Collect TermFileInfos
            ArrayList<TermFileInfo> TermFileInfos = new ArrayList<TermFileInfo>();
            int len = strArr.length;        
            for (int i = 1; i < len; i++) {
                TermFileInfo TermFileInfo = createTermFileInfo(strArr[i]);
                TermFileInfos.add(TermFileInfo);
            }
        
            Scoreboard sValue = new Scoreboard(TermFileInfos);
                
            scoreMap.put(word, sValue);    
        }   

        fs.close();
        return scoreMap;    
    }

    public static TermFileInfo createTermFileInfo(String strArr) {
        String[] strPart = strArr.split("[\\s]+");
    

        String fileName     = strPart[0];
        Double score        = Double.parseDouble(strPart[1]);
        ArrayList<Long> offset = new ArrayList<Long>();

        // Retrieve offset
        String  inputStr   = strPart[2];
        String  patternStr = "(\\d+)";
        Pattern pattern    = Pattern.compile(patternStr);
        Matcher matcher    = pattern.matcher(inputStr);
        while (matcher.find()) {
            offset.add(Long.parseLong(matcher.group().trim()));
        }
        
        return new TermFileInfo(fileName, score, offset);
    }

    public static TreeMap<Double, TermFileInfo> createPairMap(HashMap<String, Double> weights, HashMap<String, TermFileInfo> tValueMap) {
        TreeMap<Double, TermFileInfo> pairMap = new TreeMap<Double, TermFileInfo>();
        
        
        for (String vocab : weights.keySet()) {
            pairMap.put(weights.get(vocab), tValueMap.get(vocab));
        }
        return pairMap;
    }

}

