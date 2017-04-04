//Mapper
package Retrieval;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import Retrieval.Retrieval;

public class RetrievalMapper extends Mapper<LongWritable, Text, TableKey, TableValue> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	//List<String> paramsKeys = new ArrayList<String>(Arrays.asList("keyVocabs1","keyVocabs2","keyVocabs3","keyVocabs4","keyVocabs5","keyVocabs6","keyVocabs7","keyVocabs8"));
		String[] paramsKeys = {"keyVocabs1","keyVocabs2","keyVocabs3","keyVocabs4","keyVocabs5","keyVocabs6","keyVocabs7","keyVocabs8"};
		ArrayList<String> keyVocabs = new ArrayList<String>();
		int j =0;
		String keyVocabsNum = new String();

		Configuration conf=context.getConfiguration();
		keyVocabsNum = conf.get("keyVocabsNum");
		int keyVocabsLen =  Integer.parseInt(keyVocabsNum);
		for(j = 0;j < keyVocabsLen; j++ ){
			keyVocabs.add( conf.get(paramsKeys[j]));
		}

		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path("./input");
		//String inputDir = context.getConfiguration().get("inputDir");
		FileStatus[] status_list = fs.listStatus(inFile);




        String[] strArray	   = value.toString().split(";");
        String[] words = strArray[0].split("\\s+");
        String   word = words[0];
        int  docFreq = Integer.parseInt(words[1]); 

        for(j = 0;j < keyVocabsLen; j++){
        	if(word.equals(keyVocabs.get(j))){
        		int len = strArray.length;
		        int i;
		        ArrayList<WordInfo> WordInfos = new ArrayList<WordInfo>();
		        for(i = 1;i < len;i++){
		        	String[] terms = strArray[i].split(" ");
		        	String fileIdString = terms[0];
		        	int fileId = Integer.parseInt(fileIdString);
		        	String fileName = new String(status_list[fileId].getPath().getName());
		        	Double termFreq = Double.parseDouble(terms[1]);

		        	ArrayList<Long> offset = new ArrayList<Long>();
					String  inputStr = terms[2];
					String  patternStr = "(\\d+)";
			    	Pattern pattern    = Pattern.compile(patternStr);
			    	Matcher matcher    = pattern.matcher(inputStr);
			    	while (matcher.find()) {
			    		offset.add(Long.parseLong(matcher.group().trim()));
			    	}
			    	WordInfo WordInfo = new WordInfo(fileName, termFreq, offset);
			    	WordInfos.add(WordInfo);

		        }

				TableKey   tKey   = new TableKey(word);
				TableValue tValue = new TableValue(docFreq, WordInfos);

				context.write(tKey, tValue);
        	}
	        
	    }
    
	}
}