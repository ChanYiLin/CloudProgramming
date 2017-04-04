//Mapper
package InvertedIndex;

import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndex2Mapper extends Mapper<LongWritable, Text, TableKey, TableValue> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strArray	   = value.toString().split("&gt;");
		String   word    	   = strArray[0];
		String   fileName 	   = strArray[1];
		Double   termFreq 	   = Double.parseDouble(strArray[2]);
		ArrayList<Long> offset = new ArrayList<Long>();
		
		
		String  inputStr   = strArray[3];
		String  patternStr = "(\\d+)";
	    Pattern pattern    = Pattern.compile(patternStr);
	    Matcher matcher    = pattern.matcher(inputStr);
	    while (matcher.find()) {
	    	offset.add(Long.parseLong(matcher.group().trim()));
	    }
	    			
		WordInfo WordInfo = new WordInfo(fileName, termFreq, offset);
	    ArrayList<WordInfo> WordInfos = new ArrayList<WordInfo>();
	    WordInfos.add(WordInfo);
	    
		TableKey   tKey   = new TableKey(word);
		TableValue tValue = new TableValue(0, WordInfos);
		context.write(tKey, tValue);
    	
    
	}
}