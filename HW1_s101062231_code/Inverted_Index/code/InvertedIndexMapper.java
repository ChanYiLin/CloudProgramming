package InvertedIndex;

import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, I2Key, I2Value> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // get File name
		FileSplit fileSplit	   = (FileSplit)context.getInputSplit();
      	String    filename     = fileSplit.getPath().getName();

      	Configuration conf = context.getConfiguration();
		String str = conf.get("fileList");
		StringTokenizer itr = new StringTokenizer(str);
		int fileId;
		int idPool = 1;
		while (itr.hasMoreTokens()) {
			String s = itr.nextToken();
			if (s.equals(filename)) {
				fileId = idPool;
			}
			idPool++;
		}

      	String  inputStr   = value.toString();
	    String  patternStr = "[A-Za-z]+";
	    Pattern pattern    = Pattern.compile(patternStr);
	    Matcher matcher    = pattern.matcher(inputStr);;
	    while (matcher.find()) {
	    	Long tmp = key.get() + (long)matcher.start();
	    	ArrayList<Long> arr = new ArrayList<Long>();
	    	arr.add(tmp);
	    	
	    	I2Value  offset    = new I2Value (arr, (Double)0.0);
	    	I2Key wordEntry    = new I2Key(String.valueOf(fileId), matcher.group());
			context.write(wordEntry, offset);
	    	
	    }
	}
}