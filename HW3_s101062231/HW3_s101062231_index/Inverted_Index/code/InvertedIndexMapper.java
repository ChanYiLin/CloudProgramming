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

	final static private Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");

    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		int searchStart = 0, searchEnd = 0, contentStart = 0, contentEnd = -1;

		searchStart = line.indexOf("<text", searchStart);
		searchStart = line.indexOf(">", searchStart) + 1;
		searchEnd = line.indexOf("</text>", searchStart);
		

      	Matcher titleMatcher = titlePattern.matcher(line);
        if ( titleMatcher.find() ){
            String title = replaceSpecialString(titleMatcher.group(1));
            title = title.replaceAll("<title>|</title>", "");
            title = capitalizeFirstLetter(title);   //find the title

            //find the term in text
            if(searchStart < searchEnd){
            	String textContent = line.substring(searchStart,searchEnd);
                textContent = replaceSpecialString(textContent);
				String  patternStr = "[A-Za-z]+";
		    	Pattern pattern    = Pattern.compile(patternStr);
		    	Matcher matcher    = pattern.matcher(textContent);
				while (matcher.find()) {
			    	Long tmp = key.get() + (long)matcher.start();
			    	ArrayList<Long> arr = new ArrayList<Long>();
			    	arr.add(tmp);
			    	
			    	I2Value  offset    = new I2Value (arr, (Double)0.0);
			    	//I2Key wordEntry    = new I2Key(String.valueOf(fileId), matcher.group());
			    	I2Key wordEntry    = new I2Key(title, matcher.group());
					context.write(wordEntry, offset);
			    	
			    }
            }
			
        }
        else{
            throw new IOException("MYERROR: input doesn't have a title");
        }
      	
	}
	private String replaceSpecialString(String input){
        return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
    }

    private String capitalizeFirstLetter(String input){
        char firstChar = input.charAt(0);
        if ( (firstChar >= 'a' && firstChar <='z') || (firstChar>= 'A' && firstChar <= 'Z') ){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else{
                return input.substring(0, 1).toUpperCase() + input.substring(1);
            }
        }
        else{
            return input;
        }
    }
   
}
