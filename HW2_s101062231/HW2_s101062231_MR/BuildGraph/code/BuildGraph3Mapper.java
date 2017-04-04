//BuildGraph23Mapper

package BuildGraph;

import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

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

public class BuildGraph3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t");

        double rank = 0.0;
    	while(tokenizer.hasMoreTokens()){
    		String title = tokenizer.nextToken();
            context.write(new Text(title),new Text(title)); //self Link

            if(tokenizer.hasMoreTokens()){
                rank = Double.parseDouble(tokenizer.nextToken());
            }
    		while(tokenizer.hasMoreTokens()){
                context.write(new Text(tokenizer.nextToken()),new Text(title));
    			//infoList.addOutLink(tokenizer.nextToken());
    		}
    	}

    	/*if (infoList.hasSelfLink()) {
			Text title = new Text(infoList.getTitle());
			for (String link : infoList.getOutlinks())
				context.write(new Text(link), title);
		}*/

	}
}