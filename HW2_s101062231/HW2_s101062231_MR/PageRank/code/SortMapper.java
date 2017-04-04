//SortMapper.java
package PageRank;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SortMapper extends Mapper<LongWritable, Text, PageRankResult, NullWritable> {
/*
sample output:
----------------------------------------
Page_A	1.425 	X.X
Page_B	0.15	X.X	Page_A
Page_C	0.15 	X.X	Page_A,Page_D
*/
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t");
		String title = new String();
    	String rank = new String();
		if(tokenizer.hasMoreTokens())
            title = tokenizer.nextToken();
        if(tokenizer.hasMoreTokens())
           	rank = tokenizer.nextToken();
        context.write(new PageRankResult(title, Double.parseDouble(rank)), NullWritable.get());
	}
}