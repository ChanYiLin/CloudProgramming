//PageRankMapper.java
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


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank1Mapper extends Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t");
        double rank = 0.0;
        String title = new String();
        int flag = 0;
        if(tokenizer.hasMoreTokens())
            title = tokenizer.nextToken();
        if(tokenizer.hasMoreTokens())
            rank = Double.parseDouble(tokenizer.nextToken());
        while(tokenizer.hasMoreTokens()){
            flag = 1;
            context.write(new Text(title), new Text(tokenizer.nextToken()));
        }
        if(flag == 0){
            context.write(new Text(title), new Text(""));
        }




        /*InfoList infoList = new InfoList();
        
        while(tokenizer.hasMoreTokens()){
            String title = tokenizer.nextToken();
            infoList.setTitle(title);
            if(tokenizer.hasMoreTokens()){
                
                infoList.setRank(rank);
            }
            while(tokenizer.hasMoreTokens()){
                infoList.addOutLink(tokenizer.nextToken());
            }
            infoList.addOutLink(title);
        }
        //infoList.setRankDiff(0.0);
        //infoList.addOutLink(title);

        Text title = new Text(infoList.getTitle());
        //context.write(title,infoList);
        for (String link : infoList.getOutlinks())
            context.write(title, new Text(link));
        */
        
    }
}