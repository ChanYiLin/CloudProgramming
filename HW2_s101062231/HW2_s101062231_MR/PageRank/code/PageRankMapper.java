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

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
/*
sample input:
---------------------------------------
Page_A	1.0	0.0
Page_B	1.0 0.0	Page_A
Page_C	1.0	0.0	Page_A	Page_D

sample output:
---------------------------------------
Page_A !
Page_A #1.0

Page_B !
Page_A Page_B 1.0 1 //Page_B link to Page_A
Page_B |Page_A
Page_B #1.0

Page_C !
Page_A Page_C 1.0 2
Page_D Page_C 1.0 2
Page_C |Page_A	Page_D
Page_C #1.0

*/
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 		int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);
        int randDiffTabIndex = value.find("\t",rankTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
        String pageWithRankWithRankDiff = Text.decode(value.getBytes(), 0, randDiffTabIndex+1);
        //String pageWithRankDiff = Text.decode(value.getBytes(), 0, randDiffTabIndex+1);
        
        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(page), new Text("!"));//<=============

        // Skip pages with no links.
        String pageRank = new String();
        if(randDiffTabIndex != -1){
            pageRank = Text.decode(value.getBytes(), pageTabIndex+1, rankTabIndex-(pageTabIndex+1));

            String links = Text.decode(value.getBytes(), randDiffTabIndex+1, (value.getLength()-(randDiffTabIndex+1)) );
            String[] allOtherPages = links.split("\t");
            int totalLinks = allOtherPages.length;
            
            for (String otherPage : allOtherPages){
                Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
                context.write(new Text(otherPage), pageRankTotalLinks);   //<=============
            }
            
            // Put the original links of the page for the reduce output
            context.write(new Text(page), new Text("|" + links));      //<=============
            context.write(new Text(page), new Text("#" + pageRank));   //<=============
        }else{
            pageRank = Text.decode(value.getBytes(), pageTabIndex+1, rankTabIndex-(pageTabIndex+1));
            context.write(new Text(page), new Text("#" + pageRank));    //<=============
        }

        
	}
}