//PageRankReducer.java
package PageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
/*
sample input:
---------------------------------------
Page_A !
Page_A #1.0
Page_A Page_B 1.0 1 //Page_B link to Page_A
Page_A Page_C 1.0 2

Page_B !
Page_B #1.0
Page_B |Page_A


Page_C !
Page_C #1.0
Page_C |Page_A

Page_D Page_C 1.0 2

sample output:
----------------------------------------
Page_A	1.425 	X.X
Page_B	0.15	X.X	Page_A
Page_C	0.15 	X.X	Page_A,Page_D




*/



    private static final double damping = 0.85;
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {

 		boolean isExistingWikiPage = false;
        String[] split;
        double sumShareOtherPageRanks = 0;
        String links = "";
        String originPageRankString = "";
        Double originPageRank = 0.0;
        String pageWithRank;
        
    
        for (Text value : values){
            pageWithRank = value.toString();
            
            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
            
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            if(pageWithRank.startsWith("#")){
            	originPageRankString = pageWithRank.substring(1);
				originPageRank = Double.valueOf(originPageRankString);
				continue; 
            }

            split = pageWithRank.split("\\t");
            
            double pageRank = Double.valueOf(split[1]);
            double countOutLinks = Double.valueOf(split[2]);
            
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isExistingWikiPage) return;

        String titleNum     = new String();
        Configuration conf  = context.getConfiguration();
        titleNum            = conf.get("titleNum");
        double titleNumber     =  Double.parseDouble(titleNum);
		double danglingRankSum = Double.parseDouble(conf.get("danglingRankSum"));


        double newRank = ((1-damping)/titleNumber) + (damping * sumShareOtherPageRanks) + (damping * (danglingRankSum/titleNumber));
        double newRankDiff = Math.abs(newRank - originPageRank);
        context.write(page, new Text(newRank + "\t" + newRankDiff + links));
    	
    }

}



