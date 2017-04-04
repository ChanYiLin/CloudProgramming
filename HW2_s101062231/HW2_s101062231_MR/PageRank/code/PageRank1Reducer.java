//PageRankReducer.java
package PageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class PageRank1Reducer extends Reducer<Text, Text, Text, InfoList>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //get the number of title (N). 
        String titleNum     = new String();
        Configuration conf  = context.getConfiguration();
        titleNum            = conf.get("titleNum");
        int titleNumber     =  Integer.parseInt(titleNum);

        Set<String> outLinks = new HashSet<String>();
        InfoList infoValue  = new InfoList();

        /*for(Text value: values){
            String title = value.getTitle();
            infoValue.setTitle(title);
            infoValue.setRank(1.0/titleNumber);
            infoValue.setRankDiff(0.0);
            outLinks = value.getOutlinks();
            infoValue.addOutLink(outLinks);
        }*/

        InfoList infoList   = new InfoList();
        infoList.setTitle(key.toString());
        infoList.setRank(1.0/titleNumber);
        infoList.setRankDiff(0.0);

        // Construct the node list
        for (Text value : values) 
            infoList.addOutLink(value.toString());
        //infoList.deleteSelfLink();

        // Output the result
        context.write(key, infoList);
    }

}