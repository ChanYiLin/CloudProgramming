//BuildGraph2Reducer.java

package BuildGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class BuildGraph2Reducer extends Reducer<Text, Text, Text, InfoList>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	InfoList infoList = new InfoList();
		infoList.setTitle(key.toString());
		infoList.setRank(1.0);

        // Construct the node list
		for (Text value : values) 
			infoList.addOutLink(value.toString());
		
		infoList.deleteSelfLink();
		if(infoList.hasSelfLinkToSelf()){
			infoList.change();
		}

		// Output the result
		context.write(key, infoList);
    }

}



