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


public class BuildGraph3Reducer extends Reducer<Text, Text, Text, InfoList>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        InfoList infoList = new InfoList();
        for (Text value : values) {
            infoList.addOutLink(value.toString());
        }
        context.write(key, infoList);
        
    }
}

