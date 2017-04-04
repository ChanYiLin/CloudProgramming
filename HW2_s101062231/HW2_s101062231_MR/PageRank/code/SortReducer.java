//SortReducer.java
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
import org.apache.hadoop.io.NullWritable;


public class SortReducer extends Reducer<PageRankResult, NullWritable, PageRankResult, NullWritable>{

   public void reduce(PageRankResult pageRank, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {    	
 
   		context.write(pageRank, NullWritable.get());
   }

}



