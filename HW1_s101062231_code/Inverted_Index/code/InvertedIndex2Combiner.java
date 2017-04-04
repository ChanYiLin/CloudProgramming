//InvertedIndex2Cmbiner
package InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndex2Combiner extends Reducer<TableKey, TableValue, TableKey, TableValue> {
	
	public void reduce(TableKey key, Iterable<TableValue> values, Context context)  
        throws IOException, InterruptedException {  

		ArrayList<WordInfo> arr = new ArrayList<WordInfo>();
		for (TableValue value:values) {
			arr.addAll(value.getWordInfos());
		}
		
		TableValue tValue = new TableValue(0, arr);

		context.write(key, tValue);  
		

    }  

}
