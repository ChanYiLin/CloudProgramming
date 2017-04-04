package InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedIndexReducer extends Reducer<I2Key, I2Value, I2Key, I2Value>{

    public void reduce(I2Key key, Iterable<I2Value> values, Context context) throws IOException, InterruptedException {		
		ArrayList<Long> arr = new ArrayList<Long>();
		for (I2Value value:values) {
			arr.addAll(value.getOffset());
		}
		Collections.sort(arr);
		Double	 offsetLength   = new Double(arr.size());
		I2Value offset 	    = new I2Value(arr, offsetLength);
        context.write(key, offset);  
	}
}
