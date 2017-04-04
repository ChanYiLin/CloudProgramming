package InvertedIndex;

import java.lang.Character;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedIndexPartitioner extends Partitioner<I2Key, I2Value> {

	// TODO modify this function to assign the words start with Aa ~ Gg 
	//      to first reducer, and the remaining words to second reducer.
	@Override
	public int getPartition(I2Key key, I2Value value, int numPartitions) {
		
		if(key.getWord().substring(0,1).matches("^[A-Ga-g]")){
			return 0;
		}else if(key.getWord().substring(0,1).matches("^[H-Zh-z]")){
			return 1;
		}
		return 0;
	}
}
