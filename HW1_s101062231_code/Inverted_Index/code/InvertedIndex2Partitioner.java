//InvertedIndex2Partitioner
package InvertedIndex;

import java.lang.Character;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedIndex2Partitioner extends Partitioner<TableKey, TableValue> {

	@Override
	public int getPartition(TableKey key, TableValue value, int numPartitions) {
		
		if(key.getWord().substring(0,1).matches("^[A-Ga-g]")){
			return 0;
		}else if(key.getWord().substring(0,1).matches("^[H-Zh-z]")){
			return 1;
		}
		return 0;
	}
}
