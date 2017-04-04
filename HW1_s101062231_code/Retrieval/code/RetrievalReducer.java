//InvertedIndex2Reducer
package Retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;


public class RetrievalReducer extends Reducer<TableKey, TableValue, TableKey, Scoreboard>{

    public void reduce(TableKey key, Iterable<TableValue> values, Context context) throws IOException, InterruptedException {		
		ArrayList<WordInfo> fileArray = new ArrayList<WordInfo>();
		ArrayList<TermFileInfo> arr = new ArrayList<TermFileInfo>();

		String fileNum = new String();
		Configuration conf=context.getConfiguration();
		fileNum = conf.get("fileNum");
		int fileNumber =  Integer.parseInt(fileNum);

		for(TableValue value:values){
			fileArray = value.getWordInfos();
			int i = 0;
			int len = fileArray.size();

			for(i = 0;i < len; i++){
				//Double score = 0.0;
				String fileName = fileArray.get(i).getFileName();
				Double df = new Double(value.getDocFreq());
				Double tf = new Double(fileArray.get(i).getTermFreq());
				Double N = new Double(fileNumber);
				Double score = tf*Math.log10(N/df);
				//Double score = new Double(fileArray.get(i).getTermFreq());
				ArrayList<Long> offset = fileArray.get(i).getOffset();
				TermFileInfo tmpTermFileInfo = new TermFileInfo(fileName,score,offset);
				arr.add(tmpTermFileInfo);
			}

		}

		Scoreboard sValue = new Scoreboard(arr);
		
        context.write(key, sValue);  
	}
}