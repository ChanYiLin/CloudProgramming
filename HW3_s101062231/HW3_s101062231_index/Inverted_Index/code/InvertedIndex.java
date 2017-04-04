package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		//get the FileStatus list from given dir
		/*FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		String fileList = "";
		if (status_list != null) {
			for (FileStatus status : status_list) {
				fileList = fileList + " " + status.getPath().getName();
			}
		}

		conf.set("fileList", fileList);
		*/


		Job job1 = Job.getInstance(conf, "InvertedIndex");
		job1.setJarByClass(InvertedIndex.class);
		
		// set the class of each stage in mapreduce
 		job1.setMapperClass(InvertedIndexMapper.class);
 		job1.setCombinerClass(InvertedIndexCombiner.class);
		//job.setSortComparatorClass(InvertedIndexKeyComparator.class);
		job1.setPartitionerClass(InvertedIndexPartitioner.class);	
		job1.setReducerClass(InvertedIndexReducer.class);
		
		// 設置 Map 輸出類型  
        job1.setMapOutputKeyClass(I2Key.class);  
        job1.setMapOutputValueClass(I2Value.class);  
          
        // 設置 Reduce 輸出類型  
        job1.setOutputKeyClass(I2Key.class);  
        job1.setOutputValueClass(I2Value.class); 

		// set the number of reducer	
		job1.setNumReduceTasks(2);
		// 																												
		// add input/output path
		FileInputFormat.addInputPath(job1, new Path(args[0]));				
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);


		Configuration conf2=new Configuration();

      	Job job2=Job.getInstance(conf2);
      	job2.setJarByClass(InvertedIndex.class);

      	// set the class of each stage in mapreduce
 		job2.setMapperClass(InvertedIndex2Mapper.class);
 		job2.setCombinerClass(InvertedIndex2Combiner.class);
		//job.setSortComparatorClass(InvertedIndexKeyComparator.class);
		job2.setPartitionerClass(InvertedIndex2Partitioner.class);	
		job2.setReducerClass(InvertedIndex2Reducer.class);
   
      	
      	// 設置 Map 輸出類型  
        job2.setMapOutputKeyClass(TableKey.class);  
        job2.setMapOutputValueClass(TableValue.class);  
          
        // 設置 Reduce 輸出類型  
        job2.setOutputKeyClass(TableKey.class);  
        job2.setOutputValueClass(TableValue.class); 


        job2.setNumReduceTasks(2);


      	FileInputFormat.addInputPath(job2, new Path(args[1]));
      	FileOutputFormat.setOutputPath(job2, new Path(args[2]));

      	
      	System.exit(job2.waitForCompletion(true)?0:1);

  }

}

