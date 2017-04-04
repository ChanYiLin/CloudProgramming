//BuildGraph.java

package BuildGraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BuildGraph {
	//static String OUTPUT_DIR = "./output/part-r-00000";

 	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");



        Job job1 = Job.getInstance(conf, "BuildGraph");
        job1.setJarByClass(BuildGraph.class);

        // Input / Mapper

        job1.setMapperClass(BuildGraph1Mapper.class);
        job1.setMapOutputKeyClass(Text.class); 
        job1.setMapOutputValueClass(Text.class);  
        //job1.setNumMapTasks(10);
        // Output / Reducer

        job1.setReducerClass(BuildGraph1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        job1.setInputFormatClass(XmlInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));				
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        

        job1.waitForCompletion(true);

        
        Configuration conf2=new Configuration();
        
        Job job2=Job.getInstance(conf2);
        job2.setJarByClass(BuildGraph.class);

        // set the class of each stage in mapreduce
        job2.setMapperClass(BuildGraph2Mapper.class);
        job2.setReducerClass(BuildGraph2Reducer.class);
   
        
        // 設置 Map 輸出類型  
        job2.setMapOutputKeyClass(Text.class);  
        job2.setMapOutputValueClass(Text.class);  
          
        // 設置 Reduce 輸出類型  
        job2.setOutputKeyClass(Text.class);  
        job2.setOutputValueClass(InfoList.class); 


        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    
      	System.exit(job2.waitForCompletion(true)?0:1);

    }

}

