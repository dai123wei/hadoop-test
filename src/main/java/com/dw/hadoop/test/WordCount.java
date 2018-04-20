package com.dw.hadoop.test;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common_tool.HdfsDAO;  
   
   
public class WordCount {  
   
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {  
        private IntWritable one =new IntWritable(1);  
   
        protected void map(Object key, Text value, Context context)
	            throws IOException, InterruptedException {
        	String line = value.toString();
            String[] strs = line.split(",");
            for(String str : strs) {
            	context.write(new Text(str), one);
            }
	    } 
    }  
   
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {  

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		        throws IOException, InterruptedException {
		
        	int sum = 0;  
            for(IntWritable value: values) {
                sum +=value.get();  
            } 
		    context.write(key, new IntWritable(sum));
		}
 
    }  
   
    public static void main(String[] args)throws Exception {  
    	if(2 != args.length) {
            System.out.println("Usage: WordCount<input path> <output path>");
            System.exit(-1);
        }
    	
        Configuration conf = new Configuration();
        HdfsDAO.rmr(args[1], "hdfs://localhost:9000/", conf);
        
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(WordCount.class);

        //用static来触发
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //用static来触发
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/" + args[1]));

        System.exit(job.waitForCompletion(true)?1:0);
    }  
   
} 
