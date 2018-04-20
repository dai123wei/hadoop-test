package com.dw.hadoop.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common_tool.HdfsDAO;


public class MyJob{

	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	    private Integer ERROR_TEMPER = 9999;

	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	        String content = value.toString();
	        String year = content.substring(15, 19);    //获取year

	        Integer temperature  = null;    //获取温度
	        if('+' == content.charAt(45)) {
	            temperature = Integer.parseInt(content.substring(46, 50));
	        } else {
	            temperature = Integer.parseInt(content.substring(45, 50));
	        }

	        if(temperature <= ERROR_TEMPER && content.substring(50, 51).matches("[01459]")) {
	            context.write(new Text(year), new IntWritable(temperature));
	        }
	    }

	}
	
	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		        throws IOException, InterruptedException {
		
		    int maxTemperature = Integer.MIN_VALUE;
		
		    for(IntWritable intWritable : values) {
		        maxTemperature = Math.max(maxTemperature, intWritable.get());
		    }
		
		    context.write(key, new IntWritable(maxTemperature));
		
		}
	}
	
	public static void main(String[] args) throws Exception {

		if(2 != args.length) {
            System.out.println("Usage: MaxTemperature<input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        HdfsDAO.rmr(args[1], "hdfs://localhost:9000/", conf);
        
        Job job = new Job(conf, "temperature");
        job.setJarByClass(MyJob.class);

        //用static来触发
        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //用static来触发
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/" + args[1]));

        System.exit(job.waitForCompletion(true)?1:0);
    }
}

