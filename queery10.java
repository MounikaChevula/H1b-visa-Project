import java.io.IOException;
//import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class queery10 {
	public static class myclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			context.write((new Text(parts[4])),value);
		}
	}
	public static class myreducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count = 0,count1 = 0,count2 = 0;
			float success = 0.0f;
			String myval = "";
			for(Text t:values)
			{
				String parts[] = t.toString().split("\t");
				count++;
				if(parts[1].equals("CERTIFIED"))
				{
					count1++;
				}
				if(parts[1].equals("CERTIFIED-WITHDRAWN"))
				{
					count2++;
				}
			}
			myval = key.toString();
			
			if(count > 1000)
			{
				success = (count1+count2)*100/count;
				if(success > 70.0)
				{
				myval = myval+','+success;
				}
			context.write(key,new Text(myval));
			}
		}
		
	}
	public static void main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"jj");
		job.setJarByClass(queery10.class);
		job.setMapperClass(myclass.class);
		job.setReducerClass(myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}

