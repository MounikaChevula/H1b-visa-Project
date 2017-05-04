
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class query2b extends Configured implements Tool{

	public static class myclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			if(parts[1].equals("CERTIFIED"))
			{
				context.write(new Text(parts[8]),value);
			}
		}
	}
	public static class mypart extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int numReduceTasks)
		{
			String parts[] = value.toString().split("\t");
			if(numReduceTasks == 0)
			{
				return 0;
			}
			if(parts[7].equals("2011"))
			{
				return 0;
			}
			if(parts[7].equals("2012"))
			{
				return 1;
			}
			if(parts[7].equals("2013"))
			{
				return 2;
			}
			if(parts[7].equals("2014"))
			{
				return 3;
			}
			if(parts[7].equals("2015"))
			{
				return 4;
			}
			else
				return 5;
		}
	}
	public static class myreducer extends Reducer<Text,Text,NullWritable,Text>
	{

		private TreeMap<Integer,Text>  ftt = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt1 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt2 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt3 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt4 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt5 = new TreeMap<Integer,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count=0;
			String year = "";
			String myval  ="";
			String Case = "";
			for(Text t:values)
			{
				String parts[] = t.toString().split("\t");
				count++;
				year = parts[7];
				Case = parts[1];
				
			}
			myval = key.toString();
			myval = myval+','+Case+','+year+','+count;
			if(year.equals("2011"))
			{
				ftt.put(count, new Text(myval));
				if(ftt.size()>5)
				{
					ftt.remove(ftt.firstKey());
				}
			}
			if(year.equals("2012"))
			{
				ftt1.put(count, new Text(myval));
				if(ftt1.size()>5)
				{
					ftt1.remove(ftt1.firstKey());
				}
			}
			if(year.equals("2013"))
			{
				ftt2.put(count, new Text(myval));
				if(ftt2.size()>5)
				{
					ftt2.remove(ftt2.firstKey());
				}
			}
			if(year.equals("2014"))
			{
				ftt3.put(count, new Text(myval));
				if(ftt3.size()>5)
				{
					ftt3.remove(ftt3.firstKey());
				}
			}
			if(year.equals("2015"))
			{
				ftt4.put(count, new Text(myval));
				if(ftt4.size()>5)
				{
					ftt4.remove(ftt4.firstKey());
				}
			}
			if(year.equals("2016"))
			{
				ftt5.put(count, new Text(myval));
				if(ftt5.size()>5)
				{
					ftt5.remove(ftt5.firstKey());
				}
			}
		}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(Text t:ftt.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:ftt1.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:ftt2.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:ftt3.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:ftt4.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:ftt5.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
		}
	}
	public int run(String []args) throws Exception
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job name");
		job.setJarByClass(query2b.class);
		job.setPartitionerClass(mypart.class);
		job.setMapperClass(myclass.class);
		job.setReducerClass(myreducer.class);
		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
public static void main(String []ar) throws Exception
{
	ToolRunner.run(new Configuration(), new query2b(), ar);
}
}

