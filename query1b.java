
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;




public class query1b 
{
	public static class Myclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			
			context.write(new Text(parts[4]),value);
		}
	}
	public static class Myreducer extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Double,Text> tt = new TreeMap<Double,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			//long count = 0;
		long count1 = 0;
			long count2 = 0;
			long count3 = 0;
			long count4 = 0;
			long count5 = 0;
			long count6  = 0;
			String myval = "";
			double growth5 = 0.0;
			double growth1 = 0.0;
			double growth2 = 0.0;
			double growth3 = 0.0;
			double growth4 = 0.0;
			double avggrowth = 0.0;
			for(Text t:values)
			{
				String parts[] = t.toString().split("\t");
				//count++;
				if(parts[7].equals("2011"))
				{
					count1++;
				}
				if(parts[7].equals("2012"))
				{
					count2++;
				}
				if(parts[7].equals("2013"))
				{
					count3++;
				}
				if(parts[7].equals("2014"))
				{
					count4++;
				}
				if(parts[7].equals("2015"))
				{
					count5++;
				}
				if(parts[7].equals("2016"))
				{
					count6++;
				}
			}
			if( count2 > count1)                  //positive growth
			{
				if(count1==0)                      //divide by 0 exception
				{
					growth1 = ((double)count2)*100;
				}
				else
				{
				growth1 = (((double)count2-(double)count1)/(double)count1)*100;
				}
			}
			
			else if(count1!=0 && count1 > count2)          //negative growth
			{
				growth1 = (((double)count2-(double)count1)/(double)count1)*100;
			}
			else
			{
			growth1 = 0;
			}
			
			if(count2 !=0 && count3 > count2)
			{
				growth2 = (((double)count3-(double)count2)/(double)count2)*100;
			}
			else if(count2!=0 && count2 > count3)
			{
				growth2 = (((double)count3-(double)count2)/(double)count2)*100;
			}
			else
			{
			growth2 = 0;
			}
			
			if(count3 !=0 && count4 > count3)
			{
				growth3 = (((double)count4-(double)count3)/(double)count3)*100;
			}
			else if(count3!=0 && count3 > count4)
			{
				growth3 = (((double)count4-(double)count3)/(double)count3)*100;
			}
			else
			{
			growth3 = 0;
			}
			
			if(count4 !=0 && count5 > count4)
			{
				growth4 = (((double)count5-(double)count4)/(double)count4)*100;
			}
			else if(count4!=0 && count4 > count5)
			{
				growth4 = (((double)count5-(double)count4)/(double)count4)*100;
			}
			else
			{
			growth4 = 0;
			}
			
			if(count5!=0 && count6 > count5)
			{
				growth5 = (((double)count6-(double)count5)/(double)count5)*100;
			}
			else if(count5!=0 && count5 > count6)
			{
				growth5 = (((double)count6-(double)count5)*100/(double)count5)*100;
			}
			else
			{
			growth5 = 0;
			}
			avggrowth = (growth1+growth2+growth3+growth4+growth5)/5;
			/*String str1 = String.format("%f", growth1);
			String str2 = String.format("%f", growth2);
			String str3 = String.format("%f", growth3);
			String str4 = String.format("%f", growth4);
			String str5 = String.format("%f", growth5);*/
			myval = key.toString();
		myval = myval+"-"+avggrowth;
			//context.write(key,new Text(myval));
		
			tt.put(avggrowth,new Text(myval) );
			if(tt.size()>10)
			{
				tt.remove(tt.firstKey());
			}
			//myval = count+","+count1+","+count2+","+count3+","+count4+","+count5+","+count6;
			//context.write(key, new Text(myval));
		
		}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(Text t:tt.descendingMap().values())
			{
				context.write(NullWritable.get(), t);
			}
		}
	}
	public static void  main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"new job");
		job.setJarByClass(query1b.class);
	//	job.setPartitionerClass(mypart.class);
		job.setMapperClass(Myclass.class);
		//job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Myreducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	

}

