package gr.uth.ece.dsel.aknn_hadoop.phase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver1 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver1(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 8)
		{
			System.err.println("Usage: Driver1 <training dataset path> <output path> <namenode name> <treeDir> <treeFileName> <N> <partitioning> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args
		conf.set("namenode", args[2]);
		conf.set("treeDir", args[3]);
		conf.set("treeFileName", args[4]);
		conf.set("N", args[5]);
		conf.set("partitioning", args[6]);
		 
		// Create job
		Job job = Job.getInstance(conf, "MapReduce1");
		job.setJarByClass(this.getClass());
 
		// Setup MapReduce job
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setNumReduceTasks(Integer.parseInt(args[7]));
 
		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
 
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
 
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
