package gr.uth.ece.dsel.aknn_hadoop.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver3 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver3(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 12)
		{
			System.err.println("Usage: Driver3 <mapreduce2 output path> <training dataset> <output path> <namenode name> <treeDir> <treeFileName> <N> <K> <MapReduce1 output dir> <partitioning> <mode> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args
		conf.set("namenode", args[3]);
		conf.set("treeDir", args[4]);
		conf.set("treeFileName", args[5]);
		conf.set("N", args[6]);
		conf.set("K", args[7]);
		conf.set("mr_1_dir", args[8]);
		conf.set("partitioning", args[9]);
		conf.set("mode", args[10]);
		// compress map output
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		
		// Create job
		Job job = Job.getInstance(conf, "MapReduce3");
		job.setJarByClass(Driver3.class);
		
		// Setup MapReduce job
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper3_1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper3_2.class);
		job.setReducerClass(Reducer3.class);
		job.setNumReduceTasks(Integer.parseInt(args[11]));
		
		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Input / Output
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}