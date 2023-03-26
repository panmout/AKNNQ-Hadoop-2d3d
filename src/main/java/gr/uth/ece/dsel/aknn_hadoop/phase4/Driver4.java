package gr.uth.ece.dsel.aknn_hadoop.phase4;

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

public class Driver4 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver4(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 5)
		{
			System.err.println("Usage: Driver4 <mapreduce3 output> <mapreduce2 output> <output path> <K> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args (K)
		conf.set("K", args[3]);
		// compress map output
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		 
		// Create job
		Job job = Job.getInstance(conf, "MapReduce4");
		job.setJarByClass(Driver4.class);
 
		// Setup MapReduce job
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper4.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper4.class);
		job.setReducerClass(Reducer4.class);
		job.setNumReduceTasks(Integer.parseInt(args[4]));
 
		// Specify key / value
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
 
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}