package gr.uth.ece.dsel.aknn_hadoop.phase1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// utility-classes-java imports
import gr.uth.ece.dsel.aknn_hadoop.Metrics;

public final class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum = 0; // sum of training points in this cell
		
		for (IntWritable val: values) // sum all 1's for each cell_id (key)
			sum += val.get();
		
		// increment cells number
		context.getCounter(Metrics.NUM_CELLS).increment(1);
		
		context.write(key, new IntWritable(sum));
	}
}
