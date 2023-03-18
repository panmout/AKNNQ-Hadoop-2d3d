package gr.uth.ece.dsel.aknn_hadoop.phase4;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import gr.uth.ece.dsel.UtilityFunctions;
import gr.uth.ece.dsel.common_classes.*;

public final class Reducer4 extends Reducer<IntWritable, Text, IntWritable, Text>
{
	private int K; // user defined (k-nn)
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		final PriorityQueue<IdDist> neighbors = new PriorityQueue<>(this.K, new IdDistComparator("min")); // neighbors queue by distance ascending
				
		int trueCounter = 0; // +1 for "true"
		
		for (Text value: values) // run through values (knnlist) of mapper output = {id1, dist1, id2, dist2,...}
		{
			final String line = value.toString();
			final String[] data = line.trim().split("\t");
			
			if (data.length > 1) // if knnlist not empty (not MR2 output only 'false' or MR3 only 'true')
				for (int i = 0; i < data.length - 1; i += 2) // fill neighbors list
				{
					int tid = Integer.parseInt(data[i]); // first element of couple is point id
					double dist = Double.parseDouble(data[i+1]); // second element of couple is distance
					IdDist neighbor = new IdDist(tid, dist);
					if (!UtilityFunctions.isDuplicate(neighbors, neighbor))
			    		neighbors.offer(neighbor); // insert to queue
				}
			
			if (data[data.length - 1].equals("true")) // if found "true"
				trueCounter++; // increase counter
		}

		final StringBuilder outValue = new StringBuilder();
		
		if (trueCounter > 0) // found at least one "true", just print point info
		{
			while (neighbors.size() > 0) // reading neighbors list
			{
				IdDist neighbor = neighbors.poll(); // {tpoint_id, distance}
				outValue.append(String.format("%s\t", neighbor));
			}
			
			context.write(key, new Text(outValue.toString()));
		}
		else // no "true" found, read first K neighbors
		{	
			for (int i = 0; i < this.K; i++) // reading neighbors list
			{
				final IdDist neighbor = neighbors.poll(); // neighbor array
				
				outValue.append(String.format("%s\t", neighbor));
			}
			context.write(key, new Text(outValue.toString()));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration();
		this.K = Integer.parseInt(conf.get("K"));
	}
}