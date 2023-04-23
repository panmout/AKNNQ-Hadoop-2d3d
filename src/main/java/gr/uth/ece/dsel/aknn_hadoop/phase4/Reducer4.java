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
		final PriorityQueue<IdDist> neighbors = new PriorityQueue<>(this.K, new IdDistComparator("max")); // max heap of K neighbors;
		
		for (Text value: values) // run through values (knnlist) of mapper output = {id1, dist1, id2, dist2,...}
		{
			final String line = value.toString();
			final String[] data = line.trim().split("\t");
			
			for (int i = 0; i < data.length - 1; i += 2) // fill neighbors list
			{
				final int tid = Integer.parseInt(data[i]); // first element of couple is point id
				final double dist = Double.parseDouble(data[i+1]); // second element of couple is distance
				final IdDist neighbor = new IdDist(tid, dist);

				// if PriorityQueue not full, add new tpoint (IdDist)
				if (neighbors.size() < K)
				{
					if (!UtilityFunctions.isDuplicate(neighbors, neighbor))
						neighbors.offer(neighbor); // insert to queue
				}
				else // if queue is full, run some checks and replace elements
				{
					final double dm = neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
					
					if (dist < dm) // compare distance
					{  					
						if (!UtilityFunctions.isDuplicate(neighbors, neighbor))
						{
							neighbors.poll(); // remove top element
							neighbors.offer(neighbor); // insert to queue
						}
					} // end if
				} // end else
			}
		}

		System.out.println("qpoint: " + key + ", neighbors size: " + neighbors.size());
		
		context.write(key, new Text(UtilityFunctions.pqToString(neighbors, this.K, "min")));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration();
		this.K = Integer.parseInt(conf.get("K"));
	}
}