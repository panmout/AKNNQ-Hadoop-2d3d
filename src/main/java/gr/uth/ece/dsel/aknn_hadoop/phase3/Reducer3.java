package gr.uth.ece.dsel.aknn_hadoop.phase3;

// utility-classes-java imports
import gr.uth.ece.dsel.common_classes.*;
import gr.uth.ece.dsel.aknn_hadoop.FindNeighbors;
import gr.uth.ece.dsel.aknn_hadoop.Metrics;
import gr.uth.ece.dsel.UtilityFunctions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public final class Reducer3 extends Reducer<Text, Text, IntWritable, Text>
{
	private int K; // user defined (k-nn)
	private String mode; // bf or ps
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		final String cell = key.toString(); // key is cell_id (mappers' output)
		
		final ArrayList<Point> qpoints = new ArrayList<>(); // list of qpoints in this cell
		final ArrayList<Point> tpoints = new ArrayList<>(); // list of tpoints in this cell
		
		for (Text value: values) // run through mappers output
		{
			final String line = value.toString(); // read a line
			final String[] data = line.trim().split("\t");
			
			// if last element is true/false then
			// the line is a query point with coords and 'true-false' flag from mapper3_1
			// data array has these elements: pid, "true" (size = 2)
			// or [pid, xq, yq] + "false" (size = 4) for 2d
			// or [pid, xq, yq, zq] + "false" (size = 5) for 3d
			if (data[data.length - 1].equals("true")) // if flag is 'true' do nothing
				continue;
			else if (data[data.length - 1].equals("false"))
			{
				// flag is 'false', import info to qpoints for processing
				// filling with rest of data[] except last 'false'
				Point qpoint;
				
				if (data.length == 4) // 2d case
					qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				else // 3d case
					qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
				
				qpoints.add(qpoint);
			}
			else // the line is a training point from mapper3_2 output, add to tpoints list
			{
				Point tpoint;
				
				if (data.length == 3) // 2d case
					tpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				else // 3d case
					tpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
				
				tpoints.add(tpoint);
			}
		}
		
		// set TOTAL_POINTS metrics variable
		context.getCounter(Metrics.TOTAL_TPOINTS).increment(tpoints.size());
		
		if (this.mode.equals("ps"))
		{
			qpoints.sort(new PointXYComparator("min", 'x')); // sort datasets by x ascending
			tpoints.sort(new PointXYComparator("min", 'x'));
		}
		
		PriorityQueue<IdDist> neighbors; // min heap of K neighbors

		final FindNeighbors fn = new FindNeighbors(tpoints, this.K, context);

		// find neighbors for each query point
		for (Point qpoint: qpoints)
		{
			if (this.mode.equals("bf"))
				neighbors = fn.getBfNeighbors(qpoint);
			else if (this.mode.equals("ps"))
				neighbors = fn.getPsNeighbors(qpoint);
			else
				throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
		
			// write output
			// outKey = qpoint id
			final int outKey = qpoint.getId();
			
			// outValue is {xq, yq, zq, cell, neighbor list}
			final String outValue = String.format("%s\t%s\t%s", qpoint.stringCoords(), cell, UtilityFunctions.pqToString(neighbors, this.K, "min"));

			if (outValue != null)
				context.write(new IntWritable(outKey), new Text(outValue));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration();
		
		this.K = Integer.parseInt(conf.get("K"));
		
		this.mode = conf.get("mode");
	}
}
