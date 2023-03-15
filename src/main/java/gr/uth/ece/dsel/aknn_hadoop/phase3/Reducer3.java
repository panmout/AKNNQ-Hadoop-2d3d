package gr.uth.ece.dsel.aknn_hadoop.phase3;

import gr.uth.ece.dsel.aknn_hadoop.util.*;
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
	private BfNeighbors bfn;
	private PsNeighbors psn;
	private PriorityQueue<IdDist> neighbors;
	private ArrayList<Point> qpoints; // list of qpoints in this cell
	private ArrayList<Point> tpoints; // list of tpoints in this cell
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		final String cell = key.toString(); // key is cell_id (mappers' output)
		
		this.qpoints.clear();
		this.tpoints.clear();
						
		for (Text value: values) // run through mappers output
		{
			final String line = value.toString(); // read a line
			final String[] data = line.trim().split("\t");
			
			// if last element is true/false then
			// the line is a query point with coords and 'true-false' flag from mapper3_1
			// data array has these elements: pid, "true" (size = 2)
			// or [pid, xq, yq] + "false" (size = 4) for 2d
			// or [pid, xq, yq, zq] + "false" (size = 5) for 3d
			if (data[data.length - 1].equals("true")) // if flag is 'true' just print point info
			{
				final int outKey = Integer.parseInt(data[0]); // key is point_id

				final String outValue = "true"; // outvalue is 'true'
				
				context.write(new IntWritable(outKey), new Text(outValue));
			}
			else if (data[data.length - 1].equals("false"))
			{
				// flag is 'false', import info to qpoints for processing
				// filling with rest of data[] except last 'false'
				Point qpoint;
				
				if (data.length == 4) // 2d case
					qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				else // 3d case
					qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
				
				this.qpoints.add(qpoint);
			}
			else // the line is a training point from mapper3_2 output, add to tpoints list
			{
				Point tpoint;
				
				if (data.length == 3) // 2d case
					tpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				else // 3d case
					tpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
				
				this.tpoints.add(tpoint);
			}
		}
		
		// set TOTAL_POINTS metrics variable
		context.getCounter(Metrics.TOTAL_TPOINTS).increment(this.tpoints.size());
		
		if (this.mode.equals("bf"))
			this.bfn = new BfNeighbors(this.tpoints, this.K, context);
		else if (this.mode.equals("ps"))
		{
			this.qpoints.sort(new PointXYComparator("min", 'x')); // sort datasets by x ascending
			this.tpoints.sort(new PointXYComparator("min", 'x'));
			this.psn = new PsNeighbors(this.tpoints, this.K, context);
		}
		else
			throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
		
		// find neighbors for each query point
		for (Point qpoint: this.qpoints)
		{
			this.neighbors.clear();
			
			if (this.mode.equals("bf"))
				this.neighbors.addAll(this.bfn.getNeighbors(qpoint));
			else if (this.mode.equals("ps"))
				this.neighbors.addAll(this.psn.getNeighbors(qpoint));
			else
				throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
		
			// write output
			// outKey = qpoint id
			final int outKey = qpoint.getId();
			
			// outValue is {xq, yq, zq, cell, neighbor list, false}
			String outValue;
			
			if (qpoint.getZ() == Double.NEGATIVE_INFINITY) // 2d case
				outValue = String.format("%9.8f\t%9.8f\t%s\t%sfalse", qpoint.getX(), qpoint.getY(), cell, AknnFunctions.pqToString(this.neighbors, this.K));
			else // 3d case
				outValue = String.format("%9.8f\t%9.8f\t%9.8f\t%s\t%sfalse", qpoint.getX(), qpoint.getY(), qpoint.getZ(), cell, AknnFunctions.pqToString(this.neighbors, this.K));
			
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
		
		this.neighbors = new PriorityQueue<>(this.K, new IdDistComparator("min")); // min heap of K neighbors
		
		this.qpoints = new ArrayList<>();
		this.tpoints = new ArrayList<>();
	}
}
