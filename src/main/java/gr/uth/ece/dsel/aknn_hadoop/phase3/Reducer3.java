package gr.uth.ece.dsel.aknn_hadoop.phase3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import gr.uth.ece.dsel.aknn_hadoop.util.AknnFunctions;
import gr.uth.ece.dsel.aknn_hadoop.util.BfNeighbors;
import gr.uth.ece.dsel.aknn_hadoop.util.IdDist;
import gr.uth.ece.dsel.aknn_hadoop.util.IdDistComparator;
import gr.uth.ece.dsel.aknn_hadoop.util.Metrics;
import gr.uth.ece.dsel.aknn_hadoop.util.Point;
import gr.uth.ece.dsel.aknn_hadoop.util.PointXYComparator;
import gr.uth.ece.dsel.aknn_hadoop.util.PsNeighbors;

public class Reducer3 extends Reducer<Text, Text, IntWritable, Text>
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
		String cell = key.toString(); // key is cell_id (mappers' output)
		
		this.qpoints.clear();
		this.tpoints.clear();
						
		for (Text value: values) // run through mappers output
		{
			String line = value.toString(); // read a line
			String[] data = line.trim().split("\t");
			
			if (data.length == 3) // the line is a training point from mapper3_2 output, add to tpoints list
			{
				Point tpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
				this.tpoints.add(tpoint);
			}
			else // the line is a query point with coords and 'true-false' flag from mapper3_1
			{
				// data array has these elements: pid,"true" (size = 2)
				// or [pid, xi, yi] + "false" (size = 4)
				if (data[data.length - 1].equals("true")) // if flag is 'true' just print point info
				{
					int outKey = Integer.parseInt(data[0]); // key is point_id
					
					String outValue = "true"; // outvalue is 'true'
					
					context.write(new IntWritable(outKey), new Text(outValue));
				}
				else if (data[data.length - 1].equals("false"))
				{
					// flag is 'false', import info to qpoints for processing
					// filling with rest of data[] except last 'false'
					Point qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
					this.qpoints.add(qpoint);
				}
			}
		}
		
		// set TOTAL_POINTS metrics variable
		context.getCounter(Metrics.TOTAL_TPOINTS).increment(this.tpoints.size());
		
		if (this.mode.equals("bf"))
			this.bfn = new BfNeighbors(this.tpoints, this.K, context);
		else if (this.mode.equals("ps"))
		{
			Collections.sort(this.qpoints, new PointXYComparator("min", 'x')); // sort datasets by x ascending
			Collections.sort(this.tpoints, new PointXYComparator("min", 'x'));
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
			int outKey = qpoint.getId();
			// outValue is {xq, yq, cell, neighbor list, false}
			String outValue = String.format("%11.10f\t%11.10f\t%s\t%sfalse", qpoint.getX(), qpoint.getY(), cell, AknnFunctions.pqToString(this.neighbors, this.K));
			
			if (outValue != null)
				context.write(new IntWritable(outKey), new Text(outValue));
		
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		this.K = Integer.parseInt(conf.get("K"));
		
		this.mode = conf.get("mode");
		
		this.neighbors = new PriorityQueue<IdDist>(this.K, new IdDistComparator("min")); // min heap of K neighbors
		
		this.qpoints = new ArrayList<Point>();
		this.tpoints = new ArrayList<Point>();
	}
}
