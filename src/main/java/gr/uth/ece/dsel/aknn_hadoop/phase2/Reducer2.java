package gr.uth.ece.dsel.aknn_hadoop.phase2;

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

public class Reducer2 extends Reducer<Text, Text, IntWritable, Text>
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
						
		for (Text value: values) // run through value of mappers output
		{
			String line = value.toString(); // read a line
			String[] data = line.trim().split("\t");
			
			Point p = null;
			
			if (data.length == 4) // 2d case (id, x, y, Q/T)
				p = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
			else if (data.length == 5) // 3d case (x, y, z, Q/T)
				p = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
			else
				throw new IllegalArgumentException();
			
			char type = line.trim().charAt(line.length() - 1); // get last "Q" or "T"
			
			switch(type)
			{
				case 'Q':
					this.qpoints.add(p); // add point to query list
					break;
				case 'T':
					this.tpoints.add(p); // add point to training list
					break;
			}
		}
		
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
			// outValue is {xq, yq, zq, cell id, neighbor list}
			String outValue = "";
			
			if (qpoint.getZ() == Double.NEGATIVE_INFINITY) // 2d case
				outValue = String.format("%11.10f\t%11.10f\t%s\t%s", qpoint.getX(), qpoint.getY(), cell, AknnFunctions.pqToString(this.neighbors, this.K));
			else // 3d case
				outValue = String.format("%11.10f\t%11.10f\t%11.10f\t%s\t%s", qpoint.getX(), qpoint.getY(), qpoint.getZ(), AknnFunctions.pqToString(this.neighbors, this.K));
			
			context.write(new IntWritable(outKey), new Text(outValue));
		}
		
		// set TOTAL_POINTS metrics variable
		context.getCounter(Metrics.TOTAL_TPOINTS).increment(this.tpoints.size());
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
