package gr.uth.ece.dsel.aknn_hadoop.phase3;

// utility-classes-java imports
import gr.uth.ece.dsel.common_classes.*;
import gr.uth.ece.dsel.aknn_hadoop.ReadHdfsFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

public final class Mapper3_1 extends Mapper<Object, Text, Text, Text>
{
	private String partitioning;
	private int K;
	private GetOverlapsFunctions ovl;
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();
		final String[] data = line.trim().split("\t");
		
		// read input data: point id, x, y, (z), cell id, neighbor list
		// if there is no z (2d case) then data.length = 4, 6, 8,... else (3d case) data.length = 5, 7, 9,...
		Point qpoint;
		String qcell;
		IdDist neighbor;
		
		final PriorityQueue<IdDist> neighbors = new PriorityQueue<>(this.K, new IdDistComparator("max")); // max heap

		int n = 0; // n = 4 for 2d or 5 for 3d
		
		if (data.length % 2 == 0) // 2d case
		{
			qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
			qcell = data[3];
			n = 4;
		}
		else // 3d case
		{
			qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
			qcell = data[4];
			n = 5;
		}

		if (data.length > n) // creating neighbors from [tpoint_id, dist] from knn list
		{
			for (int j = n; j < data.length; j += 2)
			{
				// 1st pair element is point id, 2nd pair element is distance
				neighbor = new IdDist(Integer.parseInt(data[j]), Double.parseDouble(data[j + 1]));
				neighbors.offer(neighbor);
			}
		}
		
		// get overlapped cells
		HashSet<String> overlaps = new HashSet<>();

		if (this.partitioning.equals("gd"))
			overlaps = this.ovl.getOverlapsGD(qcell, qpoint, neighbors);
		else if (this.partitioning.equals("qt"))
			overlaps = this.ovl.getOverlapsQT(qcell, qpoint, neighbors);

		// write output:
		// overlaps: for each overlapped cell: {cell, qpoint id, x, y, z, 'Q'}
		
		// if there are overlaps
		if (!overlaps.isEmpty())
		{
			for (String cell: overlaps)
			{				
				final String outValue = String.format("%s\tQ", qpoint);

				context.write(new Text(cell), new Text(outValue));
			}
		}
	}
	
	// reading hdfs file contents into hashmap
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration(); // get configuration

		// bf or qt
		this.partitioning = conf.get("partitioning");
		this.K = Integer.parseInt(conf.get("K")); // get K
		// hostname
		final String hostname = conf.get("namenode"); // get namenode name
		// username
		final String username = System.getProperty("user.name"); // get user name
		// mapreduce1 dir name
		final String mr_1_dir = conf.get("mr_1_dir"); // mapreduce1 output dir
		// = "hdfs://HadoopStandalone:9000/user/panagiotis/mapreduce1/part-r-00000"
		final String mr_1_out_full = String.format("hdfs://%s:9000/user/%s/%s", hostname, username, mr_1_dir);

		final FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		// read MR1 output into hashmap
		// hashmap of training points per cell list from MR1 {[cell_id, number of training points]}
		final HashMap<String, Integer> cell_tpoints = new HashMap<>(ReadHdfsFiles.getMR1output(mr_1_out_full, fs));
		
		// initialize GetOverlapsFunctions object
		this.ovl = new GetOverlapsFunctions (this.K, cell_tpoints);
		
		// read qtree or N
		if (this.partitioning.equals("qt"))
		{
			// HDFS dir containing tree file
			final String treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			// tree file name in HDFS
			final String treeFileName = conf.get("treeFileName"); // get tree filename
			// full HDFS path to tree file
			final String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file

			// create root node
			final Node root = ReadHdfsFiles.getTree(treeFile, fs);
			
			this.ovl.setRoot(root);
		}
		else if (this.partitioning.equals("gd"))
		{
			// (2d) N*N or (3d) N*N*N cells
			int n = Integer.parseInt(conf.get("N")); // get N
			
			this.ovl.setN(n);
		}
	}
}
