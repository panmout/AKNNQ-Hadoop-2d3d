package gr.uth.ece.dsel.aknn_hadoop.phase3;

// utility-classes-java imports
import gr.uth.ece.dsel.common_classes.*;
import gr.uth.ece.dsel.aknn_hadoop.GetOverlaps;
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
	private GetOverlaps ovl;
	private int K;
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();
		final String[] data = line.trim().split("\t");
		
		// read input data: point id, x, y, z, cell id, neighbor list
		// if there is no z (2d case) then data.length = 4, 6, 8,... else (3d case) data.length = 3, 5, 7,...
		Point qpoint;
		String qcell;
		IdDist neighbor;
		
		final PriorityQueue<IdDist> neighbors = new PriorityQueue<>(this.K, new IdDistComparator("max")); // max heap
		
		if (data.length % 2 == 0) // 2d case
		{
			qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
			qcell = data[3];
			
			if (data.length > 4) // creating neighbors from [tpoint_id, dist] from knn list
			{
				for (int j = 4; j < data.length; j += 2)
				{
					// 1st pair element is point id, 2nd pair element is distance
					neighbor = new IdDist(Integer.parseInt(data[j]), Double.parseDouble(data[j + 1]));
					neighbors.offer(neighbor);
				}
			}
		}
		else // 3d case
		{
			qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]), Double.parseDouble(data[3]));
			qcell = data[4];
			
			if (data.length > 5) // creating neighbors from [tpoint_id, dist] from knn list
			{
				for (int j = 5; j < data.length; j += 2)
				{
					// 1st pair element is point id, 2nd pair element is distance
					neighbor = new IdDist(Integer.parseInt(data[j]), Double.parseDouble(data[j + 1]));
					neighbors.offer(neighbor);
				}
			}
		}
		
		// get overlapped cells
		this.ovl.initializeFields(qpoint, qcell, neighbors);

		final HashSet<String> overlaps = this.ovl.getOverlaps();
		
		// write output:
		// no overlaps: {query cell, qpoint id, true}
		// overlaps: for each overlapped cell: {cell, qpoint id, x, y, z, false}
		
		if (overlaps.size() == 1 && overlaps.contains(qcell))	// only cell in overlaps is query cell, so there are no overlaps
			context.write(new Text(qcell), new Text(String.format("%d\ttrue", qpoint.getId())));
		else // there are overlaps
		{
			for (String cell: overlaps)
			{				
				final String outValue = String.format("%s\tfalse", qpoint);

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
		final String partitioning = conf.get("partitioning");
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
		
		// initialize overlaps object
		this.ovl = new GetOverlaps(cell_tpoints, this.K, partitioning);
		
		// read qtree or N
		if (partitioning.equals("qt"))
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
		else if (partitioning.equals("gd"))
		{
			// (2d) N*N or (3d) N*N*N cells
			int n = Integer.parseInt(conf.get("N")); // get N
			this.ovl.setN(n);
		}
	}
}
