package gr.uth.ece.dsel.aknn_hadoop.phase3;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gr.uth.ece.dsel.aknn_hadoop.util.AknnFunctions;
import gr.uth.ece.dsel.aknn_hadoop.util.GetOverlaps;
import gr.uth.ece.dsel.aknn_hadoop.util.IdDist;
import gr.uth.ece.dsel.aknn_hadoop.util.IdDistComparator;
import gr.uth.ece.dsel.aknn_hadoop.util.Node;
import gr.uth.ece.dsel.aknn_hadoop.util.Point;
import gr.uth.ece.dsel.aknn_hadoop.util.ReadHdfsFiles;

public class Mapper3_1 extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String mr_1_dir; // mapreduce1 dir name
	private String mr_1_out_full; // = "hdfs://HadoopStandalone:9000/user/panagiotis/mapreduce1/part-r-00000"
	private HashMap<String, Integer> cell_tpoints; // hashmap of training points per cell list from MR1 {[cell_id, number of training points]}
	private String treeDir; // HDFS dir containing tree file
	private String treeFileName; // tree file name in HDFS
	private String treeFile; // full HDFS path to tree file
	private Node root; // create root node
	private int N; // (2d) N*N or (3d) N*N*N cells
	private int K;
	private HashSet<String> overlaps;
	private GetOverlaps ovl;
	private PriorityQueue<IdDist> neighbors;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] data = line.trim().split("\t");
		
		// read input data: point id, x, y, z, cell id, neighbor list
		// if there is no z (2d case) then data.length = 4, 6, 8,... else (3d case) data.length = 3, 5, 7,...
		Point qpoint = null;
		String qcell = null;
		
		this.neighbors.clear();
		
		if (data.length % 2 == 0) // 2d case
		{
			qpoint = new Point(Integer.parseInt(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
			qcell = data[3];
			
			if (data.length > 4) // creating neighbors from [tpoint_id, dist] from knn list
			{
				for (int j = 4; j < data.length; j += 2)
				{
					// 1st pair element is point id, 2nd pair element is distance
					IdDist neighbor = new IdDist(Integer.parseInt(data[j]), Double.parseDouble(data[j + 1]));
					this.neighbors.offer(neighbor);
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
					IdDist neighbor = new IdDist(Integer.parseInt(data[j]), Double.parseDouble(data[j + 1]));
					this.neighbors.offer(neighbor);
				}
			}
		}
		
		// get overlapped cells
		this.ovl.initializeFields(qpoint, qcell, this.neighbors);
		
		this.overlaps = new HashSet<String>(this.ovl.getOverlaps());
		
		// write output:
		// no overlaps: {query cell, qpoint id, true}
		// overlaps: for each overlapped cell: {cell, qpoint id, x, y, z, false}
		
		if (this.overlaps.size() == 1 && this.overlaps.contains(qcell))	// only cell in overlaps is query cell, so there are no overlaps
			context.write(new Text(qcell), new Text(String.format("%d\ttrue", qpoint.getId())));
		else // there are overlaps
		{
			for (String cell: this.overlaps)
			{
				String outValue = "";
				
				if (qpoint.getZ() == Double.NEGATIVE_INFINITY) // 2d case
					outValue = String.format("%d\t%11.10f\t%11.10f\tfalse", qpoint.getId(), qpoint.getX(), qpoint.getY());
				else // 3d case
					outValue = String.format("%d\t%11.10f\t%11.10f\t%11.10f\tfalse", qpoint.getId(), qpoint.getX(), qpoint.getY(), qpoint.getZ());
				
				context.write(new Text(cell), new Text(outValue));
			}
		}
	}
	
	// reading hdfs file contents into hashmap
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration(); // get configuration
		
		this.partitioning = conf.get("partitioning");
		this.K = Integer.parseInt(conf.get("K")); // get K
		this.hostname = conf.get("namenode"); // get namenode name
		this.username = System.getProperty("user.name"); // get user name	
		this.mr_1_dir = conf.get("mr_1_dir"); // mapreduce1 output dir
		this.mr_1_out_full = String.format("hdfs://%s:9000/user/%s/%s", this.hostname, this.username, this.mr_1_dir);
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		// read MR1 output into hashmap
		this.cell_tpoints = new HashMap<String, Integer>(ReadHdfsFiles.getMR1output(this.mr_1_out_full, fs));
		
		// initialize overlaps object
		this.ovl = new GetOverlaps(this.cell_tpoints, this.K, this.partitioning);
		
		this.neighbors = new PriorityQueue<IdDist>(this.K, new IdDistComparator("max")); // max heap
		
		// read qtree or N
		if (this.partitioning.equals("qt"))
		{
			this.treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			this.treeFileName = conf.get("treeFileName"); // get tree filename
			this.treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", this.hostname, this.username, this.treeDir, this.treeFileName); // full HDFS path to tree file
			
			this.root = ReadHdfsFiles.getTree(this.treeFile, fs);
			
			this.ovl.setRoot(this.root);
		}
		else if (this.partitioning.equals("gd"))
		{
			this.N = Integer.parseInt(conf.get("N")); // get N
			this.ovl.setN(this.N);
		}
	}
}
