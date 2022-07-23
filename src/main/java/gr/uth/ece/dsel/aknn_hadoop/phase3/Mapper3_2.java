package gr.uth.ece.dsel.aknn_hadoop.phase3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gr.uth.ece.dsel.aknn_hadoop.util.AknnFunctions;
import gr.uth.ece.dsel.aknn_hadoop.util.Node;
import gr.uth.ece.dsel.aknn_hadoop.util.Point;
import gr.uth.ece.dsel.aknn_hadoop.util.ReadHdfsFiles;

public class Mapper3_2 extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String treeDir; // HDFS dir containing tree file
	private String treeFileName; // tree file name in HDFS
	private String treeFile; // full HDFS path to tree file
	private Node root; // create root node
	private int N; // (2d) N*N or (3d) N*N*N cells
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line
		
		Point p = AknnFunctions.stringToPoint(line, "\t");
		
		String cell = null;
		
		if (partitioning.equals("qt")) // quadtree cell
			cell = AknnFunctions.pointToCellQT(p, root);
		else if (partitioning.equals("gd")) // grid cell
			cell = AknnFunctions.pointToCellGD(p, N);
		
		String outValue = "";
		
		if (p.getZ() == Double.NEGATIVE_INFINITY) // 2d case
			outValue = String.format("%d\t%11.10f\t%11.10f", p.getId(), p.getX(), p.getY());
		else
			outValue = String.format("%d\t%11.10f\t%11.10f\t%11.10f", p.getId(), p.getX(), p.getY(), p.getZ());
		
		context.write(new Text(cell), new Text(outValue));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		partitioning = conf.get("partitioning");
		
		hostname = conf.get("namenode"); // get namenode name
		username = System.getProperty("user.name"); // get user name
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		if (partitioning.equals("qt"))
		{
			treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			treeFileName = conf.get("treeFileName"); // get tree filename
			treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file
			
			root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (partitioning.equals("gd"))
			N = Integer.parseInt(conf.get("N")); // get N
	}
}
