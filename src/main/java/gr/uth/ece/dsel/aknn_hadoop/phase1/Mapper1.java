package gr.uth.ece.dsel.aknn_hadoop.phase1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gr.uth.ece.dsel.aknn_hadoop.util.AknnFunctions;
import gr.uth.ece.dsel.aknn_hadoop.util.Node;
import gr.uth.ece.dsel.aknn_hadoop.util.Point;
import gr.uth.ece.dsel.aknn_hadoop.util.ReadHdfsFiles;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String treeDir; // HDFS dir containing tree file
	private String treeFileName; // tree file name in HDFS
	private String treeFile; // full HDFS path to tree file
	private Node root; // create root node
	private int N; // N*N cells
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line
		
		Point p = AknnFunctions.stringToPoint(line, "\t");
		
		String cell = null;
		
		if (this.partitioning.equals("qt")) // quadtree cell
			cell = AknnFunctions.pointToCellQT(p, this.root);
		else if (this.partitioning.equals("gd")) // grid cell
			cell = AknnFunctions.pointToCellGD(p, this.N);	
		
		context.write(new Text(cell), new IntWritable(1));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		this.partitioning = conf.get("partitioning");
		
		if (this.partitioning.equals("qt"))
		{
			this.hostname = conf.get("namenode"); // get namenode name
			this.username = System.getProperty("user.name"); // get user name
			this.treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			this.treeFileName = conf.get("treeFileName"); // get tree filename
			this.treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", this.hostname, this.username, this.treeDir, this.treeFileName); // full HDFS path to tree file
			FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
			
			this.root = ReadHdfsFiles.getTree(this.treeFile, fs);
		}
		else if (this.partitioning.equals("gd"))
			this.N = Integer.parseInt(conf.get("N")); // get N
	}
}
