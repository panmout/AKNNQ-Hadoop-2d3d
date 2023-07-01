package gr.uth.ece.dsel.aknn_hadoop.phase1;

// utility-classes-java imports
import gr.uth.ece.dsel.common_classes.*;
import gr.uth.ece.dsel.aknn_hadoop.ReadHdfsFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class Mapper1 extends Mapper<Object, Text, Text, IntWritable>
{
	private String partitioning; // gd or qt
	private Node root; // create root node
	private int N; // (2d) N*N or (3d) N*N*N cells

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		final String line = value.toString(); // read a line

		final Point p = UtilityFunctions.stringToPoint(line, "\t");

		String cell = null;

		if (this.partitioning.equals("qt")) // quadtree cell
			cell = UtilityFunctions.pointToCell(p, this.root);
		else if (this.partitioning.equals("gd")) // grid cell
			cell = UtilityFunctions.pointToCell(p, this.N);
		
		context.write(new Text(cell), new IntWritable(1));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration();
		
		this.partitioning = conf.get("partitioning");
		
		if (this.partitioning.equals("qt"))
		{
			// hostname
			final String hostname = conf.get("namenode"); // get namenode name
			// username
			final String username = System.getProperty("user.name"); // get user name
			// HDFS dir containing tree file
			final String treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			// tree file name in HDFS
			final String treeFileName = conf.get("treeFileName"); // get tree filename
			// full HDFS path to tree file
			final String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file
			final FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
			
			this.root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (this.partitioning.equals("gd"))
			this.N = Integer.parseInt(conf.get("N")); // get N
	}
}
