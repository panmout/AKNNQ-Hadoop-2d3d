package gr.uth.ece.dsel.aknn_hadoop.phase2;

import gr.uth.ece.dsel.aknn_hadoop.util.AknnFunctions;
import gr.uth.ece.dsel.aknn_hadoop.util.Node;
import gr.uth.ece.dsel.aknn_hadoop.util.Point;
import gr.uth.ece.dsel.aknn_hadoop.util.ReadHdfsFiles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class Mapper2_Training extends Mapper<Object, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private Node root; // create root node
	private int N; // (2d) N*N or (3d) N*N*N cells
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		final String line = value.toString(); // read a line

		final Point p = AknnFunctions.stringToPoint(line, "\t");
		
		String cell = null;
		
		if (this.partitioning.equals("qt")) // quadtree cell
			cell = AknnFunctions.pointToCellQT(p, this.root);
		else if (this.partitioning.equals("gd")) // grid cell
			cell = AknnFunctions.pointToCellGD(p, this.N);
		
		String outValue;
		
		if (p.getZ() == Double.NEGATIVE_INFINITY) // 2d case
			outValue = String.format("%d\t%9.8f\t%9.8f\tT", p.getId(), p.getX(), p.getY()); // add "T" at the end
		else
			outValue = String.format("%d\t%9.8f\t%9.8f\t%9.8f\tT", p.getId(), p.getX(), p.getY(), p.getZ());
		
		context.write(new Text(cell), new Text(outValue));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		final Configuration conf = context.getConfiguration();
		
		this.partitioning = conf.get("partitioning");

		// hostname
		final String hostname = conf.get("namenode"); // get namenode name
		// username
		final String username = System.getProperty("user.name"); // get user name

		final FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		if (this.partitioning.equals("qt"))
		{
			// HDFS dir containing tree file
			String treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			// tree file name in HDFS
			String treeFileName = conf.get("treeFileName"); // get tree filename
			// full HDFS path to tree file
			String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file

			this.root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (this.partitioning.equals("gd"))
			this.N = Integer.parseInt(conf.get("N")); // get N
	}
}
