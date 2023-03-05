package gr.uth.ece.dsel.aknn_hadoop.main;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Formatter;
import java.util.FormatterClosedException;

public class Aknn
{
	private static Formatter outputTextFile; // local output text file
	private static String partitioning; // grid or quadtree
	private static String mode; // bf or ps
	private static String K;
	private static String reducers;
	private static String nameNode;
	private static String N;
	private static String treeFile;
	private static String treeDir;
	private static String queryDir;
	private static String queryDataset;
	private static String trainingDir;
	private static String trainingDataset;
	private static String mr1outputPath;
	private static String mr2outputPath;
	private static String mr3outputPath;
	private static String mr4outputPath;

	public static void main(String[] args) throws Exception
	{
		try
		{
			outputTextFile = new Formatter(new FileWriter("results.txt", true)); // appendable file
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
		
		for (String arg: args)
		{
			String[] newarg;
			if (arg.contains("="))
			{
				newarg = arg.split("=");
				
				if (newarg[0].equals("partitioning"))
					partitioning = newarg[1];
				if (newarg[0].equals("mode"))
					mode = newarg[1];
				if (newarg[0].equals("K"))
					K = newarg[1];
				if (newarg[0].equals("reducers"))
					reducers = newarg[1];
				if (newarg[0].equals("nameNode"))
					nameNode = newarg[1];
				if (newarg[0].equals("N"))
					N = newarg[1];
				if (newarg[0].equals("treeFile"))
					treeFile = newarg[1];
				if (newarg[0].equals("treeDir"))
					treeDir = newarg[1];
				if (newarg[0].equals("queryDir"))
					queryDir = newarg[1];
				if (newarg[0].equals("queryDataset"))
					queryDataset = newarg[1];
				if (newarg[0].equals("trainingDir"))
					trainingDir = newarg[1];
				if (newarg[0].equals("trainingDataset"))
					trainingDataset = newarg[1];
				if (newarg[0].equals("mr1outputPath"))
					mr1outputPath = newarg[1];
				if (newarg[0].equals("mr2outputPath"))
					mr2outputPath = newarg[1];
				if (newarg[0].equals("mr3outputPath"))
					mr3outputPath = newarg[1];
				if (newarg[0].equals("mr4outputPath"))
					mr4outputPath = newarg[1];
			}
			else
				throw new IllegalArgumentException("not a valid argument, must be \"name=arg\", : " + arg);
		}

		String queryFile = String.format("%s/%s", queryDir, queryDataset);
		String trainingFile = String.format("%s/%s", trainingDir, trainingDataset);
		
		if (!partitioning.equals("qt") && !partitioning.equals("gd"))
			throw new IllegalArgumentException("partitoning arg must be 'qt' or 'gd'");
		
		// execution starts
		long t0 = System.currentTimeMillis();
		
		String startMessage = String.format("AKNN %s-%s starts\n", partitioning.toUpperCase(), mode.toUpperCase());
		System.out.println(startMessage);
		writeToFile(outputTextFile, startMessage);
		
		// Phase 1
		// parameters: <training dataset path> <output path> <namenode name> <treeDir> <treeFileName> <N> <partitioning> <reducers>
		String[] driver1args = new String[] {trainingFile, mr1outputPath, nameNode, treeDir, treeFile, N, partitioning, reducers};
		new gr.uth.ece.dsel.aknn_hadoop.phase1.Driver1().run(driver1args);
		
		long t1 = System.currentTimeMillis();
		String phase1Message = String.format("Phase 1 time: %d millis\n", t1 - t0);
		System.out.println(phase1Message);
		writeToFile(outputTextFile, phase1Message);
		
		// Phase 2
		// parameters: <query dataset path> <training dataset path> <output path> <namenode name> <treeDir> <treeFileName> <N> <K> <partitioning> <mode> <reducers>
		String[] driver2args = new String[] {queryFile, trainingFile, mr2outputPath, nameNode, treeDir, treeFile, N, K, partitioning, mode, reducers};
		new gr.uth.ece.dsel.aknn_hadoop.phase2.Driver2().run(driver2args);
		
		long t2 = System.currentTimeMillis();
		String phase2Message = String.format("Phase 2 time: %d millis\n", t2 - t1);
		System.out.println(phase2Message);
		writeToFile(outputTextFile, phase2Message);
		
		// Phase 3
		// parameters: <mapreduce2 output path> <training dataset> <output path> <namenode name> <treeDir> <treeFileName> <N> <K> <MapReduce1 output dir> <partitioning> <mode> <reducers>
		String[] driver3args = new String[] {mr2outputPath, trainingFile, mr3outputPath, nameNode, treeDir, treeFile, N, K, mr1outputPath, partitioning, mode, reducers};
		new gr.uth.ece.dsel.aknn_hadoop.phase3.Driver3().run(driver3args);
		
		long t3 = System.currentTimeMillis();
		String phase3Message = String.format("Phase 3 time: %d millis\n", t3 - t2);
		System.out.println(phase3Message);
		writeToFile(outputTextFile, phase3Message);
		
		// Phase 4
		// parameters: <mapreduce3 output> <mapreduce2 output> <output path> <K> <reducers>
		String[] driver4args = new String[] {mr3outputPath, mr2outputPath, mr4outputPath, K, reducers};
		new gr.uth.ece.dsel.aknn_hadoop.phase4.Driver4().run(driver4args);
		
		long t4 = System.currentTimeMillis();
		String phase4Message = String.format("Phase 4 time: %d millis\n", t4 - t3);
		System.out.println(phase4Message);
		writeToFile(outputTextFile, phase4Message);
		
		String aknnMessage = String.format("%s-%s,  time: %d millis\n", partitioning.toUpperCase(), mode.toUpperCase(), t4 - t0);
		System.out.println(aknnMessage);
		writeToFile(outputTextFile, aknnMessage);
		
		outputTextFile.close();
	}
	
	private static void writeToFile(Formatter file, String s)
	{
		try
		{
			outputTextFile.format(s);
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(1);
		}
	}

}
