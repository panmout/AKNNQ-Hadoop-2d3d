package gr.uth.ece.dsel.aknn_hadoop.util;

// class Point (int id, double x, double y, char type) with constructor and set-get methods

import java.io.Serializable;

public class Point implements Serializable
{
	private final int id;
	private final double x;
	private final double y;
	private double z = Double.NEGATIVE_INFINITY;
	
	public Point(int id, double x, double y)
	{
		this.id = id;
		this.x = x;
		this.y = y;
	}
	
	public Point(int id, double x, double y, double z) // create 3d points
	{
		this.id = id;
		this.x = x;
		this.y = y;
		this.z = z;
	}
	
	public final int getId()
	{
		return this.id;
	}
	
	public final double getX()
	{
		return this.x;
	}
	
	public final double getY()
	{
		return this.y;
	}
	
	public final double getZ()
	{
		return this.z;
	}
}
