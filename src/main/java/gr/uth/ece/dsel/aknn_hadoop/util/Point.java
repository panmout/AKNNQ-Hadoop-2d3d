package gr.uth.ece.dsel.aknn_hadoop.util;

// class Point (int id, double x, double y, char type) with constructor and set-get methods

import java.io.Serializable;

public class Point implements Serializable
{
	private int id;
	private double x;
	private double y;
	
	public Point(int id, double x, double y)
	{
		this.id = id;
		this.x = x;
		this.y = y;
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
}
