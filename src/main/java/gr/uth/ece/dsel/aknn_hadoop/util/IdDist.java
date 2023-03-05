package gr.uth.ece.dsel.aknn_hadoop.util;

import java.io.Serializable;

// class IdDist (int pid, double distance) with constructor and set-get methods

public final class IdDist implements Serializable
{
	private int pid;
	private double dist;
	
	public IdDist(int pid, double dist)
	{
		setId(pid);
		setDist(dist);
	}
	
	public void setId(int pid)
	{
		this.pid = pid;
	}
	
	public void setDist(double dist)
	{
		this.dist = dist;
	}
	
	public int getId()
	{
		return this.pid;
	}
	
	public double getDist()
	{
		return this.dist;
	}
}
