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
	
	public final void setId(int pid)
	{
		this.pid = pid;
	}
	
	public final void setDist(double dist)
	{
		this.dist = dist;
	}
	
	public final int getId()
	{
		return this.pid;
	}
	
	public final double getDist()
	{
		return this.dist;
	}
}
