package gr.uth.ece.dsel.aknn_hadoop.util;

import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.hadoop.mapreduce.Reducer.Context;

public final class PsNeighbors
{
	private ArrayList<Point> tpoints;
	private int k;
	private PriorityQueue<IdDist> neighbors;
	private Context context;
	
	public PsNeighbors(ArrayList<Point> tp, int K, Context con)
	{
		this.tpoints = new ArrayList<Point>(tp);
		this.k = K;
		this.neighbors = new PriorityQueue<IdDist>(this.k, new IdDistComparator("max")); // max heap of K neighbors
		this.context = con;
	}
	
	public final PriorityQueue<IdDist> getNeighbors(Point qpoint)
	{
		this.neighbors.clear();
		
	 	if (!this.tpoints.isEmpty()) // if this cell has any tpoints
		{	
			double x_left = this.tpoints.get(0).getX(); // leftmost tpoint's x
			double x_right = this.tpoints.get(this.tpoints.size() - 1).getX(); // rightmost tpoint's x
			
			// check for neighbors to the left or to the right of the query point
			boolean check_right = false;
			boolean check_left = false;
			
			// tpoints list indexes
			int low = 0;
			int high = 0;
			
			if (qpoint.getX() < x_left) // qpoint is at left of all tpoints
			{
				check_right = true;
				low = 0;
			}
			else if (x_right < qpoint.getX()) // qpoint is at right of all tpoints
			{
				check_left = true;
				high = this.tpoints.size() - 1;
			}
			else // qpoint is among tpoints
			{
				check_left = true;
				check_right = true;
				
				int tindex = AknnFunctions.binarySearchTpoints(qpoint.getX(), this.tpoints); // get tpoints array index for qpoint interpolation
				low = tindex + 1;
				high = tindex;
			}
			
			boolean cont_search = true; // set flag to true
			
			if (check_right)
				while (low < this.tpoints.size() && cont_search == true) // scanning for neighbors to the right of tindex
					cont_search = psNeighbors(qpoint, low++);
			
			cont_search = true; // reset flag to true
			
			if (check_left)
				while (high >= 0 && cont_search == true) // scanning for neighbors to the left of tindex
					cont_search = psNeighbors(qpoint, high--);
		} // end if
	 	
	    return this.neighbors;
	 	// end PsNeighbors
	}
	
	private final boolean psNeighbors(Point qpoint, int i)
	{
		final Point tpoint = this.tpoints.get(i); // get tpoint
		
		if (this.neighbors.size() < this.k) // if queue is not full, add new tpoints 
		{
			final double dist = AknnFunctions.distance(qpoint, tpoint); // distance calculation
			final IdDist neighbor = new IdDist(tpoint.getId(), dist); // create neighbor
			
	    	if (!AknnFunctions.isDuplicate(this.neighbors, neighbor))
	    		this.neighbors.offer(neighbor); // insert to queue
		}
		else  // if queue is full, run some checks and replace elements
		{
			final double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
			
			if (AknnFunctions.xDistance(qpoint, tpoint) > dm) // if tpoint's x distance is greater than neighbors max distance
				return false; // end for loop, no other points at this side will have smaller distance
			else
			{
				final double dist = AknnFunctions.distance(qpoint, tpoint); // distance calculation
				
				if (dist < dm) // compare distance
  				{
					final IdDist neighbor = new IdDist(tpoint.getId(), dist); // create neighbor
					
  			    	if (!AknnFunctions.isDuplicate(this.neighbors, neighbor))
  			    	{
  						this.neighbors.poll(); // remove top element
  			    		this.neighbors.offer(neighbor); // insert to queue
  			    	}
  				} // end if
			} // end else
		} // end else
		return true; // neighbors updated, proceed to next qpoint
	} // end psNeighbors
}
