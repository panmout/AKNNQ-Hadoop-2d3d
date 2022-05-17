package gr.uth.ece.dsel.aknn_hadoop.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

public final class AknnFunctions
{
	// return euclidean distance between two points (x1, y1) and (x2, y2)
	public static final double distance (double x1, double y1, double x2, double y2)
	{
		return Math.sqrt(square_distance (x1, y1, x2, y2));
	}// end euclidean distance
	
	// return euclidean distance between two points
	public static final double distance (Point ipoint, Point tpoint)
	{
		return distance(ipoint.getX(), ipoint.getY(), tpoint.getX(), tpoint.getY());
	}// end euclidean distance
	
	// return square of euclidean distance between two points (x1, y1) and (x2, y2)
	public static final double square_distance (double x1, double y1, double x2, double y2)
	{
		return Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2);
	}// end square of euclidean distance
	
	// return x-distance between two points
	public static final double xDistance (Point qpoint, Point tpoint)
	{
		return Math.abs(qpoint.getX() - tpoint.getX());
	}
		
	// String to point
	public static final Point stringToPoint(String line, String sep)
	{
		final String[] data = line.trim().split(sep);
		final int id = Integer.parseInt(data[0]);
		final double x = Double.parseDouble(data[1]);
		final double y = Double.parseDouble(data[2]);
		return new Point(id, x, y);
	}
	
	// point to GD cell
	/*
	Cell array (numbers inside cells are cell_id)

	    n*ds |---------|----------|----------|----------|----------|----------|--------------|
	         | (n-1)n  | (n-1)n+1 | (n-1)n+2 |          | (n-1)n+i |          | (n-1)n+(n-1) |
	(n-1)*ds |---------|----------|----------|----------|----------|----------|--------------|
	         |         |          |          |          |          |          |              |
	         |---------|----------|----------|----------|----------|----------|--------------|
	         |   j*n   |  j*n+1   |  j*n+2   |          |  j*n+i   |          |   j*n+(n-1)  |
	    j*ds |---------|----------|----------|----------|----------|----------|--------------|
	         |         |          |          |          |          |          |              |
	         |---------|----------|----------|----------|----------|----------|--------------|
	         |   2n    |   2n+1   |   2n+2   |          |   2n+i   |          |     3n-1     |
	    2*ds |---------|----------|----------|----------|----------|----------|--------------|
	         |    n    |    n+1   |    n+2   |          |    n+i   |          |     2n-1     |
	      ds |---------|----------|----------|----------|----------|----------|--------------|
	         |    0    |     1    |     2    |          |     i    |          |      n-1     |
	         |---------|----------|----------|----------|----------|----------|--------------|
	       0          ds         2*ds                  i*ds               (n-1)*ds          n*ds


	So, cell_id(i,j) = j*n+i
	
	ds = 1.0/N;
	i = (int) (x/ds)
	j = (int) (y/ds)
	cell = j*N+i
	*/
	public static final String pointToCellGD(Point p, int n)
	{
		final double ds = 1.0 / n; // interval ds (cell width)
		final double x = p.getX();  // p.x
		final double y = p.getY();  // p.y
		final int i = (int) (x / ds); // i = (int) x/ds
		final int j = (int) (y / ds); // j = (int) y/ds
		final int cellId = j * n + i;
		return String.valueOf(cellId); // return cellId
	}
	
	// node to cell
	public static final String nodeToCell(Node node)
	{
		return pointToCellQT((node.getXmin() + node.getXmax()) / 2, (node.getYmin() + node.getYmax()) / 2, node);
	}
	
	// point to QT cell
	public static final String pointToCellQT(Point p, Node node)
	{
		return pointToCellQT(p.getX(), p.getY(), node);
	}
	
	// point to QT cell
	public static final String pointToCellQT(double x, double y, Node node)
	{
		if (node.getNW() != null)
		{
			if (x >= node.getXmin() && x < (node.getXmin() + node.getXmax()) / 2) // point inside SW or NW
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SW
				{
					return "2" + pointToCellQT(x, y, node.getSW());
				}
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NW
				{
					return "0" + pointToCellQT(x, y, node.getNW());
				}
			}
			else if (x >= (node.getXmin() + node.getXmax()) / 2 && x < node.getXmax()) // point inside SE or NE
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SE
				{
					return "3" + pointToCellQT(x, y, node.getSE());
				}
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NE
				{
					return "1" + pointToCellQT(x, y, node.getNE());
				}
			}
		}
		return new String("");
	}
	
	// check for duplicates in PriorityQueue
	public static final boolean isDuplicate(PriorityQueue<IdDist> pq, IdDist neighbor)
	{
		Iterator<IdDist> it = pq.iterator();
		while (it.hasNext())
		{
			IdDist elem = it.next();
			if (elem.getId() == neighbor.getId())
				return true;
		}
		return false;
	}
	
	// PriorityQueue<IdDist> to String
	public static final String pqToString(PriorityQueue<IdDist> pq, int k)
	{
		// if we use pq directly, it will modify the original PQ, so we make a copy
		PriorityQueue<IdDist> newPQ = new PriorityQueue<IdDist>(pq);
		
		StringBuilder output = new StringBuilder();
		
		int counter = 0;
		
		while (!newPQ.isEmpty() && counter < k) // add neighbors to output
	    {
			IdDist elem = newPQ.poll();
			int pid = elem.getId();
			double dist = elem.getDist();
			output.append(String.format("%d\t%11.10f\t", pid, dist));
			counter++;
		}
		
		return output.toString();
	}
	
	// return point array index for point interpolation
	public static final int binarySearchTpoints(double x, ArrayList<Point> points)
	{
		int low = 0;
		int high = points.size() - 1;
		int middle = (low + high + 1) / 2;
		int location = -1;
		
		do
		{
			if (x >= points.get(middle).getX())
			{
				if (middle == points.size() - 1) // middle = array length
				{
					location = middle;
				}
				else if (x < points.get(middle + 1).getX()) // x between middle and high
				{
					location = middle;
				}
				else // x greater than middle but not smaller than middle+1
				{
					low = middle + 1;
				}
			}
			else // x smaller than middle
			{
				high = middle - 1;
			}
			
			middle = (low + high + 1) / 2; // recalculate middle
			
		} while ((low < high) && (location == -1));
		
		return location;
	}
}
