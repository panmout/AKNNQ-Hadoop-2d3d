package gr.uth.ece.dsel.aknn_hadoop.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

public final class GetOverlaps
{
	private HashMap<String, Integer> cell_tpoints; // hashmap of training points per cell list from Phase 1 <cell_id, number of training points>
	private String partitioning; // gd or qt
	private int N; // N*N cells
	private int K; // AKNN K
	private Node root; // create root node
	private HashSet<String> overlaps;
	private String qcell;
	private Point qpoint;
	private PriorityQueue<IdDist> neighbors;
	
	public GetOverlaps (HashMap<String, Integer> cell_tpoints, int k, String partitioning)
	{
		this.cell_tpoints = new HashMap<String, Integer>(cell_tpoints);
		this.K = k;
		this.partitioning = partitioning;
		this.overlaps = new HashSet<String>();
	}
	
	public void initializeFields (Point qp, String qc, PriorityQueue<IdDist> phase2neighbors)
	{
		this.qpoint = qp;
		this.qcell = qc;
		this.neighbors = new PriorityQueue<IdDist>(phase2neighbors);
	}
	
	public final void setN (int n)
	{
		this.N = n;
	}
	
	public final void setRoot (Node root)
	{
		this.root = root;
	}
	
	public final HashSet<String> getOverlaps()
	{
		this.overlaps.clear();
		
		// call appropriate function according to partitioning and iterate query points of this cell
		if (this.partitioning.equals("gd"))
		{
			getOverlapsGD(this.qcell, this.qpoint); // find its overlaps
		}
		else if (this.partitioning.equals("qt"))
		{
			getOverlapsQT(this.qcell, this.qpoint); // find its overlaps
		}
		
		return this.overlaps;
	}
	
	// find grid query overlaps
	private final void getOverlapsGD (String qcell, Point qpoint)
	{
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
		
		How to find neighboring cells:

		If current is cell_id:
		 W is (cell_id - 1)
		 E is (cell_id + 1)
		 N is (cell_id + n)
		 S is (cell_id - n)
		NE is (cell_id + n + 1)
		NW is (cell_id + n - 1)
		SE is (cell_id - n + 1)
		SW is (cell_id - n - 1)
		
		south row cells ( 0, 1,..., n-1 ) don't have S, SE, SW neighbors
		north row cell's ( (n-1)n, (n-1)n+1,..., (n-1)n+(n-1) ) don't have N, NE, NW neighbors
		west column cells ( 0, n,..., (n-1)n ) don't have W, NW, SW neighbors
		east column cells ( n-1, 2n-1,..., (n-1)n+(n-1) ) don't have E, NE, SE neighbors
		
		
		                           xi mod ds (part of xi inside cell)
		     |----------xi------------->.(xi, yi)
		     |---------|----------|-----^--
	         |         |          |     |   yi mod ds (part of yi inside cell)
	    2*ds |---------|----------|-----|--
	         |         |          |     yi
	      ds |---------|----------|-----|--
	         |         |          |     |
	         |---------|----------|--------
	       0          ds         2*ds                  
	       
		 */
		
    	// read query point coordinates and neighbors list
    	final double xq = qpoint.getX();
    	final double yq = qpoint.getY();  	
    	
    	// find query point cell
    	final double ds = 1.0 / this.N; // interval ds (cell width)
    	final int intQCell = Integer.parseInt(qcell); // get int value of query point cell
    	final int iq = intQCell % this.N; // get i
    	final int jq = (intQCell - iq) / this.N; // get j
    	//final int iq = (int) (xq / ds); // get i
    	//final int jq = (int) (yq / ds); // get j
    	//final int intQCell = jq * this.N + iq; // calculate int cell_id
    	
    	// if neighbors list not empty, set circle radius R to the distance of farthest neighbor
    	// else half the cell width
		double R = !this.neighbors.isEmpty() ? this.neighbors.peek().getDist() : 0.5 * ds;
		
		final double x = xq % ds; // relative x from cell SW corner
		final double y = yq % ds; // relative y from cell SW corner
		
		// add number of training points in this cell, 0 if null
		final int num = this.cell_tpoints.containsKey(qcell) ? this.cell_tpoints.get(qcell) : 0;
		
		// top-bottom rows, far left-right columns
		final HashSet<Integer> south_row = new HashSet<Integer>(); // no S, SE, SW for cells in this set
		final HashSet<Integer> north_row = new HashSet<Integer>(); // no N, NE, NW for cells in this set
		final HashSet<Integer> west_column = new HashSet<Integer>(); // no W, NW, SW for cells in this set
		final HashSet<Integer> east_column = new HashSet<Integer>(); // no E, NE, SE for cells in this set
		
		for (int i = 0; i < this.N; i++) // filling sets
		{
			south_row.add(i);
			north_row.add((this.N - 1) * this.N + i);
			west_column.add(i * this.N);
			east_column.add(i * this.N + this.N - 1);
		}
				
		// case 1: there are at least knn in this cell
		if (num >= this.K && R <= ds)
		{
			final HashSet<Integer> int_overlaps = new HashSet<Integer>(); // set of overlapping cells
			
			// draw circle and check for overlaps
			if (x + R > ds && !east_column.contains(intQCell)) // E (excluding east column)
				int_overlaps.add(intQCell + 1);
			
			if (x < R && !west_column.contains(intQCell)) // W (excluding west column)
				int_overlaps.add(intQCell - 1);
			
			if (y + R > ds && !north_row.contains(intQCell)) // N (excluding north row)
				int_overlaps.add(intQCell + this.N);
			
			if (y < R && !south_row.contains(intQCell)) // S (excluding south row)
				int_overlaps.add(intQCell - this.N);
			
			if (x + R > ds && y + R > ds && !north_row.contains(intQCell) && !east_column.contains(intQCell)) // NE (excluding north row and east column)
			{
				final double xne = (iq + 1) * ds; // NE corner coords
				final double yne = (jq + 1) * ds;
				
				if (AknnFunctions.square_distance(xq, yq, xne, yne) < R * R) // if NE corner is inside circle, NE cell is overlapped
					int_overlaps.add(intQCell + this.N + 1);
			}
			if (x + R > ds && y < R && !south_row.contains(intQCell) && !east_column.contains(intQCell)) // SE (excluding south row and east column)
			{
				final double xse = (iq + 1) * ds; // SE corner coords
				final double yse = jq * ds;
				
				if (AknnFunctions.square_distance(xq, yq, xse, yse) < R * R) // if SE corner is inside circle, SE cell is overlapped
					int_overlaps.add(intQCell - this.N + 1);
			}
			if (x < R && y + R > ds && !north_row.contains(intQCell) && !west_column.contains(intQCell)) // NW (excluding north row and west column)
			{
				final double xnw = iq * ds; // NW corner coords
				final double ynw = (jq + 1) * ds;
				
				if (AknnFunctions.square_distance(xq, yq, xnw, ynw) < R * R) // if NW corner is inside circle, NW cell is overlapped
					int_overlaps.add(intQCell + this.N - 1);
			}
			if (x < R && y < R && !south_row.contains(intQCell) && !west_column.contains(intQCell)) // SW (excluding south row and west column)
			{
				final double xsw = iq * ds; // SW corner coords
				final double ysw = jq * ds;
				
				if (AknnFunctions.square_distance(xq, yq, xsw, ysw) < R * R) // if SW corner is inside circle, SW cell is overlapped
					int_overlaps.add(intQCell - this.N - 1);
			}
			
			// remove overlaps not containing training points
			final Iterator<Integer> it = int_overlaps.iterator();
			while (it.hasNext())
				if (!this.cell_tpoints.containsKey(String.valueOf(it.next())))
					it.remove();
			
			// subcase 1: no overlaps containing any tpoints, point goes straight to next phase
			if (int_overlaps.isEmpty())
				this.overlaps.add(qcell); // add qpoint cell
			
			// subcase 2: there are overlaps, additional checks must be made in next phase
			else // update overlaps list
				for (int over_cell : int_overlaps)
					this.overlaps.add(String.valueOf(over_cell));
		} // end if case 1
		
		// case 2: there are less than knn in this cell
		else
		{
			// set of surrounding cells
			final HashSet<Integer> surrounding_cells = new HashSet<Integer>();
			
			// dummy set of cells to be added (throws ConcurrentModificationException if trying to modify set while traversing it)
			final HashSet<Integer> addSquaresList = new HashSet<Integer>();
			
			int overlaps_points = 0; // total number of training points in overlaps
			
			// first element is query cell
			surrounding_cells.add(intQCell);
			
			int loopvar = 0; // loop control variable (runs until it finds >=k tpoints, then once more)
			
			while (loopvar < 2) // trying to find overlaps to fill neighbors list
			{
				overlaps_points = 0; // reset value
				addSquaresList.clear(); // clear list
				
				// getting all surrounding cells of qCell
				
				boolean runAgain = true;
				
				// keep filling set until it contains circle R inside it
				while (runAgain)
				{
					for (int square : surrounding_cells)
					{
						if (!west_column.contains(square)) // W (excluding west column)
							addSquaresList.add(square - 1);
						
						if (!east_column.contains(square)) // E (excluding east column)
							addSquaresList.add(square + 1);
						
						if (!north_row.contains(square)) // N (excluding north_row)
							addSquaresList.add(square + this.N);
						
						if (!south_row.contains(square)) // S (excluding south_row)
							addSquaresList.add(square - this.N);
						
						if (!south_row.contains(square) && !west_column.contains(square)) // SW (excluding south row and west column)
							addSquaresList.add(square - this.N - 1);
						
						if (!south_row.contains(square) && !east_column.contains(square)) // SE (excluding south row and east column)
							addSquaresList.add(square - this.N + 1);
						
						if (!north_row.contains(square) && !west_column.contains(square)) // NW (excluding north row and west column)
							addSquaresList.add(square + this.N - 1);
						
						if (!north_row.contains(square) && !east_column.contains(square)) // NE (excluding north row and east column)
							addSquaresList.add(square + this.N + 1);
					}
					
					surrounding_cells.addAll(addSquaresList); // add new squares to original set
					
					// boolean variables to check if surrounding cells include the circle with radius R
					boolean stopRunX = false;
					boolean stopRunY = false;
					
					int maxI = iq; // min & max column index of surrounding cells at query cell row
					int minI = iq;
					
					for (int i = 0; i < this.N; i++) // running through columns 0 to N
					{
						if (surrounding_cells.contains(jq * this.N + i)) // getting cells at query cell row (jq)
						{
							maxI = Math.max(i, maxI);	
							minI = Math.min(i, minI);
						}
					}
					
					if ((maxI - minI) * ds > 2 * R) // if surrounding cells width is more than 2*R, set stop var to 'true'
						stopRunX = true;
					
					int maxJ = jq; // min & max row index of surrounding cells at query cell column
					int minJ = jq;
					
					for (int j = 0; j < this.N; j++) // running through columns 0 to N
					{
						if (surrounding_cells.contains(j * this.N + iq)) // getting cells at query cell column (iq)
						{
							maxJ = Math.max(j, maxJ);	
							minJ = Math.min(j, minJ);
						}
					}
					
					if ((maxJ - minJ) * ds > 2 * R) // if surrounding cells width is more than 2*R, set stop var to 'true'
						stopRunY = true;
					
					// if all stop vars are set to 'true', stop loop
					if (stopRunX == true && stopRunY == true)
						runAgain = false;
				} // end while (runagain)
				
				// checking for overlaps in surroundings
				for (int square: surrounding_cells)
				{				
					// proceed only if cell contains any training points and skip query point cell
					if (square != intQCell && this.cell_tpoints.containsKey(String.valueOf(square)))
					{
						// cell_id = j*n + i
						final int i = square % this.N;
						final int j = (square - i) / this.N;
						// get cell center coordinates
						final double cx = i * ds + ds / 2;
						final double cy = j * ds + ds / 2;
						// circle center to cell center distance
						final double centers_dist_x = Math.abs(xq - cx);
						final double centers_dist_y = Math.abs(yq - cy);
						
						final String sq = String.valueOf(square);
						
						// check circle - cell collision
						if (i > iq && j == jq) // to the east of query cell, same row
						{
							if (xq + R > i * ds) // checking collision with cell's west wall
								this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i > iq && j > jq) // to the north-east of query cell
						{
							if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
								if (AknnFunctions.square_distance(xq, yq, i * ds, j * ds) < R * R) // if also SW corner is inside circle
									this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i == iq && j > jq) // to the north of query cell, same column
						{
							if (yq + R > j * ds) // checking collision with cell's south wall
								this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i < iq && j > jq) // to the north-west of query cell
						{
							if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
								if (AknnFunctions.square_distance(xq, yq, (i + 1) * ds, j * ds) < R * R) // if also SE corner is inside circle
									this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i < iq && j == jq) // to the west of query cell, same row
						{
							if (xq - R < (i + 1) * ds) // checking collision with cell's east wall
								this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i < iq && j < jq) // to the south-west of query cell
						{
							if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
								if (AknnFunctions.square_distance(xq, yq, (i + 1) * ds, (j + 1) * ds) < R * R) // if also NE corner is inside circle
									this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i == iq && j < jq) // to the south of query cell, same column
						{
							if (yq - R < (j + 1) * ds) // checking collision with cell's north wall
								this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
						else if (i > iq && j < jq) // to the south-east of query cell
						{
							if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
								if (AknnFunctions.square_distance(xq, yq, i * ds, (j + 1) * ds) < R * R) // if also NE corner is inside circle
									this.overlaps.add(sq); // there is collision, add cell to overlaps
						}
					}
				} // end for (traverse surroundings)
				
				// now find total training points from overlaps
				if (!this.overlaps.isEmpty())
					for (String s : this.overlaps)
						overlaps_points += this.cell_tpoints.get(s); // add this overlap's training points
				
				R += 0.5 * ds; // increase radius by half ds
				
				// if k neighbors found, run loop one more time and increase radius by the diagonal of a cell
				if (num + overlaps_points >= this.K)
				{
					loopvar++;
					R += Math.sqrt(2) * ds;
				}
			} // end while (loopvar)
		} // end case 2 else
	} // end getOverlapsGD
	
	// find quadtree query overlaps
	private final void getOverlapsQT (String qcell, Point qpoint)
	{
    	// read query point coordinates and neighbors list
    	final double xq = qpoint.getX();
    	final double yq = qpoint.getY();
    	
    	System.out.println("query cell: " + qcell);
		/* If
		 * root cell side length = L
		 * and for example
		 * cell id = 3012 (4 digits)
		 * then cell's length = L / (2 ^ 4)
		 */
    	
    	// total number of training points in this cell, 0 if null
    	final int num = this.cell_tpoints.containsKey(qcell) ? this.cell_tpoints.get(qcell) : 0;
    	
    	final double ds = 1.0 / Math.pow(2, qcell.length()); // ds = query cell width
		
    	// if neighbors list not empty, set circle radius R to the distance of farthest neighbor
    	// else half the cell width
		double R = !this.neighbors.isEmpty() ? this.neighbors.peek().getDist() : 0.5 * ds;
		
		// case 1: there are at least knn in this cell
		if (num >= this.K)
		{
			// draw circle and check for overlaps
			rangeQuery(xq, yq, R, this.root, "");
			
			// remove containing cell
			this.overlaps.remove(qcell);
			
			// remove overlaps not containing training points
			final Iterator<String> it = this.overlaps.iterator();
			while (it.hasNext())
				if (!this.cell_tpoints.containsKey(it.next()))
					it.remove();
			
			// subcase 1: no overlaps containing any tpoints, point goes straight to next phase
			if (this.overlaps.isEmpty())
				this.overlaps.add(qcell); // add qpoint cell
			
			// subcase 2: there are overlaps, additional checks must be made in next phase
			// nothing to do here, overlaps are already updated
		}
		
		// case 2: there are less than knn in this cell
		else
		{
			/* Define a new increasing radius r1:
			 * - if there are already x < k neighbors in this cell, we suppose a constant density of training points, so
			 *   in a circle of radius r and area pi*r*r we get x neighbors
			 *   in a circle of radius r1 and area pi*r1*r1 we will get k neighbors
			 *   so r1 = sqrt(k/x)*r
			 * - if no neighbors are found in this cell, then we suppose that in radius r = ds/2 we have found only one neighbor (x = 1)
			 *   and so r1 = 1.2*sqrt(k)*r (we gave it an additional 20% boost)
			 */
			double r1 = 0;
			double x = this.neighbors.size() / 2; // divide by 2 because knnlist also contains distances (size = 2*[number of neighbors])
			// if x > 0 (there are some neighbors in the list) set first value
			// else (no neighbors) set second value
			r1 = (x > 0) ? Math.sqrt(this.K / x) * R : 1.2 * Math.sqrt(this.K) * R;
			
			int overlaps_points = 0; // total number of training points in overlaps
			
			int loopvar = 0; // loop control variable (runs until it finds >=k tpoints, then once more)
			while (loopvar < 2) // trying to find overlaps to fill k-nn
			{
				overlaps_points = 0; // reset value
				this.overlaps.clear(); // clear overlaps list
				
				// draw circle and check for overlaps
				rangeQuery(xq, yq, r1, this.root, "");
				
				// remove containing cell
				this.overlaps.remove(qcell);
				
				for (String cell : this.overlaps)
					if (this.cell_tpoints.containsKey(cell)) // count points from non-empty cells
						overlaps_points += this.cell_tpoints.get(cell); // add this overlap's training points
				
				r1 += 0.1 * r1; // increase radius by 10%
				
				// if k neighbors found (first time only), run loop one more time and set r1 equal to the maximum distance from ipoint to all overlapped cells
				if ((num + overlaps_points >= this.K) && (loopvar == 0))
				{
					loopvar++;
					double maxSqrDist = 0; // square of maximum distance found so far (ipoint to cell)
					for (String cell : this.overlaps) // for every cell in overlaps
					{
						if (this.cell_tpoints.containsKey(cell)) // only non-empty cells
						{
							double x0 = 0; // cell's lower left corner coords initialization
							double y0 = 0;
							for (int i = 0; i < cell.length(); i++) // check cellname's digits
							{
								switch(cell.charAt(i))
								{
									case '0':
										y0 += 1.0/Math.pow(2, i + 1); // if digit = 0 increase y0
										break;
									case '1':
										x0 += 1.0/Math.pow(2, i + 1); // if digit = 1 increase x0
										y0 += 1.0/Math.pow(2, i + 1); // and y0
										break;
									case '3':
										x0 += 1.0/Math.pow(2, i + 1); // if digit = 3 increase x0
										break;
								}
							}
							double s = 1.0 / Math.pow(2, cell.length()); // cell side length
							/* cell's lower left corner: x0, y0
							 *        upper left corner: x0, y0 + s
							 *        upper right corner: x0 + s, y0 + s
							 *        lower right corner: x0 + s, y0
							 */
							 double maxX = Math.max(Math.abs(xq - x0), Math.abs(xq - x0 - s)); // maximum ipoint distance from cell in x-direction
							 double maxY = Math.max(Math.abs(yq - y0), Math.abs(yq - y0 - s)); // maximum ipoint distance from cell in y-direction
							 if (maxX*maxX + maxY*maxY > maxSqrDist)
								 maxSqrDist = maxX*maxX + maxY*maxY; // replace current maximum squared distance if new is bigger
						 } // end if
					 } // end for
					 r1 = Math.sqrt(maxSqrDist); // run overlaps check once more with this radius
				} // end if
				else if (loopvar == 1)
					loopvar++;
			} // end while
		}
	}
	
	private final void rangeQuery (double x, double y, double r, Node node, String address)
	{
		if (node.getNW() == null) // leaf node
			this.overlaps.add(address);
		
		// internal node
		else
		{
			if (intersect(x, y, r, node.getNW()))
				rangeQuery(x, y, r, node.getNW(), address + "0");
			
			if (intersect(x, y, r, node.getNE()))
				rangeQuery(x, y, r, node.getNE(), address + "1");
			
			if (intersect(x, y, r, node.getSW()))
				rangeQuery(x, y, r, node.getSW(), address + "2");
			
			if (intersect(x, y, r, node.getSE()))
				rangeQuery(x, y, r, node.getSE(), address + "3");
		}
	}
	
	private final boolean intersect (double x, double y, double r, Node node)
	{
		// if point is inside cell return true
		if (x >= node.getXmin() && x <= node.getXmax() && y >= node.getYmin() && y <= node.getYmax())
			return true;
		
		// check circle - cell collision
		final double ds = node.getXmax() - node.getXmin(); // cell's width
		
		// get cell center coordinates
		final double xc = (node.getXmin() + node.getXmax()) / 2;
		final double yc = (node.getYmin() + node.getYmax()) / 2;
		
		// circle center to cell center distance
		final double centers_dist_x = Math.abs(x - xc);
		final double centers_dist_y = Math.abs(y - yc);
		
		// if centers are far in either direction, return false
		if (centers_dist_x > r + ds / 2)
			return false;
		if (centers_dist_y > r + ds / 2)
			return false;
		
		// if control reaches here, centers are close enough
		
		// the next two cases mean that circle center is within a stripe of width r around the square 
		if (centers_dist_x < ds / 2)
			return true;
		if (centers_dist_y < ds / 2)
			return true;
		
		// else check the corner distance
		final double corner_dist_sq = (centers_dist_x - ds / 2)*(centers_dist_x - ds / 2) + (centers_dist_y - ds / 2)*(centers_dist_y - ds / 2);
		
		return corner_dist_sq <= r * r;
	}
}
