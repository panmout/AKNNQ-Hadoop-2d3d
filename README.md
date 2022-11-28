# AKNNQ-Hadoop

## A MapReduce implementation of a parallel and distributed algorithm for efficient solving of the All K Nearest Neighbor query involving Big Data

### Description
The algorithm needs two user provided spatial datasets of point objects in the form {int, double, double} for 2D or (int, double, double, double) for 3d, with their coordinates normalized in the area (0,1).
The user must also provide the number of neighbors *K* and the grid space decomposition parameter *N*.
The first dataset is called *query* and the second dataset is called *training*.

The algorithm uses two partitioning methods (grid and quad tree) and two computational methods (brute force and plane sweep).

The algorithm consists of four MapReduce phases:
1. Phase 1: Count the number of *training* points in every cell
2. Phase 2: Create a preliminary list of *K* nearest neighbors by searching inside each cell
3. Phase 3: Try to discover neighbors in adjacent cells
4. Phase 4: Merge lists of phases 3 and 4 into the final one.

### How to run
User must edit script file *run.sh* and provide the appropriate parameters:
- partitioning: *gd* or *qt* for grid or quad tree partitioning, respectively
- mode: *bf* or *ps* for brute force and quad tree computational methods, respectively
- K: the desired number of neighbors
- reducers: the number of reducers
- namenode: the name of the machine used as Namenode of the Hadoop cluster
- N: the grid space decomposition parameter (in 2D it creates NxN equal sized square cells, in 3D it creates NxNxN equal sized cubic cells)
- treeFile: the file name of the quad tree binary file, created by *createQTree.sh* or *createQTreeArray.sh* scripts
- treeDir: the HDFS directory containing the *treeFile*
- trainingDir: the HDFS directory containing the *training* dataset
- queryDir: the HDFS directory containing the *query* dataset
- queryDataset: the file name of the *query* dataset
- trainingDataset: the file name of the *training* dataset
- mr1outputPath: the name of the HDFS directory for Phase 1 output
- mr2outputPath: the name of the HDFS directory for Phase 2 output
- mr3outputPath: the name of the HDFS directory for Phase 3 output
- mr4outputPath: the name of the HDFS directory for Phase 4 (and final) output

After that, just type /run.sh

### How to create a quad tree binary file
There are two different script files, *createQTree.sh* and *createQTreeArray.sh* that create quad tree files using different methods. The first one is recommended and activated by default.
User must edit script file *createQTree.sh* and provide the appropriate parameters:
- nameNode: (same as run.sh)
- trainingDir: (same as run.sh)
- treeDir: (same as run.sh)
- trainingDataset: (same as run.sh)
- samplerate: desired sample rate of the *training* dataset. Give an integer 1 - 100
- capacity: the maximum desired number of *training* points in each cell
- type: *1* (recommended) for simple capacity based quadtree, *2* for all children split method, *3* for average width method

After that, run the script file and a copy of the created quad tree, as *qtree.ser*, will be stored both locally and in the appropriate HDFS directory.

### Delete MapReduce output HDFS directories
Run the *delete-hdfs-dirs.sh* script file
