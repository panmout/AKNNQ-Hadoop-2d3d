###########################################################################
#                             PARAMETERS                                  #
###########################################################################

partitioning=qt	    # gd or qt
mode=ps             # bf or ps
K=10
reducers=32
nameNode=Hadoopmaster
N=800
treeFile=qtree.ser
treeDir=sampletree
trainingDir=input
queryDir=input
queryDataset=linearwaterNNew.txt
trainingDataset=paskrsNNew_obj.txt
mr1outputPath=mapreduce1
mr2outputPath=mapreduce2
mr3outputPath=mapreduce3
mr4outputPath=mapreduce4

###########################################################################
#                                    EXECUTE                              #
###########################################################################

hadoop jar ./target/aknnq-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.aknn_hadoop.main.Aknn \
partitioning=$partitioning \
mode=$mode \
K=$K \
reducers=$reducers \
nameNode=$nameNode \
N=$N \
treeFile=$treeFile \
treeDir=$treeDir \
trainingDir=$trainingDir \
queryDir=$queryDir \
queryDataset=$queryDataset \
trainingDataset=$trainingDataset \
mr1outputPath=$mr1outputPath \
mr2outputPath=$mr2outputPath \
mr3outputPath=$mr3outputPath \
mr4outputPath=$mr4outputPath \
