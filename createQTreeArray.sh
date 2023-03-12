###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=Hadoopmaster
trainingDir=input
treeDir=sampletree
trainingDataset=paskrsNNew_obj.txt
samplerate=1
capacity=200

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/aknn-hadoop-2d3d-0.0.1-SNAPSHOT-SNAPSHOT.jar gr.uth.ece.dsel.aknn_hadoop.util.QuadtreeArray \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity
