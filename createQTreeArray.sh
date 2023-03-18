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

hadoop jar ./target/aknnq-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.common_classes.QuadtreeArray \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity
