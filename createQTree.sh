###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=Hadoopmaster
trainingDir=input
treeDir=sampletree
trainingDataset=paskrsNNew_obj.txt
samplerate=1
capacity=50
type=1 # 1 for simple capacity based quadtree, 2 for all children split method, 3 for average width method

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/aknnq-hadoop-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.common_classes.Qtree \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity \
type=$type
