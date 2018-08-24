###############
TASK 1 
###############
Compilation Steps:
1. hdfs dfs -put ~/inputTweets.txt/ ~/input
2. hadoop com.sun.tools.javac.Main *.java
3. jar cf wc.jar *.class
4. hadoop jar wc.jar WordCount ~/input/books ~/output1
5. hdfs dfs -get ~/output1

###############
TASK 2 
###############
Compilation Steps:

1. hdfs dfs -put ~/inputTweets.txt/ ~/input
2. hadoop com.sun.tools.javac.Main WordCooccurrencePairs.java
3. hadoop com.sun.tools.javac.Main WordCooccurrenceStripes.java
4. jar cf wcPairs.jar WordCooccurrencePairs*.class
5. jar cf wcStripes.jar WordCooccurrenceStripes*.class
6. hadoop jar wcPairs.jar ~/input/inputTweets.txt ~/pairsOutput
7. hadoop jar wcStripes.jar ~/input/inputTweets.txt ~/stripesOutput
8. hdfs dfs -get ~/pairsOutput
9. hdfs dfs -get ~/stripesOutput


###############
ACTIVITY 1 
###############
1. hdfs dfs -put ~/texts/ ~/input
2. hadoop com.sun.tools.javac.Main *.java
3. jar cf WordCountActivity1.jar *.class
4. hadoop jar WordCountActivity1.jar WordCountActivity1 ~/input/texts ~/outputFolder1 ~/la.lexicon.csv
