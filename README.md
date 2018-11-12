# TFIDF-Score-calculation-using-Hadoop-MapReduce
This is a mapReduce program written in java in Hadoop environment to calculate TermFrequency, Inverse document frequency of a search query in the given set of documents. Ultimately to find out which document is very relevant to the given search query.

Follow the below steps in executing the program on hadoop file system environment:

First copy files from local to hadoop file system using the command

hadoop fs -copyFromLocal sourcePath /user/cloudera/wordCount/input

Part 2: (Give comands in the following order)

javac DocWordCount.java -cp $(hadoop classpath) -d build/
jar -cvf DocWordCount.jar -C build/ .
hadoop jar DocWordCount.jar org.myorg.DocWordCount /user/cloudera/wordCount/input /user/cloudera/wordCount/DocWordCount

Part 3:

javac TermFrequency.java -cp $(hadoop classpath) -d build/
jar -cvf TermFrequency.jar -C build/ .
hadoop jar TermFrequency.jar org.myorg.TermFrequency /user/cloudera/wordCount/input /user/cloudera/wordCount/TermFrequency

Part 4:

javac TFIDF.java -cp $(hadoop classpath) -d build/
jar -cvf TFIDF.jar -C build/ .
hadoop jar TFIDF.jar org.myorg.TFIDF /user/cloudera/wordCount/input /user/cloudera/wordCount/TFIDF /user/cloudera/wordCount/TFIDF1

Part 5:

javac Search.java -cp $(hadoop classpath) -d build/
jar -cvf Search.jar -C build/ .
hadoop jar Search.jar org.myorg.Search "computer science" /user/cloudera/wordCount/TFIDF1 /user/cloudera/wordCount/Search1
hadoop jar Search.jar org.myorg.Search "data analysis" /user/cloudera/wordCount/TFIDF1 /user/cloudera/wordCount/Search2



