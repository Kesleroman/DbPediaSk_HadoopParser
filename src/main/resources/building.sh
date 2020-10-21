cp /home/kesler/Studing/VINF/src/main/java/DBPediaSkParser.java .
#cp /home/kesler/Studing/VINF/target/classes/WordCount*.class .
hadoop com.sun.tools.javac.Main DBPediaSkParser.java
jar cf parser.jar DBPediaSkParser*.class
hdfs dfs -rm -r output
hadoop jar parser.jar DBPediaSkParser /user/root/input /user/root/output


