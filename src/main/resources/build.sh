cp /home/kesler/Studing/VINF/src/main/java/DBPediaSkParser.java .
cp /home/kesler/Studing/VINF/src/main/avro/page.avsc .

# Compile the avro scheme
java -jar avro-tools-1.7.7.jar compile schema page.avsc .

hadoop com.sun.tools.javac.Main DBPediaSkParser.java avro/DbPage.java
jar cf parser.jar DBPediaSkParser*.class avro/*.class
hdfs dfs -rm -r output




