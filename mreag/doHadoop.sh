#! /bin/sh
#when connection refused occurs -> sudo service ssh restart
javac MR1.java -d units -cp $(/home/ganeshkumar269/hadoop/hadoop-3.3.0/bin/hadoop classpath):. &&
jar -cvf units.jar -C units/ . &&
~/hadoop/hadoop-3.3.0/bin/hadoop jar units.jar hadoop.MR1 input_dir output_dir
#~/hadoop/hadoop-3.3.0/bin/hadoop fs -ls output_dir/ &&
# ~/hadoop/hadoop-3.3.0/bin/hadoop fs -cat output_dir/part-r-00000 