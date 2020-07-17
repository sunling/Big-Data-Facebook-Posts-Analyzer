# Facebook Posts Analyzer
# Facebook Posts Analysis - inspirations
-For a single account, What the most common words used in all of the posts</br>
-For a single account, how many posts posted each year?</br>
-For a single account, what are the most commented posts?</br>
-What are the most liked posts?</br>
-Who are my ‘best friends’?

# Development Environment 
-OS : mac OS high Sierra Version 10.13.4 </br>
-Hadoop 2.6.5 </br>
-Apache Spark 2.6 </br>
-Hive 2.3.1 </br>
-Python 3.6.4 </br>
-Kafka 1.1.0

# Install libs for Python
pip install facebook</br>
pip install pandas</br>
pip install plotly</br>
pip install matplotlib</br>
pip install pyspark</br>
pip install numpy</br>
pip install kafka</br>

# Data Flow
-Using Facebook Graph API and Http to request real-timing data</br>
-Receiving and parsing data </br>
-Sending data to Kafka</br>
-Using Spark Streaming to read data from Kafka</br>
-Saving data to Hive</br>
-Using Spark SQL to query and analysis</br>
-Using Plotly to visualize data</br>

# How does it work
1.Firstly, you need to be sure all the components are correctly installed and started, mainly focus on kafka and hive.</br>
-Try to create a topic to test if kafka is working </br>
#create a topic</br>
<code>bash /usr/local/Cellar/kafka/1.1.0/bin/kafka-topics --create --zookeeper localhost: 9092 --replication-factor 1 --partitions 1 --topic bgposts </code> </br>
#read msg</br>
<code>/usr/local/Cellar/kafka/1.1.0/bin/kafka-console-consumer --bootstrap-server localhost: 9092 --topic bgposts </code></br> 
#produce msg</br>
<code>/usr/local/Cellar/kafka/1.1.0/bin/kafka-console-producer --broker-list localhost:9092 --topic bgposts</code> </br>
-To test hive is working type 'hive' on the command window, see if it runs properly and run 'show tables' see if it can show tables in the  database default</br>
</br>2.My kafka topic is 'bgposts', Run the BGConsumer.py script </br>
When it's running, it gets data from kafka. Since the spark streaming batch interval is set to 10s, the interval analysis result is stored to hive for further analysis. In the mean time, we extract data from hive and proceed further analysis using spark SQL, then visualizing the final result by Plotly. In our main program, the spark SQL will analysis the history data, and update the data visualization every 10s with the latest data.</br>
The main program BGConsumer.py need to be initialized by spark</br>
<code>$ spark-submit --jars /usr/local/Cellar/kafka/1.1.0/libexec/libs/spark-streaming-kafka-assembly_2.11-1.6.1.jar /..../BGConsumer.py</code></br>
</br>3.Run the BGProducer.py script, it requests data from facebook and sends them to kafak</br>
<code>$$ python code/BGProducer.py</code></br>
</br>4.Virtualization Results</br>
output\
