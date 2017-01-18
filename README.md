# sparkToKafka
Create Spark job (Spark API is available for Java, Scala, Python and R) which calculates top 10 countries and produce these values in JSON format to Kafka.

aggregating data in a spark-shell:

1. creating rdd from a file with lines such as "172.226.28.134,ecfuzdmqkvqktydvisaknmovbbsltqoufihtspydkagsujqcevtguvzsmwrzttmxqxqsxgollffmtchfxoqkujigsopmya,2002-08-07,0.59095734,Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0),ECU,ECU-ES,troubleshoot,7"

val text = sc.textFile("/home/data/Documents/uservisits.txt")

2. a method to clear the rdd from "null-line"

val text_filtered = text.filter(_(0).isDigit)

3. getting map where key is a country and value is a search word

val kv = text_filtered.map(_.split(",")).map(v => (v(5), v(7))).cache()

4. initializing a neutral "zero value"

val initialCount = 0;

5. a function to aggregate the values of each key

val addToCounts = (n: Int, v: String) => n + 1

6. a function to aggregate results of addToCounts

val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

7. getting number of search words from each country

val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)

8. descending sorting and taking top 10 countries

val sortedMap = countByKey.sortBy(-_._2).take(10)

sortedMap.foreach(println)
