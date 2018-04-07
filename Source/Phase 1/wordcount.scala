val htFile = sc.textFile("hdfs://localhost:9000/twitterproject/hashtags.txt")
val htWords = htFile.flatMap(line => line.split(" "))
val htCounts = htWords.map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)
val htSortedCount = htCounts.sortBy(-_._2)
htSortedCount.saveAsTextFile("hdfs://localhost:9000/twitterproject/htoutput")


val urlFile = sc.textFile("hdfs://localhost:9000/twitterproject/urls.txt")
val urlWords = urlFile.flatMap(line => line.split(" "))
val urlCounts = urlWords.map(word => (word, 1)).reduceByKey(_ + _)
val urlSortedCount = urlCounts.sortBy(-_._2)
urlSortedCount.saveAsTextFile("hdfs://localhost:9000/twitterproject/urloutput")