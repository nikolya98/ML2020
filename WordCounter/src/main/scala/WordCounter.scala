import org.apache.spark.sql.SparkSession


object WordCounter {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Word Counter App")
      .master("local").getOrCreate()

    val bookRDD = spark.sparkContext.textFile("src/main/resources/jack_london.txt")
    val wordCount = bookRDD.flatMap(line => line.split(" "))
      .map(word => word.toLowerCase
        .replaceAll("[^a-z]",  "")
        .trim)

      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(x => x._2, false)
    val df = spark.createDataFrame(wordCount)
    df.write.format("csv").save("src/main/resources/result")
  }
}
