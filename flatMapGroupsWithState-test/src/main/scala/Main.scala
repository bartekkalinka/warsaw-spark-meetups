import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val ds = spark.range(3).withColumn("group", $"id" % 2).as[(Long, Long)].groupByKey { case (id, g) => g }
    ds.flatMapGroupsWithState[Long, (Long, Long)](OutputMode.Append(), GroupStateTimeout.NoTimeout()) { case (a, iter, state) => iter }

    val wordsGrouped = kafka(spark)
    wordsGrouped.flatMapGroupsWithState[Long, (String, Long)](OutputMode.Append(), GroupStateTimeout.NoTimeout()) { case (word, iter, state) =>
      val count = state.getOption.getOrElse(0L) + iter.length
      state.update(count)
      Iterator((word, count))
    }
  }

  private def kafka(spark: SparkSession): KeyValueGroupedDataset[String, String] = {
    import spark.implicits._
    val fromKafka = spark.readStream.format("kafka").option("subscribe", "sparkathon").option("kafka.bootstrap.servers", "localhost:9092").load
    fromKafka.select($"value" cast "string" as "line").as[String].flatMap(line => line.split("\\s+")).groupByKey(identity)
  }
}

