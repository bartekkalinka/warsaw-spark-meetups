import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val ds = spark.range(3).withColumn("group", $"id" % 2).as[(Long, Long)].groupByKey { case (id, g) => g }
    ds.flatMapGroupsWithState[Long, (Long, Long)](OutputMode.Append(), GroupStateTimeout.NoTimeout()) { case (a, iter, state) => iter }
  }
}

