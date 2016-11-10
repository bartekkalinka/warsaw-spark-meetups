import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.sql.Timestamp

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.streaming.OutputMode

case class Cat(time: Timestamp)

object Main {


  def main(args: Array[String]): Unit = {
    LogManager.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    implicit val ctx = spark.sqlContext


    val catsInput = MemoryStream[Cat]
    val query = catsInput.toDS.groupBy(
      window($"time", "5 seconds", "5 seconds")
    ).count().writeStream.format("console").outputMode(OutputMode.Complete).start

    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 0, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 0, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 2, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 7, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 9, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 12, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 16, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 23, 0)))
    Thread.sleep(1000)
    catsInput.addData(Cat(new Timestamp(2016, 9, 28, 13, 0, 27, 0)))

    query.awaitTermination()

//    val catsOut = spark.table("memStream").as[Cat]
//
//    val windowedCounts = catsOut.groupBy(
//      window($"timestamp", "10 minutes", "5 minutes"),
//      $"word"
//    ).count()


//    val provider = new HelloWorldStreamSourceProvider(spark)
//    val source = provider.createSource(spark.sqlContext, "", None, "a", Map.empty )
//
//    val df = source.getBatch(None, LongOffset(-1L))
//
//    df.collect().foreach(println)

    spark.stop()
  }
}

