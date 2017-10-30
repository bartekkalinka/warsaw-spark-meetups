package xml

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.concurrent.duration._

class XmlSourceProvider extends StreamSourceProvider with DataSourceRegister {
  override def sourceSchema(
                    sqlContext: SQLContext,
                    schema: Option[StructType],
                    providerName: String,
                    parameters: Map[String, String]): (String, StructType) =
    ("xml", schema.get)

  override def createSource(
                    sqlContext: SQLContext,
                    metadataPath: String,
                    schema: Option[StructType],
                    providerName: String,
                    parameters: Map[String, String]): Source = {
    new XmlSource(schema.get)
  }

  override def shortName(): String = "xml"
}

class XmlSource(override val schema: StructType) extends Source {

  override def getOffset: Option[Offset] = Some(LongOffset(0))

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    println(s"getBatch($start, $end)")
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val hello = spark.range(1).
      withColumn("nc1", lit("Nested")).
      withColumn("nc2", lit("Nested2")).
      select(struct($"nc1", $"nc2") as "c1")
    hello
  }

  override def stop(): Unit = ()
}

//usage:
//  sbt package
//  spark-submit target/scala-2.11/spark-xml-source_2.11-1.0.jar
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import org.apache.spark.sql.types._
    import spark.implicits._
    val nc1 = $"nc".string
    val nc2 = $"nc2".string
    val c1 = new StructType().add(nc1).add(nc2)
    val schema = StructType(Seq(StructField("value", c1)))

    val sq = spark
      .readStream
      .format("xml")
      .schema(schema)
      .load
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start

    sq.awaitTermination()
  }
}

