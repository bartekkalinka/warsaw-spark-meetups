package xml

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{Offset, Source}
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
    ("hello world", StructType(Seq(StructField("value", StringType))))

  override def createSource(
                    sqlContext: SQLContext,
                    metadataPath: String,
                    schema: Option[StructType],
                    providerName: String,
                    parameters: Map[String, String]): Source = {
    val spark = SparkSession.getActiveSession.get
    val srcSchema = sourceSchema(sqlContext, schema, providerName, parameters)._2
    new XmlSource(spark, srcSchema)
  }

  override def shortName(): String = "xml"
}

class XmlSource(sparkSession: SparkSession, override val schema: StructType) extends Source {
  import sparkSession.implicits._

  override def getOffset: Option[Offset] = None

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = Seq("a", "b").toDF

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

