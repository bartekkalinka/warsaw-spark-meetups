import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class HelloWorldStreamSourceProvider(sparkSession: SparkSession) extends StreamSourceProvider {
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
                    parameters: Map[String, String]): Source = new HelloWorldSource(sparkSession)
}

class HelloWorldSource(sparkSession: SparkSession) extends Source {
  import sparkSession.implicits._
  override def schema: StructType = StructType(Seq(StructField("value", StringType)))

  override def getOffset: Option[Offset] = None

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = Seq("Hello world").toDF

  override def stop(): Unit = ()
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val provider = new HelloWorldStreamSourceProvider(spark)
    val source = provider.createSource(spark.sqlContext, "", None, "a", Map.empty )

    val df = source.getBatch(None, LongOffset(-1L))

    df.collect().foreach(println)

    spark.stop()
  }
}

