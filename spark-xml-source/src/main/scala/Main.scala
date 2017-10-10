import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
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
                    parameters: Map[String, String]): Source ={
    val srcSchema = sourceSchema(sqlContext, schema, providerName, parameters)._2
    new XmlSource(sparkSession, srcSchema)
  }
}

class XmlSource(sparkSession: SparkSession, override val schema: StructType) extends Source {
  import sparkSession.implicits._

  override def getOffset: Option[Offset] = None

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = Seq("a", "b").toDF

  override def stop(): Unit = ()
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val provider = new HelloWorldStreamSourceProvider(spark)
    val source = provider.createSource(spark.sqlContext, "", None, "a", Map.empty )

    val df = source.getBatch(None, LongOffset(-1L))

    df.collect().foreach(println)

    df.printSchema()

    spark.stop()
  }
}

