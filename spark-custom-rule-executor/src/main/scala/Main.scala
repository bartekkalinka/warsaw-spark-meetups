import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object MyStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println("Hello world")
    Nil
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.experimental.extraStrategies = Seq(MyStrategy)
    val query = spark.catalog.listTables().filter(_.name == "five")
    query.explain(extended = true)
    spark.stop()
  }
}

