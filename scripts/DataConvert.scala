// this script is used for building tpc-h data for simulation test leveraging planBuilder in velox
// 1. use spark-sql-perf(https://github.com/databricks/spark-sql-perf) to generate tpch tables, note that
//    need to set --conf spark.hive.exec.orc.write.format=0.11
// 2. use this script in spark-shell to change date format into double since planBuilder doesn't support date for now
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
val sparkSession: SparkSession = SparkSession.builder().appName("HiveSupport").enableHiveSupport().getOrCreate()
val sqlContext = sparkSession.sqlContext
val path = "hdfs://../lineitem"
var df = sqlContext.read.orc(path)

import org.apache.spark.sql.types._
val seconds_in_a_day = 86400.0
df = df.withColumn("l_shipdate",col("l_shipdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day))
df = df.withColumn("l_commitdate",col("l_commitdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day))
df = df.withColumn("l_receiptdate",col("l_receiptdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day))
val decimalSchema = df.schema.fields.map{f =>
  f match{
    case StructField(name:String, dataType:DecimalType, _, _) => col(name).cast(DoubleType)
    case _ => col(f.name)
  }
}
val df1 = df.select(decimalSchema:_*)
df1.printSchema()
df1.show(false)
df1.write.format("orc").save("hdfs://../result")
