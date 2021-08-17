
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_trunc, dayofyear, lit, max, min, sum, to_date, count}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object main {
  val InputPath = "src/data/yellow_tripdata_2020-04.csv"
  val OutputPath = "src/data/out"

  def main(args : Array[String]) : Unit = {
    val spark : SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Test")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("ERROR")
    val mySchema =  new StructType(Array(
      new StructField("vendor_id", ByteType, true),
      new StructField("tpep_pickup_datetime", TimestampType, true),
      new StructField("tpep_dropoff_datetime", TimestampType, true),
      new StructField("passenger_count", ByteType, true),
      new StructField("trip_distance", FloatType, true),
      new StructField("rate_code_id", ByteType, true),
      new StructField("store_and_fwd_flag", StringType, true),
      new StructField("pu_Location_id", IntegerType, true),
      new StructField("do_location_id", IntegerType, true),
      new StructField("payment_type", ByteType, true),
      new StructField("fare_amount", FloatType, true),
      new StructField("extra", FloatType, true),
      new StructField("mta_tax", FloatType, true),
      new StructField("tip_amount", FloatType, true),
      new StructField("tolls_amount", FloatType, true),
      new StructField("improvement_surcharge", FloatType, true),
      new StructField("total_amount", FloatType, true),
      new StructField("congestion_surcharge", FloatType, true)
    ))

    val yellowTaxiTripData_2020_04 = spark.read.format("csv")
      .option("header", "true")
      .schema(mySchema)
      .load(InputPath)
      .cache()

    val group_date_passenger = yellowTaxiTripData_2020_04
      .groupBy(to_date(col("tpep_dropoff_datetime")).as("date"), col("passenger_count"))
      .agg(count("*").as("sum_day_pas"))
      .select(col("passenger_count"), col("date"), col("sum_day_pas"))
    
    val group_date = yellowTaxiTripData_2020_04
      .groupBy(to_date(col("tpep_dropoff_datetime")).as("date"))
      .agg(count("*").as("sum_day"))
      .select(col("date"), col("sum_day"))

    var sum_by_day_pas = group_date_passenger
      .join(group_date, group_date_passenger("date") === group_date("date"), "left")
      .select(col("passenger_count"),
        group_date_passenger("date").as("date"),
        (col("sum_day_pas") / col("sum_day")).as("percent") )

    val passenger_count_4_plus = sum_by_day_pas.where(col("passenger_count") >= 4)
      .groupBy("date")
      .agg(sum("percent").alias("percent_sum"))
      .select( lit(4).as("passenger_count"), col("date"), col("percent_sum").as("percent"))

    sum_by_day_pas = sum_by_day_pas
      .where(col("passenger_count") < 4 || col("passenger_count").isNull)
      .union(passenger_count_4_plus)

    val sum_by_day_pas_new = sum_by_day_pas.na.fill(0)
    var df_transpose = sum_by_day_pas_new.groupBy("date").pivot("passenger_count").sum("percent").na.fill(0.0)
    df_transpose = df_transpose
      .orderBy(col("date"))
      .withColumnRenamed("0", "percentage_zero")
      .withColumnRenamed("1", "percentage_1p")
      .withColumnRenamed("2", "percentage_2p")
      .withColumnRenamed("3", "percentage_3p")
      .withColumnRenamed("4", "percentage_4p")

    df_transpose.show(1000)

    val group_date_passenger_minmax = yellowTaxiTripData_2020_04
      .groupBy(to_date(col("tpep_dropoff_datetime")).as("date"))
      .agg(max("total_amount").as("max_amount"), min("total_amount").as("min_amount"))
      .select(col("date"), col("max_amount"), col("min_amount"))
    df_transpose = df_transpose
      .join(group_date_passenger_minmax, df_transpose("date") === group_date_passenger_minmax("date"))
      .select(df_transpose("*"), col("max_amount"), col("min_amount"))
      .orderBy(col("date"))
    df_transpose.show(1000)
    df_transpose.repartition(2).write.format("parquet")
      .mode("overwrite")
      .option("path", OutputPath)
      .save()
    Thread.sleep(Int.MaxValue)
  }
}
