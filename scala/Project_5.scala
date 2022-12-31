//Импорт библиотек
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Project_5 {
  def main(args: Array[String]): Unit = {
    // Создается спарк-сессия
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Taxi_session")
      .getOrCreate()

    //Отключаем Spark логи
    spark.sparkContext.setLogLevel("ERROR")

    // Путь до файла, который необходимо открыть
    val path = "D://PetProject/yellow_tripdata_2020-01.csv"

    // Схема для DataFrame
    val schema = get_schema()

    // DataFrame без обработки данных
    val df_Taxi = DF_from_format(spark, "csv", schema, path)

    // Обработанный DataFrame, на основе которого строим parquet
    val filtered_df = filter_df(df_Taxi)

    // Вывод обработанного DataFrame
    filtered_df.show()

    // Создание временного вида, чтобы можно было использовать SQL запрос
    filtered_df.createOrReplaceTempView("data")

    // Parquet 0 группа
    val parquet_0 = get_group_parquet(spark, "=", "0")

    // Parquet 1 группа
    val parquet_1 = get_group_parquet(spark, "=", "1")

    // Parquet 2 группа
    val parquet_2 = get_group_parquet(spark, "=", "2")

    // Parquet 3 группа
    val parquet_3 = get_group_parquet(spark, "=", "3")

    // Parquet 4 группа
    val parquet_4 = get_group_parquet(spark, ">=", "4")

    // Final_parquet (Full)
    get_full_parquet(parquet_0, parquet_1, parquet_2, parquet_3, parquet_4, "date", "results/result.parquet", "results/result.csv")
  }

  // Методы
  // Определение схемы для DataFrame
  def get_schema ():StructType = {
    val schema = StructType(Array(
      StructField("VendorID", IntegerType, true),
      StructField("tpep_pickup_datetime", TimestampType, true),
      StructField("tpep_dropoff_datetime", TimestampType, true),
      StructField("passenger_count", IntegerType, true),
      StructField("trip_distance", FloatType, true),
      StructField("RatecodeID", IntegerType, true),
      StructField("store_and_fwd_flag", StringType, true),
      StructField("PULocationID", IntegerType, true),
      StructField("DOLocationID", IntegerType, true),
      StructField("payment_type", IntegerType, true),
      StructField("fare_amount", FloatType, true),
      StructField("extra", FloatType, true),
      StructField("mta_tax", FloatType, true),
      StructField("tip_amount", FloatType, true),
      StructField("tolls_amount", FloatType, true),
      StructField("improvement_surcharge", FloatType, true),
      StructField("total_amount", FloatType, true),
      StructField("congestion_surcharge", FloatType, true)))
    return schema
  }
  // Создание DataFrame через открытие нужного формата
  def DF_from_format(sparkSession: SparkSession, file_format:String, schema:StructType, path:String):DataFrame = {
    val df = sparkSession.read.format(file_format).schema(schema).option("header", "true").load(path)
    return df
  }
  // Фильтрация DataFrame
  def filter_df(initial_df:DataFrame):DataFrame = {
    // Фильтрация:
    // 1) Удаление null-строк
    // 2) Добавление колонки id для каждой записи (Как id serial в SQL)
    val filtered_df = initial_df.na.drop.withColumn("id", monotonically_increasing_id())
    return filtered_df
  }
  // Создание parquet для каждой группы пассажиров (5 Групп -  0: 0 пассажиров; 1: 1 пассажир; 2: 2 пассажира; 3: 3 пассажира; 4: 4 и более пассажиров)
  def get_group_parquet(sparkSession: SparkSession, sign:String, group_pas: String):DataFrame = {
    val group_parquet = sparkSession.sql(
      s"""
         |WITH pas_$group_pas AS
         |(
         |	SELECT *
         |	FROM data
         |	WHERE passenger_count $sign $group_pas
         |)
         |,
         |	parquet$group_pas AS
         |(
         |	SELECT DATE(data.tpep_pickup_datetime) as date,
         |		   (COUNT(pas_$group_pas.*) / COUNT(*) * 100.0) AS percentage_${group_pas}p,
         |		   CASE WHEN MAX(pas_$group_pas.total_amount) IS NULL THEN 0 ELSE MAX(pas_$group_pas.total_amount) END max_trip_${group_pas}p,
         |		   CASE WHEN MIN(pas_$group_pas.total_amount) IS NULL THEN 0 ELSE MIN(pas_$group_pas.total_amount) END min_trip_${group_pas}p
         |	FROM data
         |	LEFT JOIN pas_$group_pas ON data.id = pas_$group_pas.id
         |	GROUP BY date
         |)
         |SELECT *
         |FROM parquet$group_pas
         |""".stripMargin)
    return group_parquet
  }
  // Объединение всех parquet в единный full_parquet
  def get_full_parquet(parquet_0: DataFrame, parquet_1: DataFrame, parquet_2: DataFrame, parquet_3: DataFrame, parquet_4: DataFrame, using_col: String, path_parquet:String, path_csv:String) = {
    val full_parquet = parquet_0
      .join(parquet_1, using_col)
      .join(parquet_2, using_col)
      .join(parquet_3, using_col)
      .join(parquet_4, using_col)
      .orderBy(using_col).cache()

    // Сохранение таблицы в parquet формате и csv формате
    full_parquet.repartition(1).write.format("parquet").mode("overwrite").option("header", "true").save(path_parquet)
    full_parquet.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(path_csv)

    // Вывод финального полного parquet
    full_parquet.show(60, false)
  }
}
