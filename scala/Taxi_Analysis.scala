//Импорт библиотек
import Main_Object.{get_schema, DF_from_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
// Импорт библиотек Vegas для построения графиков
import vegas._
import vegas.sparkExt._

object Taxi_Analysis {
  def main(args: Array[String]): Unit = {

    // Создается спарк-сессия
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Taxi_Analysis_session")
      .getOrCreate()

    // Путь до файла, который необходимо открыть
    val path = "data csv/yellow_tripdata_2020-01.csv"

    // Схема для DataFrame
    val schema = get_schema()

    // DataFrame без обработки данных
    val df_Taxi = DF_from_format(spark, "csv", schema, path)

    // Обработанный DataFrame, на основе которого строится аналитический график
    val filtered_df = filter_df(df_Taxi)

    // Создание временного вида, чтобы можно было использовать SQL запрос
    filtered_df.createOrReplaceTempView("data")

    // Строится график зависимости чаевых от кол-ва пассажиров
    val PasCount_TipAmount_graphic = Vegas("Graphic tip amount from amount of passengers", width = 500.0, height = 500.0)
      .withDataFrame(spark.sql("select passenger_count, tip_amount from data"))
      .mark(Point)
      .encodeX("passenger_count", Quant)
      .encodeY("tip_amount", Quant)

    // Вывод графика зависимости чаевых от кол-ва пассажиров
    PasCount_TipAmount_graphic.show

    // Строится график зависимости чаевых от расстояния поездки
    val TripDist_TipAmount_graphic = Vegas("Graphic tip amount from trip distance", width = 500.0, height = 500.0)
      .withDataFrame(spark.sql("select trip_distance, tip_amount from data"))
      .mark(Bar)
      .encodeX("trip_distance", Quant)
      .encodeY("tip_amount", Quant)

    // Вывод графика зависимости чаевых от расстояния поездки
    TripDist_TipAmount_graphic.show
  }

  // Методы
  // Фильтрация DataFrame
  def filter_df(initial_df: DataFrame): DataFrame = {
    // Фильтрация:
    // 1) Удаление null-строк
    // 2) Добавление колонки id для каждой записи (Как id serial в SQL)
    val filtered_df = initial_df.na.drop.select("passenger_count", "trip_distance", "tip_amount")
    return filtered_df
  }
}
