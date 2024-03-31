import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType

/**
 * Класс для чтения данных из кафки и сохранения результата в parquet файл
 */
object BookReadStreamingApp extends App {

  override val appName: String = "BookReadStreaming"

  def main(args: Array[String]) = {

    val bookSchema = new StructType()
      .add("Name", "String")
      .add("Author", "string")
      .add("Rating", "double")
      .add("Reviews", "long")
      .add("Price", "double")
      .add("Year", "int")
      .add("Genre", "string")

    import spark.implicits._

    val readKafkaBookDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "book")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), bookSchema).as("data"))
      .select("data.*")
      .as[Book]
      .filter(book => book.Rating < 4)

    readKafkaBookDf
      .writeStream
      .format("parquet")
      .option("checkpointLocation", "tmp/checkpoint")
      .option("path", "src/main/resources/load")
      .start()
      .awaitTermination()
  }
}
