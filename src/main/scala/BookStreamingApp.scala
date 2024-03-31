import org.apache.spark.sql.types.StructType

/**
 * Класс для чтения файла из записи в кафку
 */
object BookStreamingApp extends App {

  override val appName: String = "BookStreaming"

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

    val streamReadBookDf = spark
      .readStream
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .schema(bookSchema)
      .load("src/main/resources/books")
      .as[Book]
      .toJSON
      .selectExpr("CAST(value as STRING)")

    streamReadBookDf
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "book")
      .option("checkpointLocation", "tmp/checkpoint")
      .start()
      .awaitTermination()
  }
}
