val sfmta_raw = spark.sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("file:///bdi/resources/sfmtaAVLRawData01012013.csv")

sfmta_raw.printSchema
sfmta_raw.createOrReplaceTempView("sfmta_raw")
spark.sql("SELECT count(*) FROM sfmta_raw").show()
spark.sql("SELECT * FROM sfmta_raw LIMIT 5").show()



import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
def setNotNull(df: DataFrame, columns: Seq[String]) : DataFrame = {
  val schema = df.schema
  // Modify [[StructField] for the specified columns.
  val newSchema = StructType(schema.map {
    case StructField(c, t, _, m) if columns.contains(c) => StructField(c, t, nullable = false, m)
    case y: StructField => y
  })
  // Apply new schema to the DataFrame
  df.sqlContext.createDataFrame(df.rdd, newSchema)
}
val sftmta_time = sfmta_raw
  .withColumn("REPORT_TIME", to_timestamp($"REPORT_TIME", "MM/dd/yyyy HH:mm:ss"))
val sftmta_prep = setNotNull(sftmta_time, Seq("REPORT_TIME", "VEHICLE_TAG"))
sftmta_prep.printSchema
sftmta_prep.createOrReplaceTempView("sftmta_prep")
spark.sql("SELECT count(*) FROM sftmta_prep").show()
spark.sql("SELECT * FROM sftmta_prep LIMIT 5").show()


import collection.JavaConverters._
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
val kuduContext = new KuduContext("localhost:7051,localhost:7151,localhost:7251", spark.sparkContext)

// Delete the table if it already exists.
if(kuduContext.tableExists("sfmta_kudu")) {
	kuduContext.deleteTable("sfmta_kudu")
}

kuduContext.createTable("sfmta_kudu", sftmta_prep.schema,
  /* primary key */ Seq("REPORT_TIME", "VEHICLE_TAG"),
  new CreateTableOptions()
    .setNumReplicas(3)
    .addHashPartitions(List("VEHICLE_TAG").asJava, 4))




kuduContext.insertRows(sftmta_prep, "sfmta_kudu")
// Create a DataFrame that points to the Kudu table we want to query.
val sfmta_kudu = spark.read
	.option("kudu.master", "localhost:7051,localhost:7151,localhost:7251")
	.option("kudu.table", "sfmta_kudu")
	// We need to use leader_only because Kudu on Docker currently doesn't
	// support Snapshot scans due to `--use_hybrid_clock=false`.
	.option("kudu.scanLocality", "leader_only")
	.format("kudu").load
sfmta_kudu.createOrReplaceTempView("sfmta_kudu")
spark.sql("SELECT count(*) FROM sfmta_kudu").show()
spark.sql("SELECT * FROM sfmta_kudu LIMIT 5").show()




spark.sql("SELECT * FROM sfmta_kudu ORDER BY speed DESC LIMIT 1").show()




spark.sql("SELECT count(*) FROM sfmta_kudu WHERE vehicle_tag = 5411").show()
val toDelete = spark.sql("SELECT * FROM sfmta_kudu WHERE vehicle_tag = 5411")
kuduContext.deleteRows(toDelete, "sfmta_kudu")
spark.sql("SELECT count(*) FROM sfmta_kudu WHERE vehicle_tag = 5411").show()
