import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.types._

import org.apache.kudu.spark.kudu.KuduContext

val query = """
  (select * from product where id < 'item_000100') product
"""
val options = Map(
    "url"              -> "jdbc:mysql://db_mysql/lucas_test",
    "user"             -> "root",
    "password"         -> "secret",
    "dbtable"          -> query,
)

val nearData = spark.read.format("jdbc").options(options).load()

val allQuery = "product";

val allOptions = Map(
    "url"              -> "jdbc:mysql://db_mysql/lucas_test",
    "user"             -> "root",
    "password"         -> "secret",
    "dbtable"          -> query,
)

val allData = spark.read.format("jdbc").options(allOptions).load()

val nearData = allData.filter($"id" < "item_000100")
nearData.show()
val kuduContext = new KuduContext(" kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", spark.sparkContext)
val nearDataKuduTableName = "nearData"

if(kuduContext.tableExists(nearDataKuduTableName)) {
  kuduContext.deleteTable(nearDataKuduTableName)
}

val resultSchema = StructType(
  List(
    StructField("id", StringType, false),
    StructField("name", StringType, false),
    StructField("timestamp", TimestampType, false)
  )
)

import scala.collection.JavaConverters._
import org.apache.kudu.client.CreateTableOptions
kuduContext.createTable(nearDataKuduTableName,
  resultSchema, // Kudu schema with PK columns set as Not Nullable
  Seq("id"), // Primary Key Columns
  new CreateTableOptions().
    setNumReplicas(3).
    addHashPartitions(List("id").asJava, 2))
kuduContext.insertRows(nearData, nearDataKuduTableName)

val nearDataKuduDf = spark.read.option("kudu.master", "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251"). option("kudu.table", nearDataKuduTableName). option("kudu.scanLocality", "leader_only"). format("kudu"). load

nearDataKuduDf.show



