import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.types._

val connectionProperties = new java.util.Properties();
connectionProperties.put("user", "root");
connectionProperties.put("password", "secret");
val df = spark.read.jdbc("jdbc:mysql://db_mysql", "lucas_test.product", connectionProperties)
// Looks the schema of this DataFrame.
df.printSchema()

// Counts people by age
val countsByAge = df.groupBy("name").count()
countsByAge.show()

//组装结果RDD
val arr = new Array[String](1000)
for (i<-0 to (arr.length -1 )){
  arr(i) = f"item_$i%06d"
}

val arrayRDD = sc.parallelize(arr)
//将结果RDD映射到rowRDD
val resultRowRDD = arrayRDD.map(p =>Row(
  p,
  "name_"+ p,
  new Timestamp(new java.util.Date().getTime)
))
//通过StructType直接指定每个字段的schema
val resultSchema = StructType(
  List(
    StructField("id", StringType, true),
    StructField("name", StringType, true), //是哪一天日志分析出来的结果
    StructField("timestamp", TimestampType, true) //分析结果的创建时间
  )
)
//组装新的DataFrame
val DF = spark.createDataFrame(resultRowRDD,resultSchema)
//将结果写入到Mysql
DF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://db_mysql:3306", "lucas_test.product", connectionProperties)

