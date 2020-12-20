package com.lucas;
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.URI

object MyApp {
  def main(args: Array[String]) {
    val argsLine = args.mkString(" ")
    println(s"args: $argsLine");

    /*
    val configuration = new Configuration();
    val fs = FileSystem.get(new URI("hdfs://namenode:8020"), configuration);
    val filePath = new Path("hdfs://namenode:8020/tmp/myapp/README.md");
    val fsDataInputStream = fs.open(filePath);
    val br = new BufferedReader(new InputStreamReader(fsDataInputStream));
    val str = Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n")
    println(str)
    br.close()
    */


    val spark = SparkSession.builder.appName("My Application1").getOrCreate()
    val logData = spark.read.textFile("hdfs://namenode:8020/tmp/myapp/README.md").cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
