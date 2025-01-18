package com.parquest

import org.apache.spark.sql.SparkSession

object ParquetQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Reader Example")
      .master("local")
      .getOrCreate()
    val parquetFilePath = "file:///D:/lakehouse/bucket-0/"

    val a0 = spark.read.parquet(parquetFilePath+"changelog-9aeecbd6-d633-4f52-8226-90185a0d4f2d-0.parquet")
    val a1 = spark.read.parquet(parquetFilePath+"data-9aeecbd6-d633-4f52-8226-90185a0d4f2d-1.parquet")
    val a2 = spark.read.parquet(parquetFilePath+"changelog-9aeecbd6-d633-4f52-8226-90185a0d4f2d-2.parquet")
    val a3 = spark.read.parquet(parquetFilePath+"data-9aeecbd6-d633-4f52-8226-90185a0d4f2d-3.parquet")
    val a4 = spark.read.parquet(parquetFilePath+"changelog-9aeecbd6-d633-4f52-8226-90185a0d4f2d-4.parquet")
    val a5 = spark.read.parquet(parquetFilePath+"data-9aeecbd6-d633-4f52-8226-90185a0d4f2d-5.parquet")

    //a0.show()
    a1.show()
    //a2.show()
    a3.show()
    //a4.show()
    a5.show()

    // 停止SparkSession
    spark.stop()
  }

}
