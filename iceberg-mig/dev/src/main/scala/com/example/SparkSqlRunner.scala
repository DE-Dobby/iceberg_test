package com.example

import org.apache.spark.sql.SparkSession

object SparkSqlRunner {

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "사용법: SparkSqlRunner \"<SQL>\"")

    val spark = SparkSession.builder().getOrCreate()

    try {
      spark.sql(args(0)).show(truncate = false)
    } finally {
      spark.stop()
    }
  }
}
