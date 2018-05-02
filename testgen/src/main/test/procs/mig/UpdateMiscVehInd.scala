package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import scala.collection.Parallelizable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

case class MiscInd(riskStateCd: Long, ratingPlan: String, ratingClassCd: String, miscVehInd: String)

object UpdateMiscVehInd {

  val LOG = LoggerFactory.getLogger("LocalDF")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new MiscInd(42L, "PLIC", "950000", "S"),
      new MiscInd(42L, "PLIC", "870000", "S"),
      new MiscInd(42L, "PLIC", "970000", "S"),
      new MiscInd(42L, "PLID", "970000", "S"),
      new MiscInd(84L, "PLIC", "970000", "S"),
      new MiscInd(84L, "PLIC", "870000", "S"),
      new MiscInd(42L, "PLIC", "770000", "S"),
      new MiscInd(84L, "PLID", "870000", "S"),
      new MiscInd(42L, "PLID", "960000", "S"),
      new MiscInd(84L, "PLID", "770000", "S"))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    val riskStateCdResult = col("riskStateCd") === 42L
    val ratingPlanResult = col("ratingPlan") === ("PLIC")
    val ratingClassCd9Result = col("ratingClassCd").substr(0, 1) === "9"
    val ratingClassCd7Result = col("ratingClassCd").substr(0, 1) === "7"

    val o1DF = inDF.withColumn("updMiscVehInd", when((riskStateCdResult && ratingPlanResult && ratingClassCd9Result) || (ratingClassCd9Result || ratingClassCd7Result), "Y").otherwise(col("miscVehInd")))
    o1DF.show()
    LOG.info("Ended")
  
  }
}
