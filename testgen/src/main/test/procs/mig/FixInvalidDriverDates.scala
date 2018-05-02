package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import scala.collection.Parallelizable
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

case class FixInvalidDriverDates(riskStateCd: Long, ratingPlan: String, driverBirthDate: Int, driverLicenseDate: Int)

object FixInvalidDriverDates {

  val LOG = LoggerFactory.getLogger("FixInvalidDriverDates")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new FixInvalidDriverDates(42L, "PLIC", 19430803, 19563425),
      new FixInvalidDriverDates(42L, "PLIC", 19431503, 19591203),
      new FixInvalidDriverDates(42L, "PLIC", 19431235, 19590827),
      new FixInvalidDriverDates(42L, "PLID", 19430803, 19590831),
      new FixInvalidDriverDates(84L, "PLIC", 19432503, 19590856),
      new FixInvalidDriverDates(84L, "PLIC", 19430844, 19591403),
      new FixInvalidDriverDates(42L, "PLIC", 19430810, 19590803),
      new FixInvalidDriverDates(84L, "PLID", 19430831, 19591531),
      new FixInvalidDriverDates(42L, "PLID", 19432232, 19591835),
      new FixInvalidDriverDates(84L, "PLID", 19430803, 19592178))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    // Get records from mpd_auto_driver with invalid driver_birth_dt &driver_licensed_dt
    // For  driver_birth_dt  -month > 12 update to 06
    // For  driver_birth_dt  -day > 31 update to 28
    // For  driver_licensed_dt  -month > 12 update to 06
    // For  driver_licensed_dt  -day > 31 update to 28
    val inValidDriverMonth = col("driverBirthDate").substr(5, 2) + 0 > 12
    val inValidDriverDay = col("driverBirthDate").substr(7, 2) + 0 > 31
    val inValidLicenseMonth = col("driverLicenseDate").substr(5, 2) + 0 > 12
    val inValidLicenseDay = col("driverLicenseDate").substr(7, 2) + 0 > 31

    val driverBirthYear = col("driverBirthDate").substr(0, 4)
    val driverBirthDay = col("driverBirthDate").substr(7, 2)
    val updDriverBirth1TillMonth = col("UpdDriverBirthDate1").substr(0, 6)
    
    val driverLicenseYear = col("driverLicenseDate").substr(0, 4)
    val driverLicenseDay = col("driverLicenseDate").substr(7, 2)
    val updDriverLicense1TillMonth = col("UpdDriverLicenseDate1").substr(0, 6)

    val OutDF = inDF.withColumn("UpdDriverBirthDate1", when(inValidDriverMonth, concat(driverBirthYear, lit("06"), driverBirthDay)).otherwise(col("driverBirthDate")))
                    .withColumn("UpdDriverBirthDate2", when(inValidDriverDay,concat(updDriverBirth1TillMonth,lit("28")) ).otherwise(col("UpdDriverBirthDate1")))
                    .withColumn("UpdDriverLicenseDate1", when(inValidLicenseMonth, concat(driverLicenseYear, lit("06"),driverLicenseDay)).otherwise(col("driverLicenseDate")))
                    .withColumn("UpdDriverLicenseDate2", when(inValidLicenseDay, concat(updDriverLicense1TillMonth,lit("28"))).otherwise(col("UpdDriverLicenseDate1")))
    LOG.info("Out DF Schema: {}", OutDF.schema)

    OutDF.show()
    LOG.info("Ended")

  }
}
