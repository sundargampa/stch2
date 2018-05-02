package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import scala.collection.Parallelizable
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

import java.sql.Date
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils

case class CalcDefaultDrvrLicensedDt(riskStateCd: Long, ratingPlan: String, driverBirthDate: Int, driverLicenseDate: Int)

object CalcDefaultDrvrLicensedDt {

  val LOG = LoggerFactory.getLogger("Calc_Default_Drvr_Licensed_Dt")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new CalcDefaultDrvrLicensedDt(42L, "PLIC", 19431203, 19563425),
      new CalcDefaultDrvrLicensedDt(42L, "PLIC", 19431203, 0),
      new CalcDefaultDrvrLicensedDt(42L, "PLIC", 19431435, 19591627),
      new CalcDefaultDrvrLicensedDt(42L, "PLID", 19430803, 19590831),
      new CalcDefaultDrvrLicensedDt(84L, "PLIC", 19431135, 0),
      new CalcDefaultDrvrLicensedDt(84L, "PLIC", 19430844, 19591403),
      new CalcDefaultDrvrLicensedDt(42L, "PLIC", 19430810, 19590803),
      new CalcDefaultDrvrLicensedDt(84L, "PLID", 19430831, 19591531),
      new CalcDefaultDrvrLicensedDt(42L, "PLID", 19431132, 19591835),
      new CalcDefaultDrvrLicensedDt(84L, "PLID", 19430803, 19592178))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    // Get records from mpd_auto_driver with invalid driver_birth_dt &driver_licensed_dt
    // For  driver_birth_dt is valid AND (License date null or License Date Invalid) Add 16 years to Birthdate and set as License Date

    val driverBirthDate = col("driverBirthDate").cast(StringType)

    val dtChk = udf(new EppUtils().isValidDate _)
    val isValidBirthDate = dtChk(col("driverBirthDate"), lit("yyyyMMdd"))
    val isValidLicenseDate = dtChk(col("driverLicenseDate"), lit("yyyyMMdd"))
    val isNullLicenseDate = col("driverLicenseDate") === null
    val fmtLicenseDate = col("driverBirthDate") + 160000

    val OutDF = inDF.withColumn("UpdLicenseDt", when((isNullLicenseDate or !isValidLicenseDate) and isValidBirthDate, fmtLicenseDate).otherwise(col("driverLicenseDate").cast(StringType)))

    LOG.info("Out DF Schema: {}", OutDF.schema)

    OutDF.show()
    LOG.info("Ended")

  }
}
