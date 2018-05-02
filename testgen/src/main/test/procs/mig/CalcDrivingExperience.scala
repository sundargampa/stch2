package com.hig.epp.sunproc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.slf4j.LoggerFactory
import com.hig.epp.utils.EppUtils

object CalcDrivingExperience {

  case class MpdAutoDriverCDA(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, driverBirthDt: Long, driverLicenseDt: Long, drivingExp: Int)
  case class MpdAutoPolicyPeriodCDA(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcy_efdt: Long, versionDt: Long)

  def main(args: Array[String]) {
    val LOG = LoggerFactory.getLogger("CalcDrivingExperience")
    val inDataMAD = Seq(
      new MpdAutoDriverCDA(42, "PLIC", 182398106, 19350804, 20170804, 1),
      new MpdAutoDriverCDA(42, "PLIC", 170215263, 19600808, 19510804, 1),
      new MpdAutoDriverCDA(42, "PLIC", 170215265, 19550504, 20170804, 1),
      new MpdAutoDriverCDA(42, "PLIC", 170215266, 11, 19760808, 1),
      new MpdAutoDriverCDA(42, "PLIC", 170215267, 19730419, 22, 1),
      new MpdAutoDriverCDA(42, "PLIC", 182398108, 19730419, 20170804, 1),
      new MpdAutoDriverCDA(42, "PLID", 182398106, 19730435, 19910920, 1))

    val inDataMAPP = Seq(
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 182398106, 20150310, 20150310),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 170215263, 20150310, 33),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 170215265, 20150310, 20151410),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 170215266, 20150310, 20150310),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 182398108, 44, 20150310),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 170215267, 20150310, 20150310),
      new MpdAutoPolicyPeriodCDA(42, "PLIC", 182398107, 20150310, 20153421))

    val sparkConf = new SparkConf().setAppName("CalcDriverAge").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._

    val inDFMAD = inDataMAD.toDF()
    val inDFMAPP = inDataMAPP.toDF()

    val inDrLicDtGTPlcyDt = col("driverLicenseDt") > col("plcy_efdt")
    
    val dtChk = udf(new EppUtils().isValidDate _)
    val validVersionDt = dtChk(col("versionDt"), lit("yyyyMMdd"))
    val validDriverLicDt = dtChk(col("driverLicenseDt"), lit("yyyyMMdd"))
    val validPlcyEfDt = dtChk(col("plcy_efdt"), lit("yyyyMMdd"))
    val dtFmt = udf(new EppUtils().formatDate _)
    val diffVersionLicDt = datediff(dtFmt(col("versionDt"), lit("yyyyMMdd")), dtFmt(col("driverLicenseDt"), lit("yyyyMMdd")))
    val diffPlcyEfLicDt = datediff(dtFmt(col("plcy_efdt"), lit("yyyyMMdd")), dtFmt(col("driverLicenseDt"), lit("yyyyMMdd")))

    val outDF = inDFMAD.join(inDFMAPP, inDFMAD.col("riskStateCd") === inDFMAPP.col("riskStateCd")
      && inDFMAD.col("ratingPlan") === inDFMAPP.col("ratingPlan")
      && inDFMAD.col("plcyPeriodId") === inDFMAPP.col("plcyPeriodId"), "left_outer")
      .withColumn("UpdDrivingExp", when(inDrLicDtGTPlcyDt, when(validVersionDt and validDriverLicDt, diffVersionLicDt).otherwise(lit("0")))
                                 .when(validPlcyEfDt and validDriverLicDt, diffPlcyEfLicDt).otherwise(lit("0")))
    outDF.show()

  }
}
