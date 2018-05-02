package com.hig.epp.sunproc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.slf4j.LoggerFactory
import com.hig.epp.utils.EppUtils

object CalcRenewalYr {

  case class MpdAutoVehicleCRY(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, renewal_yr: Long)
  case class MpdAutoPolicyPeriodCRY(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long,plcyEfdt: Long, originalPlcyEfDt :Long, renewalYr: Long, noOfSixMonthRenewal: Long)

  def main(args: Array[String]) {
    val LOG = LoggerFactory.getLogger("CalcRenewalYr")
    val inDataMAD = Seq(
      new MpdAutoVehicleCRY(42, "PLIC", 182398106, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398107, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398108, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398109, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398110, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398111, -1),
      new MpdAutoVehicleCRY(42, "PLIC", 182398114, -1))

    val inDataMAPP = Seq(
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398106, 20150310, 20050310,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398107, 20150310, 18150310,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398108, 20150310, 20150810,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398109, 20150310, 20120310,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398110, 20150310, 20151410,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398111, 20151810, 20150310,-1,-1),
      new MpdAutoPolicyPeriodCRY(42, "PLIC", 182398112, 20150310, 20150310,-1,-1))

    val sparkConf = new SparkConf().setAppName("CalcDriverAge").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._

    val inDFMAD = inDataMAD.toDF()
    val inDFMAPP = inDataMAPP.toDF()

  
    
    val dtChk = udf(new EppUtils().isValidDate _)
    val validPlcyEftDt = dtChk(col("plcyEfdt"), lit("yyyyMMdd"))
    val validOrigPlcyEftDt = dtChk(col("originalPlcyEfDt"), lit("yyyyMMdd"))
    val dtFmt = udf(new EppUtils().formatDate _)
    val diffPlcyEfDt = months_between(dtFmt(col("plcyEfdt"), lit("yyyyMMdd")), dtFmt(col("originalPlcyEfDt"), lit("yyyyMMdd")))
   
    val outDF = inDFMAPP.withColumn("UpdRenYear", when(validPlcyEftDt and validOrigPlcyEftDt, 
                          when(diffPlcyEfDt/12 > 0 and diffPlcyEfDt/12 <100, diffPlcyEfDt/12).otherwise(lit("0"))).otherwise(lit("0")))
                          .withColumn("UpdRen6Month", when(validPlcyEftDt and validOrigPlcyEftDt, 
                          when(diffPlcyEfDt/6 > 0 and diffPlcyEfDt/6 <100, diffPlcyEfDt/6).otherwise(lit("0"))).otherwise(lit("0")))

    outDF.show()
    
    val out1DF = inDFMAD.join(outDF,inDFMAD.col("riskStateCd") ===outDF.col("riskStateCd")
                               && inDFMAD.col("ratingPlan") ===outDF.col("ratingPlan")
                               && inDFMAD.col("plcyPeriodId") ===outDF.col("plcyPeriodId"),"left_outer" ) 

             out1DF.show()                  
  }
}
