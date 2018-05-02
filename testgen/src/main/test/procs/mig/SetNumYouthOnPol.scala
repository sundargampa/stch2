package com.hig.epp.sunproc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.slf4j.LoggerFactory

object SetNumYouthOnPol {

  case class SetNumYouthOnPolGrp(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcyDriverId: Long, driverAge: Int)
  case class SetNumYouthOnPol(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcyDriverId: Long, driverAge: Int,
    youthfulExists: String, ratedYouthDriverCnt: Long)

  def main(args: Array[String]) {
    val LOG = LoggerFactory.getLogger("SetNumYouthOnPol")
    val inData = Seq(
      new SetNumYouthOnPol(42, "PLIC", 240132159, 240132159, 10, "Y", 0),
      new SetNumYouthOnPol(42, "PLIC", 240132200, 19431200, 10, "Y", 0),
      new SetNumYouthOnPol(42, "PLIC", 240132200, 19431200, 10, "Y", 0),
      new SetNumYouthOnPol(42, "PLIC", 240132159, 240132159, 10, "Y", 0),
      new SetNumYouthOnPol(42, "PLID", 240132200, 19431200, 10, "Y", 0),
      new SetNumYouthOnPol(42, "PLIC", 240132159, 240132159, 10, "N", 0))

    val inDataGrp = Seq(
      new SetNumYouthOnPolGrp(42, "PLIC", 240132159, 19431200, 22),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132200, 19431200, 30),
      new SetNumYouthOnPolGrp(42, "PLIC", 0, 19431200, 22),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132159, 0, 23),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132159, 19431200, 20),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132200, 19431200, 12),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132159, 19431200, 20),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132200, 0, 10),
      new SetNumYouthOnPolGrp(42, "PLIC", 240132200, 19431200, 25))

      val sparkConf = new SparkConf().setAppName("Youtful").setMaster("local[2]").set("spark.executor.memory", "1g")
      val sparkContext = new SparkContext(sparkConf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    
       import sqlContext.implicits._
       
       val inDataDFGrp = inDataGrp.toDF()
       val inDataDF = inData.toDF()
       
       val validPlcyPeriodId = col("plcyPeriodId") > 0
       val validPlcyDriverId = col("plcyDriverId") > 0
       val inRangeDriverAge = col("driverAge") > 0 and  col("driverAge") < 25 
       
       val outDataDFGrp = inDataDFGrp.filter(validPlcyPeriodId and validPlcyDriverId and inRangeDriverAge).groupBy(col("riskStateCd"),col("ratingPlan"),col("plcyPeriodId")).count()
       outDataDFGrp.show()
       LOG.info("outDataGrp Schema {}",outDataDFGrp.schema)
      
       val youthFulExists = col("youthfulExists") === "Y"
       
       val outDataDF = inDataDF.join(outDataDFGrp
           .select(col("riskStateCd")as "GrpriskStateCd",col("ratingPlan") as "GrpratingPlan",col("plcyPeriodId") as "GrpplcyPeriodId", col("count") as "Cnt") ,
           col("riskStateCd") === col("GrpriskStateCd") &&
           col("ratingPlan") === col("GrpratingPlan") &&
           col("plcyPeriodId") === col("GrpplcyPeriodId"), "left_outer")
           .drop(col("GrpriskStateCd")).drop(col("GrpratingPlan")).drop(col("GrpplcyPeriodId"))
           .withColumn("UpdRatedYouthCnt", when(youthFulExists, col("Cnt")).otherwise(col("ratedYouthDriverCnt")))
           
           outDataDF.show()
       
  }
}
