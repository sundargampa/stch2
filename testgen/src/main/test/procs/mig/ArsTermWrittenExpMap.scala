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
import org.apache.spark.sql.types.{ DateType, DataType, FloatType, LongType, StringType, StructType, TimestampType, IntegerType, DoubleType, BinaryType }

import java.sql.Date
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils

object ArsTermWrittenExpMap {

  val LOG = LoggerFactory.getLogger("ArsTermWrittenExpMap")

  def main(args: Array[String]) {
    LOG.info("Started")

    val mpdAPASchema = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("originalWrittenExposure", DoubleType, false))
      .add(new StructField("originalEarnedExposure", DoubleType, false))
      .add(new StructField("accountingDt", IntegerType, false))

    val mpdAPPSchema = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
       .add(new StructField("plcyTermMonthCnt", IntegerType, false))

    val inDataMPDAPA = Seq(
      Row(42L, "PLIC", 19431203L, 19431222L, 55, "0".toDouble, "0.035".toDouble, 20150601),
      Row(42L, "PLIC", 19431200L, 19431222L, 55, "0".toDouble, "0.036".toDouble,20150601),
      Row(42L, "PLIC", 19431211L, 19431222L, 66, "0.25".toDouble, "0.037".toDouble,20150601),
      Row(42L, "PLIC", 19431222L, 19431222L, 77, "0.75".toDouble,"0.038".toDouble, 20150202),
      Row(42L, "PLIC", 19431222L, 19431222L, 77, "0.75".toDouble,"0.039".toDouble, 20160202),
      Row(42L, "PLIC", 19431222L, 19431222L, 77, "0.75".toDouble,"0.040".toDouble, 20170202),
      Row(42L, "PLIC", 19431233L, 19431222L, 77, "0.75".toDouble,"0.041".toDouble, 20150202),
      Row(42L, "PLIC", 19431244L, 19431222L, 88, "1.2".toDouble, "0.042".toDouble,20151231),
      Row(42L, "PLIC", 19431244L, 19431222L, 88, "1.2".toDouble, "0.043".toDouble,20150131))

    val inDataMPDAPP = Seq(
      Row(42L, "PLIC", 19431203L, 3),
      Row(42L, "PLIC", 19431200L, 6),
      Row(42L, "PLIC", 19431211L, 6),
      Row(42L, "PLIC", 19431222L, 6),
      Row(42L, "PLIC", 19431233L, 12),
      Row(42L, "PLIC", 19431244L, 12))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDataMPDAPArdd = sparkContext.parallelize(inDataMPDAPA)
    val inDataMPDAPADF = sqlContext.createDataFrame(inDataMPDAPArdd, mpdAPASchema)

    val inDataMPDAPPrdd = sparkContext.parallelize(inDataMPDAPP)
    val inDataMPDAPPDF = sqlContext.createDataFrame(inDataMPDAPPrdd, mpdAPPSchema)

    /*
     * min_acc_a :   mpd_auto_premium_amount : risk_state_cd,rating_plan, plcy_period_id, plcy_vehicle_id, coverage_type_id, original_written_exposure
     * min_acc_b :   mpd_auto_policy_period : risk_state_cd,rating_plan, plcy_period_id,plcy_term_month_cnt
  		 min_acc JOIN min_acc_a & min_acc_b on risk_state_cd, rating_plan,plcy_period_id
  		  min_acc Filter : original_written_exposure != 0 & ((b.plcy_term_month_cnt = 6  and abs(a.original_written_exposure) > .5) or '||
  												' (b.plcy_term_month_cnt = 12 and abs(a.original_written_exposure) > 1))
  			Then group by a.plcy_period_id, a.plcy_vehicle_id, a.coverage_type_id, b.plcy_term_month_cnt									
     * 
     */

    val minaccJoinDF = inDataMPDAPPDF.join(inDataMPDAPADF, Seq("riskStateCd","ratingPlan","plcyPeriodId"))
        
      /*inDataMPDAPADF.col("riskStateCd") === inDataMPDAPPDF.col("riskStateCd")
        && inDataMPDAPADF.col("ratingPlan") === inDataMPDAPPDF.col("ratingPlan")
        && inDataMPDAPADF.col("plcyPeriodId") === inDataMPDAPPDF.col("plcyPeriodId"))
*/
    minaccJoinDF.show()
   
    val nonZeroOrigWritExpo = col("originalWrittenExposure") > 0
    val gtP5OrigWritExpo = abs(col("originalWrittenExposure")) > 0.5 
    val gtOneOrigWritExpo = abs(col("originalWrittenExposure")) > 1
    val eq6PlcyTermMonthCnt = col("plcyTermMonthCnt") === 6
    val eq12PlcyTermMonthCnt = col("plcyTermMonthCnt") === 12
    
    val minaccFilterDF = minaccJoinDF.filter(nonZeroOrigWritExpo and 
        ((eq6PlcyTermMonthCnt and gtP5OrigWritExpo) or (eq12PlcyTermMonthCnt and gtOneOrigWritExpo) ) )
        
        minaccFilterDF.show()
        
     val minaccGrpByDF = minaccFilterDF.groupBy(col("riskStateCd"),col("ratingPlan"),col("plcyPeriodId")
         ,col("plcyVehicleId"),col("coverageTypeId"),col("plcyTermMonthCnt")).agg(min("accountingDt") as "minaccountingDt") 
     
     minaccGrpByDF.show()
    
     val BjoinMinaccMAPA = inDataMPDAPADF.join(minaccGrpByDF,
           inDataMPDAPADF.col("riskStateCd") === minaccGrpByDF.col("riskStateCd")
        && inDataMPDAPADF.col("ratingPlan") === minaccGrpByDF.col("ratingPlan")
        && inDataMPDAPADF.col("plcyPeriodId") === minaccGrpByDF.col("plcyPeriodId")
        && inDataMPDAPADF.col("plcyVehicleId") === minaccGrpByDF.col("plcyVehicleId")
        && inDataMPDAPADF.col("coverageTypeId") === minaccGrpByDF.col("coverageTypeId")
        && inDataMPDAPADF.col("accountingDt") === minaccGrpByDF.col("minaccountingDt")
        )
        .drop(minaccGrpByDF.col("riskStateCd"))
        .drop(minaccGrpByDF.col("ratingPlan"))
        .drop(minaccGrpByDF.col("plcyPeriodId"))
        .drop(minaccGrpByDF.col("plcyVehicleId"))
        .drop(minaccGrpByDF.col("coverageTypeId"))
  
        BjoinMinaccMAPA.show()
        
        val chkPlcyTermMCnt = col("plcyTermMonthCnt") === 6
        //val p5ExpFact = "0.5".toDouble / col("originalWrittenExposure")
        
        val calcExpoFact = BjoinMinaccMAPA.withColumn("expoFact", when(chkPlcyTermMCnt, abs(lit("0.5").divide(col("originalWrittenExposure"))))
                .otherwise(abs(lit("1").divide(col("originalWrittenExposure")))))
        
        calcExpoFact.show()
        
       val updMAPPDF = calcExpoFact.withColumn("WrittenExposure", col("originalWrittenExposure") * col("expoFact"))
                                   .withColumn("EarnedExposure", col("originalEarnedExposure") * col("expoFact")) 
          updMAPPDF.show()                         
    LOG.info("Ended")

  }
}
