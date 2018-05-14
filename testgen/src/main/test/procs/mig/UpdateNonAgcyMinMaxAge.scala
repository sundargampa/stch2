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

case class UpdateNonAgcyMinMaxAgeMPD(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcyDriverId: Long, plcyVehicleId : BigDecimal, driverAge : Int )
case class UpdateNonAgcyMinMaxAgeMPDAPP(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, youngestDriversAge : Int,oldestDriversAge : Int)

object UpdateNonAgcyMinMaxAge {

  val LOG = LoggerFactory.getLogger("UpdateNonAgcyMinMaxAge")

  def main(args: Array[String]) {
    LOG.info("Started")

   
    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

     val inDataMPD = Seq(
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 194062538,42945899,25),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 194062538,42945899,35),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 194062538,42945899,99),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287488, 194062538,42945899,26),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 194062538,42945899,12),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 0,42945899,2),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287488, 194062538,42945899,56),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287428, 194062538,42945899,81),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287436, 0,42945899,25),
      new UpdateNonAgcyMinMaxAgeMPD(42L, "PLIC", 183287488, 194062538,null,25))

     val inDataMPDAPP = Seq(
        new UpdateNonAgcyMinMaxAgeMPDAPP(42L, "PLIC", 183287428, -1,-1),
        new UpdateNonAgcyMinMaxAgeMPDAPP(42L, "PLIC", 183287424, -1,0),
        new UpdateNonAgcyMinMaxAgeMPDAPP(42L, "PLIC", 183287488, -1,-1),
        new UpdateNonAgcyMinMaxAgeMPDAPP(42L, "PLIC", 183287444, -2,-4)
        ) 
        
    
    val inDFMPD = inDataMPD.toDF()
    LOG.info("DF Schema: {}", inDFMPD.schema)
    val inDFMPDAPP = inDataMPDAPP.toDF()
    
    val isNullPlcyVehicleID = col("plcyVehicleId") === null
    val validDriverId = col("plcyDriverId") > 0

    val outDFMPD = inDFMPD.filter(isNullPlcyVehicleID or validDriverId)
                          .groupBy(col("riskStateCd"), col("ratingPlan"),col("plcyPeriodId"))
                          .agg(min("driverAge") as "minDriverAge", max("driverAge") as "maxDriverAge")
    LOG.info("Out DF Schema: {}", outDFMPD.schema)

    outDFMPD.show()
    
    
    val outMPDAPP = inDFMPDAPP.join(outDFMPD.select(col("riskStateCd") as "OutriskStateCd", col("ratingPlan") as "OutratingPlan", 
        col("plcyPeriodId") as "OutplcyPeriodId", col("minDriverAge"), col("maxDriverAge") ), 
            col("riskStateCd") === col("OutriskStateCd")
          && col("ratingPlan") === col("OutratingPlan")
          && col("plcyPeriodId") === col("OutplcyPeriodId"), "left_outer")
          .withColumn("MinDriverAge", when(col("minDriverAge") > 0,col("minDriverAge")).otherwise(col("youngestDriversAge")))
          .withColumn("MaxDriverAge", when(col("maxDriverAge") > 0  ,col("maxDriverAge")).otherwise(col("oldestDriversAge")))
    
    outMPDAPP.show()
    
    LOG.info("Ended")

  }
}
