package com.hig.epp.utils

import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.hive._

class EppUtils extends Serializable {

  val LOG = LoggerFactory.getLogger("EppUtils")

  def isValidDate(inDate: String, inDateFmt: String): Boolean = {
    var validStatus = false
    LOG.info("isValidDate {}- {}", Array(inDate, inDateFmt): _*)
    val dtFmt = new SimpleDateFormat(inDateFmt)
    dtFmt.setLenient(false)

    try {
      if (inDate.length() < 8) {
        validStatus = false
        LOG.error("Less number Date {}", inDate)
      } else {
        dtFmt.parse(inDate)
        validStatus = true

      }

    } catch {
      case e: Exception => {
        LOG.error("Invalid Date {}", inDate)
      }

    }

    validStatus

  }

  def formatDate(inDate: String, inDateFmt: String): String = {

    var outDate = "0"
    LOG.info("formatDate {}- {}", Array(inDate, inDateFmt): _*)
    
    if (isValidDate(inDate, inDateFmt)) {
      
      val dtFmt = new SimpleDateFormat(inDateFmt)
      dtFmt.setLenient(false)

      val outDtFmt = new SimpleDateFormat("yyyy-MM-dd")
      outDtFmt.setLenient(false)

      outDate = outDtFmt.format(dtFmt.parse(inDate))
      LOG.info("Fmt Date : {}", outDate)

    }
    outDate

  }

}
