import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Soumyakanti on 14-11-2016.
  */
object usecase1And2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("daimler")
      .setMaster("local[*]")
    //      .setMaster("yarn-client")

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    Logger.getRootLogger().setLevel(Level.ERROR)

    // landing directory
    val dir = "D:\\global-coding-env\\Daimler"

    val listOfDates = getListOfFolder(dir)

    //outer date folder in the landing directory
    for (date <- listOfDates) {
      val listOfFolders = getListOfFolder(date.getAbsolutePath)
      //folder within date folders
      for (folder <- listOfFolders) {
        val listOfFiles = getListOfFiles(folder.getAbsolutePath)
        listOfFiles.foreach(file => {
          if (file.getName == "BALogFile.txt") {
            val lineNumber = folder.getName.substring(4, 8) // 8 is exclusive
            val rowData = transformFile(sc, file.getAbsolutePath, lineNumber)
            saveBALogToHive(hc, rowData)

          } else if (file.getName == "DosKonfigLog.txt") {
            val lineNumber = folder.getName.substring(10)
            val rowData = transformFile(sc, file.getAbsolutePath, lineNumber)
            saveDosKonfigLogToHive(hc, rowData)

          }
        })
      }
    }

  }

  def getListOfFolder(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val folders = d.listFiles.filter(_.isDirectory).toList
      folders
    } else {
      List[File]()
    }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val files = d.listFiles.filter(_.isFile).toList
      files
    } else {
      List[File]()
    }
  }

  def transformFile(sc: SparkContext, path: String, lineNumber: String): RDD[String] = {
    val LogRDD = sc.textFile(path)
    val header = LogRDD.first()
    val structureSize = header.split(";;")(0).split(";").size

    val rowData = LogRDD.filter(_ != header)
      .map(_ + "a")
      .map(line => {
        val someArray = line.split(";")
        val slicedArray = someArray.slice(0, someArray.size - 1)
        val rowList = new ListBuffer[String]
        var indexCount = 0
        while (indexCount + structureSize - 1 < slicedArray.size) {
          rowList += lineNumber + slicedArray.slice(indexCount, structureSize + indexCount).mkString(";") + ";"
          indexCount += structureSize
        }
        rowList
      })
      .flatMap(li => li)

    rowData
  }

  def saveBALogToHive(hc: HiveContext, rowData: RDD[String]): Unit = {

    //    val schema = hc.sql("select * from daimler_test_db.DW_PRODUCTION_DOSR_MALFUCTION where 1=2").schema
    val dataRDD = rowData
      .map(line => {
        val Array(lnNum, msgNum, dtTm, dsrID, msgTyp, msgTxt, usr, a) = (line + "a").split(";")
        Row(lnNum, dsrID.toInt, msgNum.toInt, dtTm, msgTyp, msgTxt, usr)
      })
    dataRDD.foreach(println)
    /*val dataDF = hc.createDataFrame(dataRDD, schema) // check if this also works without schema. remove row in front of tuple and remove schema
    dataDF.write.mode(SaveMode.Append).format("orc").save("/apps/hive/warehouse/daimler_test_db.db/dw_production_dosr_malfuction/")*/
  }

  def saveDosKonfigLogToHive(hc: HiveContext, rowData: RDD[String]): Unit = {

    //    val schema = hc.sql("select * from daimler_test_db.DW_PRODUCTION_DOSR_MALFUCTION where 1=2").schema
    val dataRDD = rowData
      .map(line => {
        val Array(lnNum, msgNum, dtTm, dsrID, dsrPrt, paramTyp, oVal, modVal, usr, chngComm, a) = (line + "a").split(";")
        Row(lnNum, dsrID.toInt, msgNum.toInt, dtTm, dsrPrt, paramTyp, oVal.toInt, modVal.toInt, usr, chngComm)
      })
    dataRDD.foreach(println)
    /*val dataDF = hc.createDataFrame(dataRDD, schema) // check if this also works without schema. remove row in front of tuple and remove schema
    dataDF.write.mode(SaveMode.Append).format("orc").save("/apps/hive/warehouse/daimler_test_db.db/dw_production_dosr_malfuction/")*/
  }

}
