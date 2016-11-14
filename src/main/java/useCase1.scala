import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soumyaka on 11/14/2016.
  */
object useCase1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IOT-test")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    Logger.getRootLogger().setLevel(Level.ERROR)

    val dir = "D:\\Users\\soumyaka\\Desktop\\Daimler"


    val listOfDates = getListOfFolder(dir)

    for (date <- listOfDates) {
      val listOfFolders = getListOfFolder(date.getAbsolutePath)
      //      val dateString = date.getName

      for (folder <- listOfFolders) {
        val lineNumber = folder.getName.substring(4, 8) // 8 is exclusive

        val listOfFiles = getListOfFiles(folder.getAbsolutePath)
        listOfFiles.foreach(file => {
          if (file.getName == "BALogFile.txt") {
            val BALogRDD = sc.textFile(file.getAbsolutePath)
            val header = BALogRDD.first()
            val structure = header.split(";;")(0).split(";")
            val structureSize = structure.size



            val rowData = BALogRDD.filter(_ != header)
              .map(_ + "a")
              .map(line => {
                val someArray = line.split(";")
                val slicedArray = someArray.slice(0, someArray.size - 1)
                //                val rowList = new ListBuffer[String]
                var str = ""
                var indexCount = 0
                while (indexCount + structureSize - 1 < slicedArray.size) {
                  str += lineNumber + slicedArray.slice(indexCount, structureSize + indexCount).mkString(";") + ";" + "\n"
                  //                  hc.sql("insert into table values(" + rowList(0),  + ")");
                  indexCount += structureSize
                }
                str
              })
            //              .filter(_ != "\n")
            rowData.saveAsTextFile("D:\\Daimler\\src\\main\\resources\\testOutput\\" + date.getName + "\\" + folder.getName)
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

}
