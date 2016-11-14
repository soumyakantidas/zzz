import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * Created by soumyaka on 11/10/2016.
  */
object fileTransformations {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setAppName("IOT-test")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)

    Logger.getRootLogger().setLevel(Level.ERROR)

    val dir = "D:\\global-coding-env\\Daimler"


    val listOfDates = getListOfFolder(dir)

    for (date <- listOfDates){
      val listOfFolders = getListOfFolder(date.getAbsolutePath)
//      val dateString = date.getName

      for(folder <- listOfFolders){
        val lineNumber = folder.getName.substring(4,8) // 8 is exclusive

        val listOfFiles = getListOfFiles(folder.getAbsolutePath)
        listOfFiles.foreach(file => {
          if(file.getName == "BALogFile.txt"){
            val BALogRDD = sc.textFile(file.getAbsolutePath)
            val header = BALogRDD.first()
            /*val headerLength = header.split(";").size
            val RDDLength = (BALogRDD.count() - 1).toInt*/
            /*val currRow = new AtomicInteger(0)
            val currCol = new AtomicInteger(0)

            val masterArray = ArrayBuffer.fill(headerLength, RDDLength)("")*/

            /* val rowData = BALogRDD.filter(_ != header)
               .map(_ + "a")
               .map(line => {
                 val someArray = line.split(";")
                 val slicedArray = someArray.slice(0, someArray.size - 1)
                 slicedArray
               }).collect()
               /*.map(arr => {
                 while

               })*/*/

            val structureSize = header.split(";;")(0).split(";").size





            val rowData = BALogRDD.filter(_ != header)
              .map(_ + "a")
              .map(line => {
                val someArray = line.split(";")
                val slicedArray = someArray.slice(0, someArray.size - 1)
                val rowList = new ListBuffer[String]
                var indexCount = 0
                while(indexCount + structureSize - 1 < slicedArray.size){
                  rowList += lineNumber + slicedArray.slice(indexCount, structureSize + indexCount).mkString(";") + ";"

//                  hc.sql("insert into table values(" + rowList(0),  + ")");


                  indexCount += structureSize
                }
                rowList
              })
              .flatMap(li => li)
              .map(line => {
                val Array(lnNum, msgNum, dtTm, dsrID, msgTyp, msgTxt, usr, a) = (line + "a").split(";")
                (lnNum, dsrID.toInt, msgNum.toInt, dtTm, msgTyp, msgTxt, usr)
              })

            /*.map(listB => {

            })*/
            //            rowData.saveAsTextFile("D:\\global-coding-env\\IdeaProjects\\Daimler\\src\\main\\resources\\testOutput\\" + date.getName + "\\" + folder.getName)
            rowData.foreach(println)


            /*val BALogRDDData = BALogRDD.filter(_ != header)
            val splitData = BALogRDDData.map(line => {
              val array = line.split(";")

            })*/

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
