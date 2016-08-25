package edu.buaa.spark

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser


/**
  * Created by Administrator on 2016/8/22.
  */

/**
  * WordCount
  */


object WordCount {

  case class Params(
                     input: String = null,
                     output: String = null,
                     time: String = null
                   )


  def main(args: Array[String]) {

    val parser = new OptionParser[Params]("Etl for mydata") {
      head("Etl for mydata")
      arg[String]("<input path>")
        .required()
        .text("the input path")
        .action((x, c) => c.copy(input = x))
      arg[String]("<output>")
        .required()
        .text("the output path")
        .action((x, c) => c.copy(output = x))
      arg[String]("<time>")
        .required()
        .text("the extract day time")
        .action((x, c) => c.copy(time = x))
    }

    parser.parse(args, Params()).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }


    def run(params: Params): Unit = {
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName +params.time).setMaster("local")
      println(this.getClass.getSimpleName)
      val sc = new SparkContext(conf)
      val output = params.output + "/" + params.time
      val slice = sc.textFile(params.input).flatMap(_.split(" ")).map{word => (word, 1)}.reduceByKey(_ + _).map(pair => (pair._2, pair._1)).sortByKey(false, 1).map(pair => (pair._2, pair._1))

      slice.foreach(println)
//      slice.saveAsTextFile(output)
      sc.stop()
    }

  }

}

//object learning2 {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("learning2").setMaster("local")
//    val sc = new SparkContext(conf)
//
////    val list = List("ss", "rdd", "egerg", 324, 123)
////    val r = sc.makeRDD(list, 1)
////    r.saveAsObjectFile("E:\\data\\serializeFIle\\listout")
//
//    val value = sc.sequenceFile("E:\\data\\serializeFIle\\input", classOf[LongWritable], classOf[BytesWritable]).map(x =>{
//
//      val firstBytes = Array[Byte](64)
//      System.arraycopy(x._2.getBytes,0,firstBytes,0,8)
//      val firststr = new String(firstBytes)
//    } )
//  }
//
//
//}