package io.saagie.devoxx.ma

import io.saagie.devoxx.ma.model.Quote
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.slf4j.LoggerFactory
import scopt.OptionParser


/**
  * Created by aurelien on 27/10/16.
  */
case class Config(
                   batchDuration: Long = 10l,
                   // HDFS configuration
                   hdfsNameNodeHost: String = "",
                   hdfsPathCheckpoint: String = "",
                   hdfsPathQuotes: String = "",
                   // kafka configuration
                   kafkaBrokerUrl: String = "",
                   kafkaTopic: String = "quotes"
                 )

object QuoteInjector {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val appName: String = getClass.getSimpleName

    val parser = parseArgs(appName)
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>

        val brokers = config.kafkaBrokerUrl
        val topics = Set(config.kafkaTopic)
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        val checkpointDirectoryPath: String = config.hdfsNameNodeHost + config.hdfsPathCheckpoint

        val outputPath: String = config.hdfsNameNodeHost + config.hdfsPathQuotes


        val streamingContext = StreamingContext.getOrCreate(checkpointDirectoryPath,
          () => {

            val ssc = createContext(appName, checkpointDirectoryPath, config.batchDuration)
            val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
            val ratingsStream: InputDStream[(String, String)] = ??? // TODO create Kafka stream

            val msgs: DStream[Quote] = ratingsStream.transform {
              (message: RDD[(String, String)], batchTime: Time) => {
              ??? // TODO parse data in model object quote
              }
            }

            ???  // TODO save each RDD in parquet with SaveMode.Append

            ssc
          })

        streamingContext.start() // Start the computation
        streamingContext.awaitTermination() // Wait for the computation to terminate
      case None =>
        // arguments are bad, error message will have been displayed
        logger.debug("Error in run arguments : {}", args)
    }
  }



  def createContext(appName: String, checkpointDirectoryPath: String,  batchDuration: Long): StreamingContext = {

    val sparkConf = new SparkConf()
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .setAppName(appName)

    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
    ssc.checkpoint(checkpointDirectoryPath)
    ssc
  }


  def parseArgs(appName: String): OptionParser[Config] = {
    new OptionParser[Config](appName) {
      head(appName, "version")
      help("help") text """prints this usage text"""
      /* Batch configuration */
      opt[String]("batchDuration") required() action { (data, conf) =>
        conf.copy(batchDuration = data.toLong)
      } text "Batch duration in second. Example : 600 (for 10 min)"
      /* HDFS configuration */
      opt[String]("hdfsNameNodeHost") required() action { (data, conf) =>
        conf.copy(hdfsNameNodeHost = data)
      } text "URI of hdfs nameNode. Example : hdfs://IP:8020"

      opt[String]("hdfsPathCheckpoint") required() action { (data, conf) =>
        conf.copy(hdfsPathCheckpoint = data)
      } text "Directory path where the checkpoiting data are writen. Example: /user/spark/checkpointing/injector "

      opt[String]("hdfsPathQuotes") required() action { (data, conf) =>
        conf.copy(hdfsPathQuotes = data)
      } text "Directory path where the quotes data are writen : Example: /user/hdfs/quotes"

      opt[String]("kafkaBrokerUrl") required() action { (data, conf) =>
        conf.copy(kafkaBrokerUrl = data)
      } text "Url of kafka broker : Example: localhost:9092"

      opt[String]("kafkaTopic") required() action { (data, conf) =>
        conf.copy(kafkaTopic = data)
      } text "Name of your quote topics"

    }
  }

}
