package sparksandbox

import java.util.{Date, UUID}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import sparksandbox.configure.JobConfigure


object StreamingKafkaTwoTopics {

  val batchIntervalSeconds = 5
  val batchInterval = Seconds(batchIntervalSeconds)
  val checkpointInterval = Seconds(batchIntervalSeconds * 3)

  def main(args: Array[String]) {

    val ssc = JobConfigure.StreamingContextHDFS(this, batchInterval)
    val sc = ssc.sparkContext

    val kafkaBrokers = JobConfigure.getKafkaBrokers(sc)

    // incoming message. Key is the sender's trigram
    val messagesStream = createKafkaStream(ssc, "messages", kafkaBrokers, randomId = false)
                          .map(e => (e.key, (e.value, new Date())))
    messagesStream.checkpoint(checkpointInterval)

    // key = trigram, value = name
    // should be a compacted topic
    // use random groupId to ensure we always read from the start
    val trigramsStream = createKafkaStream(ssc, "trigrams", kafkaBrokers, randomId = true)
                          .map(line => (line.key, line.value))
    trigramsStream.checkpoint(checkpointInterval)

    resolveTrigramsForMessages(messagesStream, trigramsStream)

    ssc.remember(Minutes(1))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 5 * 1000)
  }


  case class Message(date: Date, msg: String, name: String, uid: String)

  def resolveTrigramsForMessages(messagesStream: DStream[(String, (String, Date))], trigramsStream: DStream[(String, String)]): Unit = {

    def updateState(batchTime: Time, key: String, value: Option[String], state: State[String]): Option[(String, String)] = {
      val line = value.getOrElse("")
      val output = (key, line)
      state.update(line)
      Some(output)
    }

    val stateSpec = StateSpec.function(updateState _)

    val stateStream = trigramsStream.mapWithState(stateSpec)

    stateStream.print()

    val snapshots = stateStream.stateSnapshots()

    messagesStream.leftOuterJoin(snapshots)
      .foreachRDD(rdd => {
        rdd.map(line => {
          val trigram = line._1
          val messageBody = line._2._1
          val senderInfo = line._2._2

          Message(date = messageBody._2,
                  msg = messageBody._1,
                  name = senderInfo.getOrElse("? "+trigram ),
                  uid = trigram)
          })
        .collect()
        .foreach(m => {
          println("[" + m.date + "] " + m.name + ": " + m.msg)
        })
      })
  }

  def createKafkaStream(ssc: StreamingContext, kafkaTopics: String, brokers: String, randomId: Boolean): DStream[ConsumerRecord[String, String]] = {
    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "auto.offset.reset" -> "earliest",
      "group.id" -> ("Message reader" + (if (randomId) UUID.randomUUID().toString else ""))
    )

    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
  }
}
