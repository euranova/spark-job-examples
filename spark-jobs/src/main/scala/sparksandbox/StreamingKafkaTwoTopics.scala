package sparksandbox

import java.util.Date

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
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
    val messagesStream = JobConfigure.createKafkaStream(ssc, "messages", kafkaBrokers, randomId = false)
                          .map(e => (e.key, (e.value, new Date())))
    messagesStream.checkpoint(checkpointInterval)

    // key = trigram, value = name
    // should be a compacted topic
    // use random groupId to ensure we always read from the start
    val trigramsStream = JobConfigure.createKafkaStream(ssc, "trigrams", kafkaBrokers, randomId = true)
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

}
