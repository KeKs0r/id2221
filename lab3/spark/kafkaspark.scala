import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topics = Set("avg")

    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    //val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder(ssc, kafkaConf, topics);
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics);



    val values = messages.map(_._2.split(","))
    val pairs = values.map(x => (x(0), x(1).toDouble));


    def mappingFunc(key: String, value: Option[Double], state: State[Double]): Option[(String, Double)] = {
      val currentState: Double = state.getOption.getOrElse(0D);
      val currentIteration: Double = value.getOrElse(0);
      val sum = currentState + currentIteration;
      val result = (key, sum);
      state.update(sum);
      Some(result);
    }

    val stateSpec = StateSpec.function(mappingFunc _)

    val stateDstream = pairs.mapWithState(stateSpec)

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
