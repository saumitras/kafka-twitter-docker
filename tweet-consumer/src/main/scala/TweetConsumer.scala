import java.util.Properties

import kafka.serializer.DefaultDecoder
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}

object TweetConsumer extends App {

  val ZK_HOST = "zookeeper:2181" //change it to localhost:2181 if not connecting through docker
  val TOPIC = "tweets"

  private val props = new Properties()
  props.put("group.id", "tweet-consumer")
  props.put("zookeeper.connect", ZK_HOST)
  props.put("auto.offset.reset", "smallest")
  props.put("consumer.timeout.ms", "120000")
  props.put("zookeeper.connection.timeout.ms","20000")
  props.put("auto.commit.interval.ms", "10000")

  private val consumerConfig = new ConsumerConfig(props)
  private val consumerConnector = Consumer.create(consumerConfig)
  private val filterSpec = new Whitelist(TOPIC)

  def read() = try {
    val streams = consumerConnector.createMessageStreamsByFilter(filterSpec, 1,
      new DefaultDecoder(), new DefaultDecoder())(0)

    lazy val iterator = streams.iterator()

    while (iterator.hasNext()) {
      val tweet = iterator.next().message().map(_.toChar).mkString
      val numTags = tweet.count(_ == '#')
      println(s"[Consumer] [TagCount=$numTags] $tweet")
    }

  } catch {
    case ex: Exception =>
      ex.printStackTrace()
  }

  read()

}
