import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object TweetProducer extends App {

  val BROKER_LIST = "kafka:9092" //change it to localhost:9092 if not connecting through docker
  val KEYWORDS = List("#scala", "#kafka", "#cassandra", "#solr", "#apachespark", "#fastdata", "#bigdata")
  val TOPIC = "tweets"

  val props = new Properties()
  props.put("bootstrap.servers", BROKER_LIST)
  props.put("client.id", "KafkaTweetProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def startTweetStream() = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    cb.setOAuthConsumerKey("p5vABCjRWWSXNBkypnb8ZnSzk")  //replace this with your own keys
    cb.setOAuthConsumerSecret("wCVFIpwWxEyOcM9lrHa9TYExbNsLGvEUgJucePPjcTx83bD1Gt")  //replace this with your own keys
    cb.setOAuthAccessToken("487652626-kDOFZLu8bDjFyCKUOCDa7FtHsr22WC3PMH4iuNtn")  //replace this with your own keys
    cb.setOAuthAccessTokenSecret("4W3LaQTAgGoW5SsHUAgp6gK9b5AKgl8hRcFnNYgvPTylU")  //replace this with your own keys

    val stream = new TwitterStreamFactory(cb.build()).getInstance()

    val listener = new StatusListener {

      override def onTrackLimitationNotice(i: Int) = println(s"Track limited $i tweets")
      override def onStallWarning(stallWarning: StallWarning) = println("Stream stalled")
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = println("Status ${statusDeletionNotice.getStatusId} deleted")
      override def onScrubGeo(l: Long, l1: Long) = println(s"Geo info scrubbed. userId:$l, upToStatusId:$l1")
      override def onException(e: Exception) = println("Exception occurred. " + e.getMessage)

      override def onStatus(status: Status): Unit = {
        val tweet = status.getText
        println("[Producer] " + tweet)
        val data = new ProducerRecord[String, String](TOPIC, tweet)
        producer.send(data)
      }

    }

    stream.addListener(listener)
    val fq = new FilterQuery()
    fq.track(KEYWORDS.mkString(","))
    stream.filter(fq)

  }

  startTweetStream()

}

