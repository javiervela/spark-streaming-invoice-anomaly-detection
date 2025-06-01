package es.dmr.uimp.simulation

import java.util.{Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random

object InvoiceDataProducer extends App {
  // Parameters
  val events = args(0)
  val topic = args(1)
  val brokers = args(2)

  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  System.out.println("Sending purchases!")

  // Read events from file and push them to kafka
  for (line <- Source.fromFile(args(0)).getLines()) {
    // Get invoice id
    val invoiceNo = line.split(",")(0)
    val data = new ProducerRecord[String, String](topic, invoiceNo, line)
    producer.send(data)

    // Introduce random delay
    Thread.sleep(5 +  (5*Random.nextFloat()).toInt)
  }

  producer.close()
}