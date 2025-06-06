package es.dmr.uimp.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import java.util.HashMap

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object InvoicePipeline {

  // Purchase: represents a single purchase line in an invoice
  case class Purchase(
      invoiceNo: String,
      quantity: Int,
      invoiceDate: String,
      unitPrice: Double,
      customerID: String,
      country: String
  )

  // Invoice: represents a complete invoice with aggregated data
  case class Invoice(
      invoiceNo: String,
      avgUnitPrice: Double,
      minUnitPrice: Double,
      maxUnitPrice: Double,
      time: Double,
      numberItems: Double,
      lastUpdated: Long,
      lines: Int,
      customerId: String
  )

  // Event: represents an event in the processing purchases pipeline
  sealed trait Event
  // GoodPurchase: valid purchase line
  case class GoodPurchase(p: Purchase) extends Event
  // ParseErrorPurchase: parsing error or invalid data
  case class ParseErrorPurchase(reason: String) extends Event

  // InvoiceState: maintains the state of an invoice during processing
  // - lines: list of Purchase objects
  // - ticksSinceUpdate: number of ticks since the last update
  // - expired: whether the invoice has expired (no updates for 2 ticks)
  // - errorReason: optional reason for errors (e.g., parsing issues)
  case class InvoiceState(
      lines: List[Purchase],
      ticksSinceUpdate: Int,
      expired: Boolean,
      errorReason: Option[String]
  )

  // CSV parser for parsing purchase lines
  lazy val csvParser: CsvParser = {
    val settings = new CsvParserSettings()
    settings.getFormat.setLineSeparator("\n")
    settings.setIgnoreLeadingWhitespaces(true)
    settings.setIgnoreTrailingWhitespaces(true)
    settings.setQuoteDetectionEnabled(true)
    new CsvParser(settings)
  }

  // Safely parses a CSV line into a Purchases or returns the parse error
  def safeParseCsv(line: String): Either[(String, String), Purchase] = {
    // If line is null or empty, return an error
    if (line == null || line.trim.isEmpty) {
      Left("UNKNOWN" -> "empty line")
    } else {
      try {
        // Parse the line using the CSV parser
        val parts = csvParser.parseLine(line)
        // If parts are null or have less than 8 elements, return an error
        if (parts == null || parts.length < 8) {
          val key =
            if (parts != null && parts.length >= 1) parts(0) else "UNKNOWN"
          Left(key -> "malformed purchase line")
        } else {
          try {
            // Create a Purchase object from the parsed parts
            Right(
              Purchase(
                parts(0),
                parts(3).toInt,
                parts(4),
                parts(5).toDouble,
                parts(6),
                parts(7)
              )
            )
          } catch {
            // If parsing fails, return an error with the first part as key
            case e: Exception =>
              Left(parts(0) -> s"parse error: ${e.getMessage}")
          }
        }
      } catch {
        // If CSV parsing fails, return an error with "UNKNOWN" key
        case e: Exception =>
          Left("UNKNOWN" -> s"parser exception: ${e.getMessage}")
      }
    }
  }

  def main(args: Array[String]) {

    // Get command line arguments
    val Array(
      modelFile,
      thresholdFile,
      modelFileBisect,
      thresholdFileBisect,
      zkQuorum,
      group,
      topics,
      numThreads,
      brokers
    ) = args

    // Create Spark Streaming context
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))
    // Set log level
    ssc.sparkContext.setLogLevel("ERROR")
    // Set checkpoint directory
    ssc.checkpoint("./checkpoint")

    // Load KMeans and BisectingKMeans models and thresholds
    val (kmeansModel, kmeansThreshold) =
      loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val (bisectModel, bisectThreshold) =
      loadBisectKMeansAndThreshold(sc, modelFileBisect, thresholdFileBisect)

    // Broadcast models and thresholds
    val kmeansModelBroadcast = sc.broadcast(kmeansModel)
    val kmeansThresholdBroadcast = sc.broadcast(kmeansThreshold)
    val bisectModelBroadcast = sc.broadcast(bisectModel)
    val bisectThresholdBroadcast = sc.broadcast(bisectThreshold)
    val brokersBroadcast = sc.broadcast(brokers)

    // Pipeline starts

    // 1) Ingest raw lines from Kafka “purchases” topic
    val raw: DStream[(String, String)] =
      connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    // 2) Parse CSV lines into Purchase or parsing errors
    val parsedPurchases: DStream[Either[(String, String), Purchase]] =
      raw.map(_._2).map(safeParseCsv)

    // 3) Split into parsing events into the valid Purchases and the parse errors Purchases
    // Also, set the invoice number as the key for events
    val parseErrorPurchases: DStream[(String, Event)] =
      parsedPurchases.flatMap {
        // Only keep parse errors
        case Left((invNo, reason)) if reason.startsWith("parse error:") =>
          Some(invNo -> ParseErrorPurchase(reason))
        case _ =>
          None
      }
    val validPurchases: DStream[(String, Event)] =
      parsedPurchases.flatMap {
        case Left(_) => None
        case Right(purchase) =>
          Some(purchase.invoiceNo -> GoodPurchase(purchase))
      }
    val allPurchases: DStream[(String, Event)] =
      validPurchases.union(parseErrorPurchases)

    // 4) Maintain InvoiceState per each invoice number (key)

    // Updates the state of each invoice based on incoming events and previous state.
    //  - Maintains an `InvoiceState` per `invoiceNo`, tracking invoice lines or parse errors.
    //  - Uses a tick (20s batch) to measure inactivity; expires state after 2 ticks (>40s) without updates.
    //  - On new valid purchase: merges lines and resets inactivity counter.
    //  - On parse error: records error and resets inactivity counter.
    //  - On no new events: increments inactivity counter.
    //  - When inactivity counter reaches 2, marks invoice as expired for processing.
    def updateInvoiceState(
        events: Seq[Event],
        oldStateOpt: Option[InvoiceState]
    ): Option[InvoiceState] = {

      // Get previous state or create a new one if not present
      val prev = oldStateOpt.getOrElse(
        InvoiceState(
          lines = List.empty,
          ticksSinceUpdate = 0,
          expired = false,
          errorReason = None
        )
      )

      // Drop state if already expired in previous batch
      if (prev.expired) return None

      events.collectFirst { case ParseErrorPurchase(reason) => reason } match {
        // If any new parse error purchase appears, store errorReason and reset ticks
        case Some(reason) =>
          Some(
            prev.copy(
              errorReason = Some(reason),
              ticksSinceUpdate = 0,
              expired = false
            )
          )

        // Else, check for new valid Purchases
        case None =>
          val goods = events.collect { case GoodPurchase(p) => p }

          if (goods.nonEmpty) {
            // If new valid purchases:
            // Merge with previous lines
            val mergedLines = prev.lines ++ goods
            // Reset ticksSinceUpdate and expired
            // Return updated state
            Some(
              prev.copy(
                lines = mergedLines,
                ticksSinceUpdate = 0,
                expired = false
              )
            )
          } else {
            // If no new purchases:
            // Increment ticksSinceUpdate
            val nextTicks = prev.ticksSinceUpdate + 1
            // If nextTicks >= 2, mark as expired
            val isExpired = nextTicks >= 2
            // Return updated state
            Some(prev.copy(ticksSinceUpdate = nextTicks, expired = isExpired))
          }
      }
    }

    // Update state by key using the updateInvoiceState function
    val stateDStream: DStream[(String, InvoiceState)] =
      allPurchases.updateStateByKey(updateInvoiceState)

    // 5) After timeout (expired == true), process completed invoices:
    val completedInvoices: DStream[(String, InvoiceState)] = stateDStream
      .transform { (rdd, _) => rdd.filter { case (_, st) => st.expired } }
      .cache()

    // (a) Erroneous due to parse errors or missing fields
    val erroneousInvoices: DStream[(String, String)] =
      completedInvoices.flatMap { case (invoiceNo, st) =>
        // If there is an error reason, return it
        st.errorReason
          .map(reason => invoiceNo -> reason)
          .orElse {
            // If no error reason, check for missing fields or invalid data
            // Check for empty lines
            if (st.lines.isEmpty) {
              Some(invoiceNo -> "no purchase lines")
            } else {
              // Check for missing customerID
              val missingCustomer =
                st.lines.find(p => StringUtils.isEmpty(p.customerID))
              // Check for invalid invoiceDate (hour < 0)
              val invalidDate =
                st.lines.find(p => extractHour(p.invoiceDate) < 0)
              // Check for missing country
              val missingCountry =
                st.lines.find(p => StringUtils.isEmpty(p.country))

              missingCustomer
                .map(_ => invoiceNo -> "missing customer ID")
                .orElse(
                  invalidDate.map(_ => invoiceNo -> "invalid invoice date")
                )
                .orElse(missingCountry.map(_ => invoiceNo -> "missing country"))
            }
          }
      }

    // (b) Build valid Invoice objects
    val validInvoices: DStream[Invoice] = completedInvoices.flatMap {
      case (invoiceNo, st) =>
        // If there is no error reason, there are lines, and fields are valid: create an Invoice
        if (st.errorReason.isEmpty && st.lines.nonEmpty) {
          val hasMissingCustomer =
            st.lines.exists(p => StringUtils.isEmpty(p.customerID))
          val hasInvalidDate =
            st.lines.exists(p => extractHour(p.invoiceDate) < 0)
          val hasMissingCountry =
            st.lines.exists(p => StringUtils.isEmpty(p.country))

          if (!hasMissingCustomer && !hasInvalidDate && !hasMissingCountry) {
            // Create an Invoice object from the valid lines
            Some(toInvoice(invoiceNo, st.lines, System.currentTimeMillis()))
          } else {
            None
          }
        } else {
          None
        }
    }

    validInvoices.foreachRDD { rdd =>
      println(s"[INFO] Valid invoices this batch: ${rdd.count()}")
    }

    // (c) Cancellations: invoiceNo starts with "C"
    val cancellations: DStream[Invoice] =
      validInvoices.filter(_.invoiceNo.startsWith("C"))

    // Count cancellations in the last 8 minutes, sliding every 1 minute
    // This will create a DStream of counts, one per minute
    val cancellationsCount: DStream[String] = cancellations
      .map(inv => s"${inv.invoiceNo}")
      .window(Minutes(8), Minutes(1))
      .transform { rdd =>
        val count = rdd.count()
        println(s"[INFO] Cancellations in last 8 min: $count")
        rdd.context.parallelize(Seq(s"$count"))
      }

    // (d) Anomaly candidates: validInvoices that are not cancellations
    val anomalyCandidates: DStream[Invoice] =
      validInvoices.filter(!_.invoiceNo.startsWith("C"))

    // 6) Anomaly detection

    // KMeans anomaly detection
    val kmeansAnomalies: DStream[String] = anomalyCandidates
      .filter { inv =>
        val features = Vectors.dense(
          inv.avgUnitPrice,
          inv.minUnitPrice,
          inv.maxUnitPrice,
          inv.time,
          inv.numberItems
        )
        val centroid = kmeansModelBroadcast.value.clusterCenters(
          kmeansModelBroadcast.value.predict(features)
        )
        val dist = Vectors.sqdist(features, centroid)
        dist > kmeansThresholdBroadcast.value
      }
      .map { inv =>
        println(s"[WARN] KMeans anomaly: ${inv.invoiceNo}")
        s"${inv.invoiceNo}"
      }

    // BisectingKMeans anomaly detection
    val bisectAnomalies: DStream[String] = anomalyCandidates
      .filter { inv =>
        val features = Vectors.dense(
          inv.avgUnitPrice,
          inv.minUnitPrice,
          inv.maxUnitPrice,
          inv.time,
          inv.numberItems
        )
        val centroid = bisectModelBroadcast.value.clusterCenters(
          bisectModelBroadcast.value.predict(features)
        )
        val dist = Vectors.sqdist(features, centroid)
        dist > bisectThresholdBroadcast.value
      }
      .map { inv =>
        println(s"[WARN] BisectKMeans anomaly: ${inv.invoiceNo}")
        s"${inv.invoiceNo}"
      }

    // 7) Publish to Kafka topics
    erroneousInvoices.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val keyed = rdd.map { case (invNo, reason) => s"$invNo,$reason" }
        println(s"[INFO] Publishing ${keyed.count()} erroneous invoices")
        publishToKafka("facturas_erroneas")(brokersBroadcast)(keyed)
      }
    }

    cancellationsCount.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println(s"[INFO] Publishing cancellation count")
        publishToKafka("cancelaciones")(brokersBroadcast)(rdd)
      }
    }

    kmeansAnomalies.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println(s"[INFO] Publishing ${rdd.count()} KMeans anomalies")
        publishToKafka("anomalias_kmeans")(brokersBroadcast)(rdd)
      }
    }

    bisectAnomalies.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println(s"[INFO] Publishing ${rdd.count()} BisectKMeans anomalies")
        publishToKafka("anomalias_bisect_kmeans")(brokersBroadcast)(rdd)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  //
  // Helper methods
  //

  // Publishes an RDD of strings to a Kafka topic.
  def publishToKafka(
      topic: String
  )(kafkaBrokers: Broadcast[String])(rdd: RDD[String]) = {
    rdd.foreachPartition { partition =>
      val producer =
        new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach { value =>
        // key is null; value is "invoiceId,reason" or similar
        producer.send(new ProducerRecord[String, String](topic, null, value))
      }
      producer.close()
    }
  }

  // Creates a Kafka configuration map for the producer
  def kafkaConf(brokers: String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props
  }

  // Load the KMeans model information: centroid and threshold
  def loadKMeansAndThreshold(
      sc: SparkContext,
      modelFile: String,
      thresholdFile: String
  ): (KMeansModel, Double) = {
    val model = KMeansModel.load(sc, modelFile)
    val threshold = sc.textFile(thresholdFile).first().toDouble
    (model, threshold)
  }

  // Load the BisectingKMeans model information: centroid and threshold
  def loadBisectKMeansAndThreshold(
      sc: SparkContext,
      modelFile: String,
      thresholdFile: String
  ): (BisectingKMeansModel, Double) = {
    val model = BisectingKMeansModel.load(sc, modelFile)
    val threshold = sc.textFile(thresholdFile).first().toDouble
    (model, threshold)
  }

  // Converts a list of Purchase objects into an Invoice object
  def toInvoice(
      invoiceNo: String,
      lines: List[Purchase],
      lastUpdated: Long
  ): Invoice = {
    val prices = lines.map(_.unitPrice)
    val quantitySum = lines.map(_.quantity).sum
    val hour = extractHour(lines.head.invoiceDate)
    val cust = lines.head.customerID
    Invoice(
      invoiceNo,
      avgUnitPrice = prices.sum / prices.size,
      minUnitPrice = prices.min,
      maxUnitPrice = prices.max,
      time = hour,
      numberItems = quantitySum.toDouble,
      lastUpdated = lastUpdated,
      lines = lines.size,
      customerId = cust
    )
  }

  // Extracts the hour from the invoice date string
  def extractHour(invoiceDate: String): Double = {
    try invoiceDate.split(" ")(1).split(":")(0).toDouble
    catch { case _: Exception => -1.0 }
  }

  // Connects to Kafka and returns a DStream of (key, value) pairs
  def connectToPurchases(
      ssc: StreamingContext,
      zkQuorum: String,
      group: String,
      topics: String,
      numThreads: String
  ): DStream[(String, String)] = {
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

}
