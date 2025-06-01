package es.dmr.uimp.clustering

import es.dmr.uimp.clustering.Clustering._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BisectingKMeansClusterInvoices {

  def main(args: Array[String]) {

    import Clustering._

    val sparkConf =
      new SparkConf().setAppName(" BisectingKMeansClusterInvoices")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    // load data
    val df = loadData(sc, args(0))

    // Very simple feature extraction from an invoice
    val featurized = featurizeData(df)
    featurized.printSchema()

    // Filter not valid entries
    val filtered = filterData(featurized)
    filtered.printSchema()

    // Transform in a dataset for MLlib
    val dataset = toDataset(filtered)

    // We are going to use this a lot (cache it)
    dataset.cache()

    // Print a sampl
    dataset.take(5).foreach(println)

    val model = trainModel(dataset)
    // Save model
    model.save(sc, args(1))

    // Save threshold
    val distances = dataset.map(d => distToCentroid(d, model))
    val threshold =
      distances
        .top(2000)
        .last // set the last of the furthest 2000 data points as the threshold

    saveThreshold(threshold, args(2))
  }

  /** Train a KMean model using invoice data.
    */
  def trainModel(data: RDD[Vector]): BisectingKMeansModel = {

    val models = 1 to 20 map { k =>
      val model = new BisectingKMeans()
      model.setK(k) // find that one center
      model.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection(costs, 0.7)
    System.out.println("Selecting BisectingKMeans Model: " + models(selected).k)
    models(selected)
  }

  /** Calculate distance between data point to centroid.
    */
  def distToCentroid(datum: Vector, model: BisectingKMeansModel): Double = {
    val centroid =
      model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

}
