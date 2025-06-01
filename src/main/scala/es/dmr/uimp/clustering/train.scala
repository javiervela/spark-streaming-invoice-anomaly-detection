package es.dmr.uimp.clustering

import es.dmr.uimp.clustering.Clustering.elbowSelection
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KMeansClusterInvoices {

  def main(args: Array[String]) {

    import Clustering._

    val sparkConf = new SparkConf().setAppName("ClusterInvoices")
    val sc = new SparkContext(sparkConf)

    // load data
    val df = loadData(sc, args(0))

   // Very simple feature extraction from an invoice
    val featurized = featurizeData(df)

    // Filter not valid entries
    val filtered = filterData(featurized)

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
    val threshold = distances.top(2000).last // set the last of the furthest 2000 data points as the threshold

    saveThreshold(threshold, args(2))
  }

  /**
   * Train a KMean model using invoice data.
   */
  def trainModel(data: RDD[Vector]): KMeansModel = {

    val models = 1 to 20 map { k =>
      val kmeans = new KMeans()
      kmeans.setK(k) // find that one center
      kmeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection(costs, 0.7)
    System.out.println("Selecting model: " + models(selected).k)
    models(selected)
  }

  /**
   * Calculate distance between data point to centroid.
   */
  def distToCentroid(datum: Vector, model: KMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

}

