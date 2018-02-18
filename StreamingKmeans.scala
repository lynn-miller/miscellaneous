/** FIT5202 Assignment 3 - Big Data Project
  * Part 3 POC01 - Streaming K-Means Demo
  *
  * Author: Lynn Miller
  * Group: 02
  * Version: 1
  * Date: 19/10/2017
  *
  * Description:
  *   1) Sets up a streaming context to run streaming k-means 
  *   2) Creates an input file for streaming K-means in the streaming input directory every 5 seconds
  *
  * Files:
  *   Uses these directories in the current location; any existing files will be removed; will be
  *   created if it doesn't exist:
  *   input - the location of the streaming files
  *   output - three types of output files are created:
  *     trueCentres-xx - the true centres of the data, starting from batch xx
  *     model-xx - the centres found by the streaming k-means algorithm after processing batch xx
  *     predictions-xx - the predicted cluster for each data point in batch xx
  *
  * Instructions:
  *   Run from spark-shell using:
  *   :load PoC01.scala
  *
  *   The demo runs for 50 iterations with 5 seconds between each iteration, so lasts for about 4 minutes
  */

sc.setLogLevel("OFF")

import org.apache.spark._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io._
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

val numCentres = 3
val numDims = 2
val batchSize = 100
val numBatches = 50
val batchInterval = 5000L

// Streaming directory is in the current directory
val streamDir = "input/"
val outputDir = "output/"
val streamFilePre = streamDir + "batch"

var predArray = new ArrayBuffer[RDD[(Double,Int)]]
var predArray = new ArrayBuffer[Array[(Double,Int)]]
val dateFormat = new SimpleDateFormat("YYYYMMdd-hhmmss")

/** Create the streaming directory if it doesn't exist
  * clear it out if it does
  */
val dir = new File(streamDir)
if (dir.exists()) {
  dir.list().foreach(f => {
    val f1 = new File(streamDir + f)
    f1.delete()
  })
} else {
  dir.mkdir()
}

val dir = new File(outputDir)
if (dir.exists()) {
  dir.list().foreach(f => {
    val f1 = new File(outputDir + f)
    f1.delete()
  })
} else {
  dir.mkdir()
}

/** Streaming K-Means - Set up the input data stream and define the model
  *
  * The K-Means model parameters are:
  * - Number of centres is 3
  * - Initial centres are randomly generated
  * - The Decay factor is 0.01. This controls how long old data is retained in the model
  *   so how quickly is adapts to changes in the data - a low decay factor ensures the
  *   model changes can be easily seen
  *
  * Each streaming file is processed by:
  * - Add the data points from the new file to the model
  * - Predict the cluster for each data point and calculate the model cost
  */
sc.stop()
val conf = new SparkConf().setAppName("StreamingKMeansDemo")
val ssc = new StreamingContext(conf, Seconds(batchInterval/1000))
val sc = ssc.sparkContext
sc.setLogLevel("OFF")

/** Create the test stream */
val testData = ssc.textFileStream(streamDir).map(LabeledPoint.parse)
/** Create the training stream */
val trainingData = testData.map(lp => lp.features)
/** Define the model */
val model = new StreamingKMeans().setK(numCentres).setDecayFactor(0.01).setRandomCenters(numDims, 0.0)

/** If a new streaming file is found, re-train the model */
model.trainOn(trainingData)

/** Then use the model to predict a cluster for each record */
val preds = model.predictOnValues(testData.map(lp => (lp.label, lp.features)))

/** Saves the prediction results into an array */
preds.foreachRDD((r,t) => {
  if ( r.count() > 0 ) {
    predArray.append(r.collect())
  }
})

/** Display the model cost - the sum of the euclidean distance of each point from its cluster centre */
trainingData.foreachRDD(r => {
  if ( r.count() > 0 ) {
    val wssse = model.latestModel().computeCost(r)
    println("Model cost is : " + wssse.round)
  }
})

/** Start the stream */
ssc.start()
ssc.awaitTerminationOrTimeout(1)





/** Data Generation - create the streaming batches
  *
  * Randomly pick a true centre for each cluster.
  *
  * Each point in each batch will belong to a randomly chosen cluster, and will be
  * randomly distributed around the centre
  */
import breeze.linalg._
import breeze.stats.distributions._

/** Converts a breeze vector to a string */
def trueCentreToString ( c : DenseVector[Double] ) : String = {
  var str = ""
  var d = "["
  c.foreach(x => {str = str + f"$d${x.round}";d=", "})
  str = str + "]"
  str
}

/** Converts a linalg vector to a string */
def modelCentreToString ( c : org.apache.spark.mllib.linalg.Vector ) : String = {
  var str = ""
  var d = "["
  c.foreachActive((x,y) => {str = str + f"$d${y.round}";d=", "})
  str = str + "]"
  str
}

val maxCentreVal = 20
var centres = Array.ofDim[breeze.linalg.DenseVector[Double]](numCentres)
val rv1 = scala.util.Random
var pt = DenseVector[Int](numDims)

/** covariance matrix for points generate */
val coVars = DenseMatrix((1.5,0.0),(0.0,1.5))

/** Generate the true centres */
for (c <- 0 until numCentres) { centres(c) = DenseVector(rv1.nextInt(maxCentreVal),rv1.nextInt(maxCentreVal)) }
println("True centres are:")
centres.foreach(c => println(trueCentreToString(c)))
val outputFile = new File(outputDir + "trueCentres-0")
val outputWriter = new PrintWriter(outputFile)
centres.foreach(c => outputWriter.write(trueCentreToString(c) + "\n"))
outputWriter.close

/** Generate the batches */
for (f <- 0 until numBatches) {

  /** Every 10th batch, randomly change a centre */
  if (f%10 == 0 && f > 0) {
    val c = rv1.nextInt(numCentres)
    centres(c) = DenseVector(rv1.nextInt(maxCentreVal),rv1.nextInt(maxCentreVal))
    println("\n\n=============================")
    println(f"Changed centre $c to: " + trueCentreToString(centres(c)))
    println("=============================")
    val outputFile = new File(outputDir + "trueCentres-" + f)
    val outputWriter = new PrintWriter(outputFile)
    centres.foreach(c => outputWriter.write(trueCentreToString(c) + "\n"))
    outputWriter.close
  }

  val streamFile = new File(streamFilePre + f)
  println("\n\n---------------------------------------")
  println("Generating streaming file " + streamFilePre + f)
  println("---------------------------------------")
  val streamWriter = new PrintWriter(streamFile)
  for (i <- 0 until batchSize) {
    /** Randomly pick a centre */
    val c = rv1.nextInt(numCentres)
    /** Sample from MultivariateGaussian with means of the true centre */
    MultivariateGaussian(centres(c), coVars).sample(1).foreach(v => pt=v.map(_.round.toInt))
    /** output streaming record */
    streamWriter.write("(" + c + ",[" + pt.toArray.mkString(",") + "])\n")
  }
  streamWriter.close

  /** wait until the current files have been processed */
  Thread.sleep(batchInterval)

  /** how are we going? print and save the model centres and the true centres for comparison */
  println("Latest model centres are:")
  model.latestModel().clusterCenters.foreach(pt => println(modelCentreToString(pt)))
  val outputFile = new File(outputDir + "model-" + f)
  val outputWriter = new PrintWriter(outputFile)
  model.latestModel().clusterCenters.foreach(c => outputWriter.write(modelCentreToString(c) + "\n"))
  outputWriter.close
  println("True centres are:")
  centres.foreach(c => println(trueCentreToString(c)))
}

/** Save the true cluster and predicted cluster for each record in each batch. 
  * True clusters are converted to letters - we don't need the cluster numbers to match
  * We want to see that the model has clustered the records together correctly
  */
for ( i <- 0 until predArray.size ) { 
  val predsFile = new File(outputDir + "predictions-" + i)
  val predsWriter = new PrintWriter(predsFile)
  predArray(i).foreach(c => predsWriter.write((c._1.toInt + 'A').asInstanceOf[Char] + "," + c._2 + "\n"))
  predsWriter.close
}

/** Stop the streaming context */
ssc.stop()
