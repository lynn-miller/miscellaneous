/** FIT5202 Assignment 2 - Big Data Programming Fundamentals
  * Part 3 q3b-2 - Bloom filter
  *
  * Author: Lynn Miller
  * Student ID: 27610020
  * Version: 1
  * Date: 27/09/2017
  *
  * Description:
  *   Demonstrates a bloom filter using randomly generated strings
  *   1) Create the "existing" dataset as random strings of 4 alphanumeric characters
  *   2) Create a bloom filter from the "existing" dataset, using 4 hash functions
  *   3) Test the bloom filter using strings generated from another random variable (inputStream)
  *   4) For each string in inputStream, generate the hashes and test against the bloom filter
  *   4) If a possible match is found, check for a true or false match against the actual dataset
  *   5) Record as a new record, existing record or false match as appropriate
  *   6) Print the results
  *
  * Instructions:
  *   Can be run from Linux shell using:
  *   scala q3b-2.scala
  *   Or from scala using
  *   :load q3b-2.scala
  *   (Hint: if running from scala issue the :silent command first to suppress unnecessary messages)
  *
  *   The program takes 15-20 seconds to run
  */
import scala.util.hashing
import scala.util.hashing._
import util.control.Breaks._
import scala.collection.mutable.BitSet

val hashSeeds = Array[Int](3856, 8842, 5981, 7294)
val numHashes = hashSeeds.length
val datasetSize = 100000
val filterLength = 800000
var bloomFilter = new BitSet
val inputStreamLength = 50000


/** [F1] Generates a hash value between 0 and maxValue-1 */
def genHash (x : String, hashSeed : Int, maxValue : Int) : Int = {
  val hashValue = MurmurHash3.stringHash(x, hashSeed)
  Math.abs(hashValue) % maxValue
}


/** [F2] Updates the bloom filter
  * Generate all the hash values for the string and sets the bloom filter bits
  */
def setFilterBits (x : String) {
  for (h <- 0 until numHashes) {
    val idx = genHash(x, hashSeeds(h), filterLength)
    bloomFilter = bloomFilter + idx
  }
}


/** [F3] Checks if the bitset contains all the hashes for the string */
def checkFilterBits (x : String) : Boolean = {
  for (h <- 0 until numHashes) {
    val idx = genHash(x, hashSeeds(h), filterLength)
    if (!bloomFilter.contains(idx)) return false
  }
  true
}


/** [A] Create the "existing" dataset and bloom filter */
var sample = Array.ofDim[String](datasetSize)
val rand1 = scala.util.Random
rand1.setSeed(9527)
for (i <- 0 until datasetSize) {
  sample(i) = rand1.alphanumeric.take(4).mkString
  setFilterBits(sample(i))
}

/** [B] Test the input stream elements for membership of the existing set */
var numFound = 0
var numNew = 0
var numFalseMatches = 0
val rand2 = scala.util.Random
rand2.setSeed(3976)
// process the input stream
for (i <- 0 until inputStreamLength) {
  var inputValue = rand2.alphanumeric.take(4).mkString
// [C] check if the element already exists in data set
  if (checkFilterBits(inputValue)) {
    var found = false
    breakable {
// [D] We've found a possible match - check for a false match
	  for (i <- 0 until datasetSize) {
        if ( inputValue == sample(i) ) {
          numFound += 1
          found = true
          break
        }
      }
    } 
    if ( !found ) { numFalseMatches += 1 }
// [E] We've found a new element
  } else {
    numNew += 1
  }
}

/** Print the results */
println("\nBloom filter details\n====================")
println(f"Existing dataset size: $datasetSize")
println(f"Filter length in bits: $filterLength")
println(f"Number of hashes: $numHashes")
println("\nBloom filter results\n====================")
println(f"Length of input stream: ${numFound+numFalseMatches+numNew}")
println(f"Strings found: $numFound")
println(f"False Matches: $numFalseMatches")
println(f"New strings: $numNew")
// Calculate and print the false match ratio
println(f"False match ratio: ${numFalseMatches*1.0/(numFalseMatches+numNew)}%4.4f")
// For comparison, calculate and print the probability of a false match
println(f"Pr[false match] = ${Math.pow(1-Math.exp(-1.0*numHashes*datasetSize/filterLength),numHashes)}%4.4f")
