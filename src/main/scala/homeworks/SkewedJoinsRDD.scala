package homeworks

import generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SkewedJoinsRDD {
  val spark = SparkSession.builder()
    .appName("Skewed Joins on RDDs")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  Case Description:
  => 2 laptops are “similar” if they have the same make & model, but proc speed within 0 and 1,
  e.g. some products are similar if their specifications are within some intervals.
  => For each laptop configuration we are interested in the average sale price of similar models.
  => Assume that we have Acer Predator 2.9GHz with id = “hkjkjhkjh”
  and we are interested in average sale price for all Acer Predator laptops with CPU between 2.8 and 3.0 GHz.
  And we want to this average price to be computed for every single Acer laptop in this dataset.
  */

  val laptops: RDD[Laptop] = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
  val laptopOffers: RDD[LaptopOffer] = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

  // to make a join we need to convert these RDDs to key-value RDDs
  def plainJoin(): Long = {
    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }
    val preparedLaptopOffer = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result = preparedLaptops.join(preparedLaptopOffer) // RDD[((make, model), ((registration, procSpeed), (procSpeed, salePrice)))]
      .filter {
        case ((make, model), ((registration, laptopPprocSpeed), (offerProcSpeed, salePrice))) => Math.abs(laptopPprocSpeed - offerProcSpeed) <= 0.1
      }
      .map {
        case ((make, model), ((registration, laptopPprocSpeed), (offerProcSpeed, salePrice))) => (registration, salePrice)
      }
      // group by registration and aggregate by salesPrice -> for RDD this combination of groupByKey + mapValues is VERY SLOW
      .groupByKey() // receive a batch of aggregated records for every key
      .mapValues(prices => prices.sum / prices.size) // map the batch to one record
    result.count()
  }

  // to make a join we need to convert these RDDs to key-value RDDs
  def fastJoin(): Long = {
    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }
    val preparedLaptopOffer = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result: RDD[(String, Double)] = preparedLaptops.join(preparedLaptopOffer) // RDD[((make, model), ((registration, procSpeed), (procSpeed, salePrice)))]
      .filter {
        case ((make, model), ((registration, laptopPprocSpeed), (offerProcSpeed, salePrice))) => Math.abs(laptopPprocSpeed - offerProcSpeed) <= 0.1
      }
      .map {
        case ((make, model), ((registration, laptopPprocSpeed), (offerProcSpeed, salePrice))) => (registration, salePrice)
      }
      .aggregateByKey((0.00, 0)) ( // starting tuple defines an output aggregated values we need to calculate average value for every key
        { // first combined function
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        { // second combined function
          // This function combines 2 different states in 2 single states
          case ((totalPrice1, numPrices1), (totalPrice2, numPrices2)) => (totalPrice1 + totalPrice2, numPrices1 + numPrices2) // ombine 2 states into 1 state
        }
      ) // return RDD[(String, (Double, Int))]
      // then divide Double by Int to obtain an average
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }
    result.count()
  }

  def noSkewJoins() = {
    val preparedLaptops = laptops
      .flatMap {
        laptop =>
          // for every laptop we return a sequence of that laptop and 2 copies
          Seq(
            laptop,
            laptop.copy(procSpeed = laptop.procSpeed - 0.1), // initialize procSpeed
            laptop.copy(procSpeed = laptop.procSpeed + 0.1)  // initialize procSpeed
          )
      }
      .map {
        case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), registration)
      }

    val preparedLaptopOffer = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedLaptopOffer) // RDD[((make, model, procSpeed), ((registration, salePrice)))]
      .map(_._2) // RDD[(registration, salePrice)]
      .aggregateByKey((0.00, 0)) ( // starting tuple defines an output aggregated values we need to calculate average value for every key
        { // first combined function
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        { // second combined function
          // This function combines 2 different states in 2 single states
          case ((totalPrice1, numPrices1), (totalPrice2, numPrices2)) => (totalPrice1 + totalPrice2, numPrices1 + numPrices2) // ombine 2 states into 1 state
        }
      ) // return RDD[(String, (Double, Int))]
      // then divide Double by Int to obtain an average
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }
    result.count()
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    fastJoin()
    noSkewJoins()
    Thread.sleep(1000000)
  }
}
