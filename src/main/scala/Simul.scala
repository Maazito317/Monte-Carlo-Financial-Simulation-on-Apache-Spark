import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{SQLContext, SparkSession};
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object Simul {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    //Starting the spark session
    val logger = LoggerFactory.getLogger("Simul")
    val confiz = ConfigFactory.load("my_app.conf")
    val conf = new SparkConf().setAppName("monte-carlo-var-calculator").setMaster(confiz.getString("config-info.Master"))
    val sc =  new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()
    val r = new scala.util.Random

    //Specify investment, trials, stocks, and data
    val FRAC_TRIALS = confiz.getString("config-info.FRAC_TRIALS").toDouble
    logger.info("FRAC_TRIALS loaded from config")
    val totalInvestement = confiz.getString("config-info.totalInvestement").toFloat
    logger.info("Total investments loaded from config:",totalInvestement)
    val symbolDir = confiz.getString("config-info.symbolDir")
    logger.info("Symbol directory loaded from config")
    val stockDataDir = confiz.getString("config-info.stockDataDir")
    logger.info("Stock data directory loaded from config")
    val symbolRDD = sc.textFile(symbolDir)
    val symbolList = symbolRDD.collect()
    val numSymbols = symbolRDD.count().toInt
    logger.debug("Number of symbols:",numSymbols)

    //Specify investments for individual stocks

    val symbolsAndRawWeightRDD = symbolRDD.map(x => {
      (x, r.nextInt(10))
    })

    val sumOfWeights = symbolsAndRawWeightRDD.values.sum()
    val symbolsAndWeightsIm = symbolsAndRawWeightRDD.map(x => (x._1, x._2/sumOfWeights.toFloat)).collectAsMap()
    val symbolsAndWeights = collection.mutable.Map(symbolsAndWeightsIm.toSeq: _*)

    logger.debug("Symbols and weights:",symbolsAndWeights)

    symbolsAndWeights.foreach(x => println(x._2 + 1f))

    //map the data into tuples of: (date,(stock name, change in price of stock)
    val datesToSymbolsAndChangeRDD = sc.textFile(stockDataDir).filter(x => !x.contains("symbol")).map((x) => {
      val splits = x.split(",", -2)
      val changeInPrice = splits(8).toFloat
      val symbol = splits(7)
      val date = splits(0)
      (date, (symbol, changeInPrice))
    })

    val groupedDatesToSymbolsAndChangeRDD = datesToSymbolsAndChangeRDD.groupByKey()

    val numEvents = groupedDatesToSymbolsAndChangeRDD.count()
    logger.debug("Number of events registered:",numEvents)

    if (numEvents < 1) {
      println("No trade data")
      return "No trade data"
    }

    //Monte Carlo simulation samples random dates and accumulates total change in price with respective weights
    logger.info("Beginning monte carlo simulation.....")
    val resultOfTrials = groupedDatesToSymbolsAndChangeRDD.sample(true,FRAC_TRIALS).map(i => {
      var total = 0f
      for (t <- i._2) {
        val symbol = t._1
        val changeInPrice = t._2
        val weight = symbolsAndWeights.getOrElse(symbol,0f)
        total += changeInPrice * weight
0      }
      //If going in loss, shift the investments
      if (total < 0) {
        val lossSymbol = i._2.minBy(_._2)._1
        val lossInvested = symbolsAndWeights.getOrElse(lossSymbol,0f)
        val investInSymbol1 = symbolList(r.nextInt(numSymbols))
        val investInSymbol2 = symbolList(r.nextInt(numSymbols))
        symbolsAndWeights(lossSymbol) = 0f
        symbolsAndWeights(investInSymbol1) = symbolsAndWeights(investInSymbol1) + lossInvested/2
        symbolsAndWeights(investInSymbol2) = symbolsAndWeights(investInSymbol2) + lossInvested/2
      }
      (i._1,total)
    })
    logger.info("Monte carlo simulation complete")

    //Create a table for results showing worst case(5th percentile), best case(95th percentile), and most likely outcome
    logger.info("Creating result table....")
    val resultOfTrialsRows = resultOfTrials.map(x => RowFactory.create(x._1,  (x._2*100).asInstanceOf[Object]))
    val schema = DataTypes.createStructType(Array[StructField](DataTypes.createStructField("date", DataTypes.StringType, false), DataTypes.createStructField("changePct", DataTypes.FloatType, false)))
    val resultOfTrialsDF = sqlContext.createDataFrame(resultOfTrialsRows, schema)
    resultOfTrialsDF.createOrReplaceTempView("results")
    val percentilesRow = sqlContext.sql("select percentile(changePct, array(0.05,0.50,0.95)) from results").collectAsList

    logger.info("Printing results for worst, best, and most likely case")
    val worstCase = percentilesRow.get(0).getList(0).get(0).toString().toFloat / 100;
    val mostLikely = percentilesRow.get(0).getList(0).get(1).toString().toFloat / 100;
    val bestCase = percentilesRow.get(0).getList(0).get(2).toString().toFloat / 100;
    println("In a single day, this is what could happen to your stock holdings if you have $" + totalInvestement + " invested");
    println("Worst case", Math.round(totalInvestement * worstCase / 100),"$", worstCase,"%");
    println("most likely scenario", Math.round(totalInvestement * mostLikely / 100),"$", mostLikely,"%");
    println("best case", Math.round(totalInvestement * bestCase / 100),"$", bestCase,"%");

  }

}
