# Monte Carlo Financial Simulation on Apache Spark

## Author
<Maaz Khan, 667601660>

## How Monte Carlo works
Monte Carlo simulation is based on the notion of solving a problem by random sampling. As a practical example, consider a game of dice where a user can roll a dice for an outcome of 1 to 100. If the user rolls anything from 1-50, the "house" wins. If the user rolls anything from 51 to 100, the "user" wins. By constantly rolling the dice for a large number of turns, we can calculate the probability of winning by simply counting the number of times the user rolls between 1-50. In this simulation, we sample a substantial amount of trading dates at random and observe what happens on those dates. The end result is an aggregation of these results.
In our simulation, we take a list of stocks and their data over a period of time and invest a particular amount in them. Then we take the percentage change in price of the stock of our samples and multiply it with the respective weights of our investments to calculate an overall daily %loss. And at the end of the simulation, we choose the 5th percentile (worst), 95th (best), and most likely outcomes and present them to the user. 

## Setup
We import our stock data using the python file 'getStocks.py'. The names of the stocks are present in 'symbols.txt' and starting and end dates are located in 'my_script_conf.json'.

We need to import all the spark libraries and documentation into IntelliJ first for which we will use the sbt. The following lines need to be added in the built.sbt

    name := "Sparking"
    
    version := "0.1"
    
    scalaVersion := "2.11.12"
    
    libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.1",
      "com.typesafe" % "config" % "1.3.2",
    "org.apache.spark" %% "spark-sql" % "2.3.2"
    )
This enables us to use spark directly on intelliJ.
To start running it on sandbox, we have to do the following:

 1. First we must change the argument of `conf.setMaster("local")` to yarn to be able to run it in a distributed fashion over multiple machines. 
 2. Upload the symbols.txt and stocks.xlsx to HDFS by using ambari or ssh
 3. Change the directories in the configuration file into the respective directory in HDFS
 4. We are going to package this code into a compiled jar file that can be deployed on the sandbox through the use of `sbt package`. This is done through the command line while present in the project directory.
 5. Upload the jar file to HDFS: `scp -P 2222 <jarfile> root@sandbox-hdp.hortonworks.com:/root `
 6. To run: `spark-submit --class <main class>  --master yarn ./<jar file>`

## Logging and Config
Logging is done at various levels and configuration file is present in the resources folder.
