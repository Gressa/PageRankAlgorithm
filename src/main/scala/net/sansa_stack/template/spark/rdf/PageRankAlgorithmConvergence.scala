package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import scala.collection.mutable
import org.apache.spark.graphx.Graph
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io.PrintWriter
import java.util.concurrent.TimeUnit;
import java.io.File
import java.net.URI
import scala.collection.mutable
import org.apache.jena.riot.Lang
import org.apache.jena.graph._
import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.spark.kge.triples.Triples


object PageRankAlgorithmConvergence {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.predicate, config.tolerance, config.considertotalpages, config.out, config.limit)
      case None =>
        println(parser.usage)
    }
  }
  
  def run(input: String, predicate: String, tolerance: Double, considertotalpages: Boolean, output: String, limit: Int): Unit = {
    println("================")
    println("|   PageRank   |")
    println("================")

    val spark = SparkSession.builder
      .appName(s"PageRank ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //Page rank with convergence
    println("\n\n================================================")
    println("PageRank with Convergence => Tolerance: " + tolerance)
    println("================================================")

    //convergence page rank
    convergencePageRank(spark, predicate, input, tolerance, considertotalpages, limit)

    convergenceGraphXPageRank(spark, predicate, input, tolerance, limit)
    
    spark.stop
  }
  
  def convergencePageRank(spark: SparkSession, outlink: String, input: String, tolerance: Double, considertotalpages: Boolean, limit: Int): Unit = {
    var startTime = System.currentTimeMillis()

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    val subjects = triples.getSubjects().distinct()
    val filteredTriples = triples.find(None, Some(NodeFactory.createURI(outlink)), None)

    val pairs = filteredTriples.map { f => (f.getSubject, f.getObject) }
    val links = pairs.distinct().groupByKey().cache()
    val allPages = subjects.union(filteredTriples.getObjects().distinct()).distinct()
    val numOfAllPages = allPages.distinct().count().toDouble

    val preprocessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0) // in minutes
    println("Num of all pages: " + numOfAllPages)
    println("\nPageRank with Convergence Preprocessing Time: " + preprocessingTime)

    startTime = System.currentTimeMillis()

    var pageRank = links.mapValues(v => 1.0)
    
    var leftPages = allPages.subtract(pageRank.map(f => f._1))
    var rankedLeftPages = leftPages.distinct().flatMap(f => Some(f, 1.0))
    pageRank = pageRank.union(rankedLeftPages).distinct()

    var didConverge = false

    while (!didConverge) {
      val contributions = links.join(pageRank)
        .values
        .flatMap {
          case (urls, rank) =>
            val size = urls.size.toDouble
            urls.map(url => (url, rank / size))
        }
      var leftSum = 1 - 0.85
      if (considertotalpages) {
        leftSum = leftSum / numOfAllPages
      }

      //This calculation is done in case a page has no outlinks
      var newPageRanks = contributions.reduceByKey(_ + _).mapValues(leftSum + 0.85 * _)
      leftPages = allPages.subtract(newPageRanks.map(f => f._1))
      rankedLeftPages = leftPages.distinct().flatMap(f => Some(f, leftSum))
      newPageRanks = newPageRanks.union(rankedLeftPages).distinct()
      
      val outer = new Breaks;
      outer.breakable {
        for (newRank <- newPageRanks.collect()) {
          //find old page rank for this value
          val oldRank = pageRank.filter { case (key, value) => key == newRank._1 }
          if (Math.abs(newRank._2 - oldRank.values.sum()) < tolerance) {
            didConverge = true
            pageRank = newPageRanks
          } else {
            didConverge = false
            pageRank = newPageRanks
            outer.break
          }
        }
      }
    }
    val pageRankCalculationTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("Page Rank with Convergence calculation time: " + pageRankCalculationTime)

    startTime = System.currentTimeMillis()
    val triplesRank = pageRank.sortBy(_._2, ascending = false)
    .map(f => Triple.create(NodeFactory.createURI(f._1.toString()), NodeFactory.createURI("http://property/pageRank"), NodeFactory.createURI(f._2.toString())))
    val sparqlQuery = s"""SELECT *
                         WHERE { ?s ?p ?o }
                         LIMIT $limit"""
    val result = triplesRank.sparql(sparqlQuery)
    result.rdd.foreach(println(_))
    
    val postProcessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("\nPage Rank with Convergence postprocessing time: " + postProcessingTime)

    val totalRunningTime = preprocessingTime + pageRankCalculationTime + postProcessingTime
    println("Page Rank with Convergence total running time: " + totalRunningTime + " minutes")
    println("===================================================================")
  }
  
  def convergenceGraphXPageRank(spark: SparkSession, outlink: String, input: String, tolerance: Double, limit: Int): Unit = {
    //GraphX with convergence
    var startTime = System.currentTimeMillis()
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    val subjects = triples.getSubjects().distinct()
    val filteredTriples = triples.find(None, Some(NodeFactory.createURI(outlink)), None)
    val graph = filteredTriples.asGraph()

    val preprocessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0) // in minutes
    println("\nConvergence GraphX Preprocessing Time: " + preprocessingTime)

    startTime = System.currentTimeMillis()
    val convergenceStartTimePageRank = System.currentTimeMillis()
    val pagerank = graph.pageRank(tolerance).vertices
    val cpr = pagerank.join(graph.vertices)
      .map({ case (k, (r, v)) => (r, v, k) })

    val calculationTime = (System.currentTimeMillis() - convergenceStartTimePageRank) / (60 * 1000.0)
    println("\nGraphX Page Rank with Convergence calculation time: " + calculationTime)

    startTime = System.currentTimeMillis()
    val triplesRanked = cpr.sortBy(_._1, ascending = false)
    .map(f => Triple.create(NodeFactory.createURI(f._1.toString()), NodeFactory.createURI("http://property/graphXPageRank"), NodeFactory.createURI(f._2.toString())))
    val sparqlQuery = s"""SELECT *
                         WHERE { ?s ?p ?o }
                         LIMIT $limit"""
    val result = triplesRanked.sparql(sparqlQuery)
    result.rdd.foreach(println(_))

    val postProcessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("\nGraphX Page Rank with Convergence postprocessing time: " + postProcessingTime)

    val totalConvergenceGraphXRunningTime = preprocessingTime + calculationTime + postProcessingTime
    println("GraphX Page Rank with Convergence total running time: " + totalConvergenceGraphXRunningTime + " minutes")
    println("===============================================================================")
  }
  
  case class Config(in: String = "", predicate: String = "", iterations: Int = 0, tolerance: Double = 0, considertotalpages: Boolean = false, out: String = "", limit: Int = 0)

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("Page Rank") {

    head(" PageRank ")

    opt[String]('i', "input").required().valueName("<path>")
      .action((x, c) => c.copy(in = x))
      .text("path to file that contains the data (in N-Triples format)")
    help("help").text("prints this usage text")

    opt[Double]('t', "tolerance")
      .action((x, c) => c.copy(tolerance = x))
      .text("tolerance")

    opt[Boolean]('c', "considertotalpages")
      .action((x, c) => c.copy(considertotalpages = x))
      .text("consider total pages")

    opt[String]('p', "predicate")
      .action((x, c) => c.copy(predicate = x))
      .text("Predicate")

    opt[Int]('q', "limit")
      .action((x, c) => c.copy(limit = x))
      .text("Query Limit")

    opt[String]('o', "output").valueName("<directory>")
      .action((x, c) => c.copy(out = x))
      .text("path to file that contains the page rank of data (in N-Triples format)")
  }
  
}