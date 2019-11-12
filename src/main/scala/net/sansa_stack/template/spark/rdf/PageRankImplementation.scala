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

object PageRankImplementation {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.predicate, config.iterations, config.considertotalpages, config.out, config.limit)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, predicate: String, iterations: Int, considertotalpages: Boolean, output: String, limit: Int): Unit = {
    println("================")
    println("|   PageRank   |")
    println("================")

    val spark = SparkSession.builder
      .appName(s"PageRank ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println("Static Page Rank => Num of iterations: " + iterations)
    println("============================================")

    //static page rank
    staticPageRank(spark, predicate, input, output, iterations, considertotalpages, limit)

    staticGraphXPageRank(spark, predicate, input, iterations, limit)
    
    spark.stop
  }

  def staticPageRank(spark: SparkSession, outlink: String, input: String, output: String, iterations: Int, considertotalpages: Boolean, limit: Int): Unit = {
    var startTime = System.currentTimeMillis()
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    val subjects = triples.getSubjects().distinct()
    val filteredTriples = triples.find(None, Some(NodeFactory.createURI(outlink)), None)

    var pairs = filteredTriples.flatMap(f => Some(f.getSubject, f.getObject))
    var links = pairs.distinct().groupByKey().cache() // RDD1 <url, outlink url> -> RDD2<url, Array(outlink url)> }
    val allPages = (subjects.union(filteredTriples.getObjects().distinct())).distinct()
   
    val numOfAllPages = allPages.distinct().count().toDouble

    val preprocessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0) // in minutes
    println("Num of all pages: " + numOfAllPages)
    println("Static PageRank Preprocessing Time: " + preprocessingTime)
    println("No of subjects: " + subjects.count())
    println("No of filtered triples: " + filteredTriples.count())

    startTime = System.currentTimeMillis()
    //set initial rank
    var pageRank = links.mapValues(v => 1.0) // RDD create the ranks <url, 1>

    var leftPages = allPages.subtract(pageRank.map(f => f._1))
    var rankedLeftPages = leftPages.distinct().flatMap(f => Some(f, 1.0))
    pageRank = pageRank.union(rankedLeftPages).distinct()

    //Calculate page rank
    for (i <- 1 to iterations) {
      var contributions = links.join(pageRank)
        .values
        .flatMap {
          case (urls, rank) =>
            val size = urls.size.toDouble
            urls.map(url => (url, rank / size))
        }

      var leftSum = 1 - 0.85
      //take in consideration number of all pages
      if (considertotalpages) {
        leftSum = leftSum / numOfAllPages
      }

      pageRank = contributions.reduceByKey(_ + _).mapValues(leftSum + 0.85 * _)

      //This calculation is done when a page has no outlinks
      val leftPages = allPages.subtract(pageRank.map(f => f._1))
      val rankedLeftPages = leftPages.distinct().flatMap(f => Some(f, leftSum))
      pageRank = pageRank.union(rankedLeftPages).distinct()
    }

    val pageRankCalculationTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("Static Page Rank calculation time: " + pageRankCalculationTime)

    startTime = System.currentTimeMillis()
    val triplesRank = pageRank
    .sortBy(_._2, ascending = false)
    .map(f => Triple.create(NodeFactory.createURI(f._1.toString()), NodeFactory.createURI("http://property/pageRank"), NodeFactory.createURI(f._2.toString())))
    val sparqlQuery = s"""SELECT *
                         WHERE { ?s ?p ?o }
                         LIMIT $limit"""
    val result = triplesRank.sparql(sparqlQuery)
    result.rdd.foreach(println(_))

    val postProcessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("\nStatic Page Rank postprocessing time: " + postProcessingTime)

    val totalRunningTime = preprocessingTime + pageRankCalculationTime + postProcessingTime
    println("Static Page Rank total running time: " + totalRunningTime + " minutes")
    println("=======================================================")
  }

  def staticGraphXPageRank(spark: SparkSession, outlink: String, input: String, iterations: Int, limit: Int): Unit = {
    //GraphX
    var startTime = System.currentTimeMillis()
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    val subjects = triples.getSubjects().distinct()
    val filteredTriples = triples.find(None, Some(NodeFactory.createURI(outlink)), None)
    val graph = filteredTriples.asGraph()

    val preprocessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0) // in minutes
    println("\nStatic GraphX Preprocessing Time: " + preprocessingTime)

    startTime = System.currentTimeMillis()
    //GraphX static pagerank
    val staticpagerank = graph.staticPageRank(iterations).vertices
    val spr = staticpagerank.join(graph.vertices)
      .map({ case (k, (r, v)) => (r, v, k) }) 

    val calculationTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("Static GraphX PageRank calculation time: " + calculationTime)

    startTime = System.currentTimeMillis()
     // sort by the rank
    val triplesPageRank = spr.sortBy(_._1, ascending = false)
    .map(f => Triple.create(NodeFactory.createURI(f._2.toString()), NodeFactory.createURI("http://property/graphXPageRank"), NodeFactory.createURI(f._1.toString())))    
 
    val sparqlQuery = s"""SELECT *
                         WHERE { ?s ?p ?o }
                         LIMIT $limit"""
    val result = triplesPageRank.sparql(sparqlQuery)
    result.rdd.foreach(println(_))

    val postProcessingTime = (System.currentTimeMillis() - startTime) / (60 * 1000.0)
    println("\nStatic GraphX Page Rank postprocessing time: " + postProcessingTime)

    val totalRunningTime = preprocessingTime + calculationTime + postProcessingTime
    println("Static GraphX Page Rank total running time: " + totalRunningTime + " minutes")
    println("================================================================")
  }

  
  case class Config(in: String = "", predicate: String = "", iterations: Int = 0, considertotalpages: Boolean = false, out: String = "", limit: Int = 0)

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("Page Rank") {

    head(" PageRank ")

    opt[String]('i', "input").required().valueName("<path>")
      .action((x, c) => c.copy(in = x))
      .text("path to file that contains the data (in N-Triples format)")
    help("help").text("prints this usage text")

    opt[Int]('n', "iterations")
      .action((x, c) => c.copy(iterations = x))
      .text("number of iterations")

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