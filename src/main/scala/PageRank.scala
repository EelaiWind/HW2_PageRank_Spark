import org.apache.spark._
import org.apache.hadoop.fs._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

object PageRank {
    def main(args: Array[String]) {
        val inputPath = args(0)
        val outputPath = args(1)

        val conf = new SparkConf().setAppName("Pagerank Application")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        var hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

        // Read input file
        val inputFile = sc.textFile(inputPath, sc.defaultParallelism)

        val titleAccumulator = sc.accumulator(Set[String](), "TotalTitles")(TitleAccumulatorParam)
        val nodeCountAccumulator = sc.accumulator(0, "TotalNodeCount")

        inputFile.map(line => parseTitle(line)).foreach( title =>{
            titleAccumulator += Set(title)
            nodeCountAccumulator += 1
            println(title)
        })

        val broadcastedTitles = sc.broadcast( titleAccumulator.value )
        val broadcastedTotalNodeCount = sc.broadcast( nodeCountAccumulator.value )

        var graphStructure = inputFile.map(line =>{
            val initScore : Double = 1.0/broadcastedTotalNodeCount.value
            val allLinks = parseLink(line, broadcastedTitles.value)
            ( parseTitle(line), (initScore, allLinks) )
        })

        val deadEndScoreAccumulator = sc.accumulator(0.0, "TotalDeadEndScore")
        val deadEndCountAccumulator = sc.accumulator(0,"TotalDeadEndCount")
        val pageRankAccumulator = sc.accumulator(0.0,"TotalPageRankCount")

        graphStructure.filter(data=>{data._2._2.size==0}).foreach( data => {
            deadEndScoreAccumulator += data._2._1
            deadEndCountAccumulator += 1
        })

        val broadcastedTotalDeadEndScore = sc.broadcast(deadEndScoreAccumulator.value)

        graphStructure = graphStructure.flatMap( data => {
            val output = ListBuffer[ (String, (Double, List[String])) ]();
            val links:List[String] = data._2._2

            output +=  ( (data._1, (0.0, links) ) )
            if (links.size > 0){
                val distributedScore:Double = data._2._1/links.size
                links.foreach( link =>{
                    output+= ( (link, (distributedScore, List[String]())) )
                })
            }
            output.toList
        })
        .reduceByKey( (v1,v2) => mergePageRank(v1,v2) )
        .map(
            addRankWalkAndDeadEndScore(_, broadcastedTotalDeadEndScore.value, broadcastedTotalNodeCount.value)
        )
        graphStructure.foreach( data => {pageRankAccumulator+=data._2._1} )

        graphStructure.sortBy( data => (-1*data._2._1, data._1) ).map( data => data._1+"\t"+data._2._1 ).saveAsTextFile(outputPath)
        sc.stop();

        println("Totla Dead End Score = " + deadEndScoreAccumulator.value )
        println("Totla Dead End Count = " + deadEndCountAccumulator.value )
        println("Totla Page Rank = " + pageRankAccumulator.value )
    }

    def addRankWalkAndDeadEndScore( data : (String, (Double, List[String])), totalDeadEndScore:Double, totalNodeCount:Double ) : (String, (Double, List[String])) ={
        val alpha:Double = 0.85
        val newPageRank = alpha*data._2._1 + ( (1-alpha) + alpha*totalDeadEndScore )/totalNodeCount
        ( data._1, (newPageRank, data._2._2) )
    }

    def mergePageRank(v1 : (Double, List[String]), v2 : (Double, List[String]) ) : (Double, List[String]) = {
        if ( v1._2.size > 0 && v2._2.size > 0){
            throw new Exception("MYERROR: Concat 2 non-empty list\n"+v1._2+"\n"+v2._2)
        }

        if ( v1._2.size > 0 ){
            (v1._1+v2._1, v1._2)
        }
        else{
            (v1._1+v2._1, v2._2)
        }

    }

    def parseTitle(line: String) : String = {
        val titlePattern = new Regex("<title>(.+?)</title>")
        titlePattern.findFirstMatchIn(line) match {
            case Some(m) =>
                println("Hello")
                capitalizeFirstLetter(covertSprcailLetter(m.group(1)))
            case _ =>
                throw new Exception("title tag not found")
        }
    }

    def parseLink(line : String, avalableTitles : Set[String]) : List[String] = {
        val linkPattern = new Regex("\\[\\[(.+?)([\\|#]|\\]\\])")
        var links = ListBuffer[String]()
        for ( words <- linkPattern.findAllIn(line).matchData ){
            val title = capitalizeFirstLetter(covertSprcailLetter(words.group(1)))
            if ( avalableTitles.contains(title) ){
                links += title
            }
        }
        links.toList
    }

    def covertSprcailLetter(str : String) : String = {
        str.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'")
    }
    def capitalizeFirstLetter(str: String) : String = {
        val firstChar = str.charAt(0)
        if ( (firstChar>='a' && firstChar<='z') || (firstChar>='A' && firstChar<='Z') ){
            str.substring(0,1).toUpperCase() + str.substring(1,str.length)
        }
        else{
            str
        }
    }

    object TitleAccumulatorParam extends AccumulatorParam[Set[String]] {
        def zero(initValue : Set[String]) : Set[String] = {
            initValue
        }

        def addInPlace(v1: Set[String], v2: Set[String]) : Set[String] = {
            v1 ++ v2
        }
    }
}

