import org.apache.spark._
import org.apache.hadoop.fs._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.util.control._

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

        val nodeCountAccumulator = sc.accumulator(0, "TotalNodeCount")

        val parsedTitle = inputFile.map(line => parseTitle(line))
        parsedTitle.cache
        parsedTitle.foreach( title =>{nodeCountAccumulator += 1})

        val allTitles = parsedTitle.collect()

        val broadcastedTitles = sc.broadcast( allTitles )
        val broadcastedTotalNodeCount = sc.broadcast( nodeCountAccumulator.value )

        var graphStructure = inputFile.map(line =>{
            val initScore : Double = 1.0/broadcastedTotalNodeCount.value
            val allLinks = parseLink(line, broadcastedTitles.value)
            ( parseTitle(line), (initScore,initScore, allLinks) )
        })

        val deadEndScoreAccumulator = sc.accumulator(0.0, "TotalDeadEndScore")
        val deadEndCountAccumulator = sc.accumulator(0,"TotalDeadEndCount")
        val pageRankAccumulator = sc.accumulator(0.0,"TotalPageRankCount")
        val convergenceErrorAccumulator = sc.accumulator(0.0,"TotalConvergenceError")

        var iterationCount:Int = 0
        val loop = new Breaks;
        loop.breakable {
        while(iterationCount < 1){

            deadEndScoreAccumulator.setValue(0)
            deadEndCountAccumulator.setValue(0)
            pageRankAccumulator.setValue(0)
            convergenceErrorAccumulator.setValue(0)

            graphStructure.filter(data=>{data._2._3.size==0}).foreach( data => {
                deadEndScoreAccumulator += data._2._2
                deadEndCountAccumulator += 1
            })

            val broadcastedTotalDeadEndScore = sc.broadcast(deadEndScoreAccumulator.value)

            graphStructure = graphStructure.flatMap( data => {
                val output = ListBuffer[ (String,(Double, Double, List[String])) ]();
                val value = data._2
                val links = value._3
                output +=  ( (data._1, (value._1,0.0,links) ) )
                if (links.size > 0){
                    val distributedScore:Double = value._2/links.size
                    links.foreach( link =>{
                        output+= ( (link, (0.0,distributedScore, List[String]())) )
                    })
                }
                output.toList
            })
            .reduceByKey( (v1,v2) => mergePageRank(v1,v2) )
            .map(
                addRankWalkAndDeadEndScore(_, broadcastedTotalDeadEndScore.value, broadcastedTotalNodeCount.value)
            )

            graphStructure.foreach( data => {
                pageRankAccumulator += data._2._2
                convergenceErrorAccumulator += math.abs(data._2._1 - data._2._2)
            })
            graphStructure = graphStructure.map( data=>{
                (data._1, (data._2._2,data._2._2,data._2._3) )
            })

            iterationCount = iterationCount + 1
            println("==="+iterationCount+"===")
            println("Totla Dead End Score = " + deadEndScoreAccumulator.value )
            println("Totla Dead End Count = " + deadEndCountAccumulator.value )
            println("Totla Page Rank = " + pageRankAccumulator.value )
            println("Totla Convergence Error = " + convergenceErrorAccumulator.value )
            if (convergenceErrorAccumulator.value < 0.001){
                loop.break
            }
        }
        }
        graphStructure.sortBy( data => (-1*data._2._2, data._1) ).map( data => data._1+"\t"+data._2._2 ).saveAsTextFile(outputPath)
        sc.stop();
    }

    def addRankWalkAndDeadEndScore( data : (String, (Double, Double, List[String])), totalDeadEndScore:Double, totalNodeCount:Double ) : (String, (Double, Double, List[String])) ={
        val alpha:Double = 0.85
        val newPageRank = alpha*data._2._2 + ( (1-alpha) + alpha*totalDeadEndScore )/totalNodeCount
        ( data._1, (data._2._1, newPageRank, data._2._3) )
    }

    def mergePageRank(v1 : (Double, Double, List[String]), v2 : (Double, Double, List[String]) ) : (Double, Double, List[String]) = {
        if ( v1._3.size > 0 && v2._3.size > 0){
            throw new Exception("MYERROR: Concat 2 non-empty list\n"+v1._3+"\n"+v2._3)
        }

        val newPreviousScore = v1._1 + v2._1
        val newScore = v1._2 + v2._2
        if ( v1._3.size > 0 ){
            (newPreviousScore, newScore, v1._3)
        }
        else{
            (newPreviousScore, newScore, v2._3)
        }

    }

    def parseTitle(line: String) : String = {
        val titlePattern = new Regex("<title>(.+?)</title>")
        titlePattern.findFirstMatchIn(line) match {
            case Some(m) =>
                capitalizeFirstLetter(covertSprcailLetter(m.group(1)))
            case _ =>
                throw new Exception("title tag not found")
        }
    }

    def parseLink(line : String, avalableTitles : Array[String]) : List[String] = {
        val linkPattern = new Regex("\\[\\[(.+?)([\\|#]|\\]\\])")
        //var links = ListBuffer[String]()
        linkPattern.findAllIn(line).matchData
        .map(word => capitalizeFirstLetter(covertSprcailLetter(word.group(1))))
        .filter( word => avalableTitles.contains(word) ).toList
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
}

