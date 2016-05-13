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
            ( parseTitle(line), new ScoreLinksPair(initScore,initScore, allLinks) )
        })

        val deadEndScoreAccumulator = sc.accumulator(0.0, "TotalDeadEndScore")
        val deadEndCountAccumulator = sc.accumulator(0,"TotalDeadEndCount")
        val pageRankAccumulator = sc.accumulator(0.0,"TotalPageRankCount")
        val convergenceErrorAccumulator = sc.accumulator(0.0,"TotalConvergenceError")

        var iterationCount:Int = 0
        val loop = new Breaks;
        loop.breakable {
        while(iterationCount<1){

            deadEndScoreAccumulator.setValue(0)
            deadEndCountAccumulator.setValue(0)
            pageRankAccumulator.setValue(0)
            convergenceErrorAccumulator.setValue(0)

            graphStructure.filter(data=>{data._2.links.size==0}).foreach( data => {
                deadEndScoreAccumulator += data._2.score
                deadEndCountAccumulator += 1
            })

            val broadcastedTotalDeadEndScore = sc.broadcast(deadEndScoreAccumulator.value)

            graphStructure = graphStructure.flatMap( data => {
                val output = ListBuffer[ (String,ScoreLinksPair) ]();
                val value = data._2

                output +=  ( (data._1, new ScoreLinksPair(value.previousScore,0.0,value.links) ) )
                if (value.links.size > 0){
                    val distributedScore:Double = value.score/value.links.size
                    value.links.foreach( link =>{
                        output+= ( (link, new ScoreLinksPair(0.0,distributedScore, List[String]())) )
                    })
                }
                output.toList
            })
            .reduceByKey( (v1,v2) => mergePageRank(v1,v2) )
            .map(
                addRankWalkAndDeadEndScore(_, broadcastedTotalDeadEndScore.value, broadcastedTotalNodeCount.value)
            )

            graphStructure.foreach( data => {
                pageRankAccumulator += data._2.score
                convergenceErrorAccumulator += math.abs(data._2.previousScore - data._2.score)
            })
            graphStructure = graphStructure.map( data=>{
                (data._1, new ScoreLinksPair(data._2.score,data._2.score,data._2.links) )
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
        graphStructure.sortBy( data => (-1*data._2.score, data._1) ).map( data => data._1+"\t"+data._2.score ).saveAsTextFile(outputPath)
        sc.stop();
    }

    def addRankWalkAndDeadEndScore( data : (String, ScoreLinksPair), totalDeadEndScore:Double, totalNodeCount:Double ) : (String, ScoreLinksPair) ={
        val alpha:Double = 0.85
        val newPageRank = alpha*data._2.score + ( (1-alpha) + alpha*totalDeadEndScore )/totalNodeCount
        ( data._1, new ScoreLinksPair(data._2.previousScore, newPageRank, data._2.links) )
    }

    def mergePageRank(v1 : ScoreLinksPair, v2 : ScoreLinksPair ) : ScoreLinksPair = {
        if ( v1.links.size > 0 && v2.links.size > 0){
            throw new Exception("MYERROR: Concat 2 non-empty list\n"+v1.links+"\n"+v2.links)
        }

        val newPreviousScore = v1.previousScore + v2.previousScore
        val newScore = v1.score + v2.score
        if ( v1.links.size > 0 ){
            new ScoreLinksPair(newPreviousScore, newScore, v1.links)
        }
        else{
            new ScoreLinksPair(newPreviousScore, newScore, v2.links)
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

    class ScoreLinksPair( _previousScore:Double, _score:Double, _links:List[String] ) extends Serializable{
        var previousScore:Double = _previousScore
        var score:Double = _score
        var links:List[String] = _links
    }

}

