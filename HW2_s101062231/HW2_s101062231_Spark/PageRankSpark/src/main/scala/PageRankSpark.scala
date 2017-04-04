import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageRankSpark {
  def main(args: Array[String]) {
    val filePath = args(0)

    val outputPath = "PageRankSpark"

    val conf = new SparkConf().setAppName("Page Rank Spark")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }


    val lines = sc.textFile(filePath, sc.defaultParallelism * 10)

    val linkRegex = "\\[\\[(.+?)([\\|#]|\\]\\])".r
    val titleRegex = "<title>(.+?)</title>".r

    //val newLines = lines.toString().replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
    
    val infoList = lines.map (line => { 
      val title = titleRegex.findFirstMatchIn(line).get.group(1)
      title.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'").capitalize
      
      val outLink = linkRegex.findAllIn(line).toArray
          .map{link => link.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'").replaceAll("\\[\\[|\\]\\]|\\||#", "").capitalize}
          .filter { _.length > 0 }
      outLink.map { link => (link, title) }
              .+:(title, "<title>") //map 回傳一個 Array(Array(link, title), Array(link, title), Array(link, title))
    })
    .flatMap(x => x).groupByKey().filter(_._2.exists { _ == "<title>" })
    .map(list => {
          list._2.toArray.filter(_ != "<title>").map(tmp => (tmp, list._1)).+:(list._1, "<title>")
    })
    .flatMap(x => x).groupByKey().map(tmp => (tmp._1, tmp._2.toArray.filter { _ != "<title>" })) //格式: (title, (outLink))

    infoList.cache();



    val damping = 0.85;
    
    val totalNum = infoList.count();

    var pageRankList = infoList.map(x => (x._1, (1.0 / totalNum, x._2)));  //格式: (title, (outLinks,PageRank))
    var error = 1.0;
    var runs = 1;

    pageRankList.cache();

    while (error > 0.001) {

      val danglingRankSum = pageRankList.filter(_._2._2.length == 0).map(_._2._1).reduce(_ + _) / totalNum; //抓outLink長度為0的項取出PageRank .reduce(_ + _)加總

      var tmpPageRankList = pageRankList.map(list => {
        list._2._2.map { outLinks => (outLinks, damping * list._2._1 / list._2._2.length) } //(link, a*(PR/linkCount))
                  .+:(list._1, (1 - damping)/totalNum + damping * danglingRankSum)          //多加一個(title, 公式中其餘部分)
      }).flatMap(x => x).reduceByKey(_ + _);

      error = (tmpPageRankList.join(pageRankList.map(x => (x._1, x._2._1)), sc.defaultParallelism * 10)).map(x => (x._2._1 - x._2._2).abs).reduce(_ + _);
      pageRankList = tmpPageRankList.join(pageRankList.map(x => (x._1, x._2._2)))


      System.out.println("**********************");
      System.out.println("Runs : " + runs);
      System.out.println("error: " + error);
      System.out.println("**********************");

      runs = runs+1;

    }

    val res = pageRankList.map(x => (x._1, x._2._1));
    res.sortBy({ case (page, pr) => (-pr, page) }, true, sc.defaultParallelism * 10).map(x => x._1 + "\t" + x._2).saveAsTextFile(outputPath);
    sc.stop
  }
}
