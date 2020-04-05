package MergeFile

import MergeFile.utils.Init._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode}

/*
* 合并日志文件，支持parquet格式
* 1.从fromDir加载数据
* 2.将结果输出到tmpDir，文件以 result- 开头
* 3.mv tmpDir中文件到fromDir中
* 4.删除fromDir目录中非 result- 开头的文件
* Create by Jerry on 2019/03
* */
object CrushFile {
  def main(args: Array[String]): Unit = {
    if (args.length < 3){
      System.err.println("Usage:" + "| CrushFile <dfsUri> <abFromDir> <abTmpDir>")
      System.exit(1)
    }
//    val Array(dfsUri,fromDir,tmpDir) = args
//    val dfsUri = "hdfs://10.4.1.4:9000"
//    val fromDir ="/test/dw/2015/10/28/topic_common_event/13"
//    val tmpDir = "/test/tmp"
    val fromDir = "/test/dw/2019/10/28/topic_common_event/13"
    val tmDir = "/test/tmp"
    val sc: SparkContext = initSpark("CrushFile")
//    sc.hadoopConfiguration.set("fs.defaultFS",dfsUri)
    val fs = getFs(sc)
    val sql = getSQL(sc)
    sql.setConf("spark.sql.parquet.compression.codec","snappy")

    val df: DataFrame = sql.read.parquet(fromDir)
    df.repartition(getPartition(fromDir,fs)).write.format("parquet").mode(SaveMode.Overwrite).save(tmDir)
    //删除源目录 ，移动新目录到源路径下
    del(fromDir,fs)
    mv(fromDir,tmDir,fs)
  }
}
