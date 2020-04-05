package MergeFile.utils

import org.apache.hadoop.fs._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Init {
  def initSpark(appName:String): SparkContext ={
    val conf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc
  }
  def getFs(sc: SparkContext): FileSystem ={
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs
  }
  def getSQL(sc: SparkContext): SQLContext ={
    val sql = new SQLContext(sc)
    sql
  }
  /*
  * 计算输入目录文件大小，根据128M就散partition数量
  * */
  def getPartition(src:String,fs:FileSystem): Int ={
    val summary = fs.getContentSummary(new Path(src))
    val size: Long = summary.getLength/1024/1024
    val num: Int = size / 128 match {
      case 0 => 1
      case _ => (size / 128).toInt
    }
    num
  }
  /*
  * 将临时目录中的结果文件mv到源目录，并以result-为文件前缀
  * */
  def mv(fromDir:String,toDir:String,fs:FileSystem): Unit ={
    val srcFiles: Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir)))
    for (p <- srcFiles){
      if(p.getName.startsWith("part")){
        fs.rename(p,new Path(toDir + "/result-" + p.getName))
      }
    }
  }
  /*
  * 删除原始小文件
  * */
  def del(fromDir:String,fs:FileSystem): Unit ={
    val files = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir)))
    for (f <- files){
      fs.delete(f,true)
    }
  }
}








