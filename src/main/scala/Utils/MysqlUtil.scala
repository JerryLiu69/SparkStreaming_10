package Utils

import scalikejdbc.{DB, SQL}

/*
* MysqlPool 可以直接使用scalikeJdbc这个包
* */

object MysqlUtil {
  def insert()={
    //插入使用localTx
    DB.localTx(implicit session => {
      SQL("insert into aa(word,num) values(?,?)").bind("bigdata",10).update().apply()
    })
  }
  def update()={
    //更新使用的是autoCommit
    DB.autoCommit(implicit session=>{
      // SQL里面是普通的sql语句，后面bind里面是语句中"?"的值，update().apply()是执行语句
      SQL("update aa set num = ? where word = ?").bind(20,"bigdata").update().apply()
    })
  }
  def delete()={
    // 删除使用的也是autoCommit
    DB.autoCommit(implicit session =>{
      SQL("delete from aa where word = ?").bind("bigdata").update().apply()
    })
  }
  def query()={
    //读取使用的是readOnly
    val users: List[Words] = DB.readOnly(implicit session => {
      SQL("select * from aa").map(rs => {
        Words(
          rs.string("word"),
          rs.int("num")
        )
      }).list().apply()
    })
    users.foreach(println)
  }
  case class Words(word:String,num:Int)
}
