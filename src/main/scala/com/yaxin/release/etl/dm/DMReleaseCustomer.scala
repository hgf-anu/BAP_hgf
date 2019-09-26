package com.yaxin.release.etl.dm

import com.yaxin.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class DMReleaseCustomer{

}

/**
  * 数据流向:从dw层到dm集市层
  * 大部分微聚在dw层,dm层是针对需求进行一个操作
  */
object DMReleaseCustomer{
	//日志
	private val logger:Logger = LoggerFactory.getLogger( DMReleaseCustomer.getClass )


/**
*和dm层的main函数类似
 *
* @param args 系统参数
  */
def main(args: Array[String]): Unit = {
		val appName = "dm_release_job"
		val bdp_day_begin = "20190924"
		val bdp_day_end = "20190924"
		// 执行Job
		handleJobs(appName,bdp_day_begin,bdp_day_end)
	}

//和dw层类似,处理job,spark环境的准备,和dw层类似
	def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
		var spark:SparkSession =null
		try{
			// 配置Spark参数
			val conf = new SparkConf()
			           .set("hive.exec.dynamic.partition", "true")
			           .set("hive.exec.dynamic.partition.mode", "nonstrict")
			           .set("spark.sql.shuffle.partitions", "32")
			           .set("hive.merge.mapfiles", "true")
			           .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
			           .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
			           .set("spark.sql.crossJoin.enabled", "true")
			           .setAppName(appName)
			           .setMaster("local[*]")
			// 创建上下文
			spark = SparkHelper.createSpark(conf)
			// 解析参数
			val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
			// 循环参数
			for(bdp_day <- timeRange){
				val bdp_date = bdp_day.toString
				handleReleaseJob(spark,appName,bdp_date)
			}
		}catch {
			case ex:Exception=>{
				logger.error(ex.getMessage,ex)
			}
		}finally {
			if(spark != null){
				spark.stop()
			}
		}
	}

	/**
	*具体实现job内部的代码逻辑
* @param spark sparksession
* @param appName appname
* @param bdp_date 当前日期
	  */
	def handleReleaseJob(spark: SparkSession, appName: String, bdp_date: String) :Unit= {
		// TODO


	}
}
