package com.yaxin.exam.dw

import com.yaxin.release.etl.dw.DWReleaseCustomer
import com.yaxin.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class DWUserAction{

}

object DWUserAction{
	private val logger:Logger = LoggerFactory.getLogger( DWReleaseCustomer.getClass )

	def main(args:Array[String]):Unit ={
		val appName = "dw_user_action_log"
		val bdp_day_begin = "20190902"
		val bdp_day_end = "20190908"
		//执行job作业
		handleJobs( appName, bdp_day_begin, bdp_day_end )
	}


	def handleJobs(appName:String, bdp_day_begin:String, bdp_day_end:String):Unit ={
		var spark:SparkSession = null
		try {
			// 1.配置Spark参数,同时也配置了hive的参数
			val conf:SparkConf = new SparkConf().set( "hive.exec.dynamic.partition", "true" ).set(
				"hive.exec.dynamic.partition.mode",
				"nonstrict" ).set( "spark.sql.shuffle.partitions", "32" ).set( "hive.merge.mapfiles", "true" ).set(
				"hive.input.format",
				"org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" ).set( "spark.sql.autoBroadcastJoinThreshold",
			                                                                 "50485760" ).set(
				"spark.sql.crossJoin.enabled",
				"true" ).setAppName( appName ).setMaster( "local[*]" )

			// 2.创建上下文,使用自定义工具类函数
			spark = SparkHelper.createSpark( conf )

			// 3.处理时间
			val timeRange:Seq[String] = SparkHelper.rangeDates( bdp_day_begin, bdp_day_end )

			// 4.循环处理每一天的数据
			for( bdp_day <- timeRange ) {
				val bdp_date:String = bdp_day
				handleReleaseJob( spark, appName, bdp_date )
			}
		} catch {
			//收集错误信息到日志文件
			case ex:Exception =>
				logger.error( ex.getMessage, ex )
		}finally {
			if(spark != null){
				spark.stop()
			}
		}
	}


	def handleReleaseJob(spark:SparkSession, appName:String, bdp_date:String) :Unit={
		import org.apache.spark.sql.functions._
		val begin:Long = System.currentTimeMillis()
		try{
			val saveMode:SaveMode = SaveMode.Overwrite

			val userActionColumns:ArrayBuffer[String] = DWUserActionHelper.selectDWUserActionColumns()

			val userActionCondition: Column = col(s"bdp_day") === lit(bdp_date)
			//3.编写工具类读取一张表的数据,返回值是一个DataFrame
			val userActionDF:Dataset[Row] = SparkHelper.readTableData( spark, "ods_user_action", userActionColumns ).where( userActionCondition ).repartition( 4 )

			//查看结果
			userActionDF.show( 10, truncate = false)
			//4.编写工具类函数,把过滤数据存储到hive表dw层中,支持设置存储模式
			SparkHelper.writeTableData(userActionDF,"ods_user_action_trans",saveMode)


		}catch {
			// 错误信息处理
			case ex:Exception =>
				logger.error(ex.getMessage,ex)
		}finally {
			println( s"任务处理时长:$appName,bdp_day = $bdp_date,${System.currentTimeMillis() - begin}" )
		}
	}

}
