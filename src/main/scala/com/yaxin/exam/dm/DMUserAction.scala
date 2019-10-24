package com.yaxin.exam.dm

import com.yaxin.release.etl.dm.DMReleaseCustomer
import com.yaxin.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class DMUserAction{

}

object DMUserAction{
	private val logger:Logger = LoggerFactory.getLogger( DMReleaseCustomer.getClass )

	def main(args:Array[String]):Unit ={
		val appName = "dm_user_action"
		val bdp_day_begin = "20190902"
		val bdp_day_end = "2019098"
		// 执行Job
		handleJobs( appName, bdp_day_begin, bdp_day_end )
	}

	def handleJobs(appName:String, bdp_day_begin:String, bdp_day_end:String):Unit ={
		var spark:SparkSession = null
		try {
			// 配置Spark参数
			val conf:SparkConf = new SparkConf().set( "hive.exec.dynamic.partition", "true" ).set(
				"hive.exec.dynamic.partition.mode",
				"nonstrict" ).set( "spark.sql.shuffle.partitions", "32" ).set( "hive.merge.mapfiles", "true" ).set(
				"hive.input.format",
				"org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" ).set( "spark.sql.autoBroadcastJoinThreshold",
			                                                                 "50485760" ).set(
				"spark.sql.crossJoin.enabled",
				"true" ).setAppName( appName ).setMaster( "local[*]" )
			// 创建上下文
			spark = SparkHelper.createSpark( conf )
			// 解析参数
			val timeRange:Seq[String] = SparkHelper.rangeDates( bdp_day_begin, bdp_day_end )
			// 循环参数
			for( bdp_day <- timeRange ) {
				val bdp_date:String = bdp_day.toString
				handleReleaseJob( spark, appName, bdp_date )
			}
		} catch {
			case ex:Exception => logger.error( ex.getMessage, ex )
		} finally {
			if( spark != null ) {
				spark.stop()
			}
		}
	}


	def handleReleaseJob(spark:SparkSession, appName:String, bdp_date:String):Unit ={
		// 导入内置函数和隐式转换
		import org.apache.spark.sql.functions._
		//val begin:Long = System.currentTimeMillis()
		try {
			// 缓存级别
			//val saveMode = SaveMode.Overwrite
			//val storageLevel:StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

			val userActionColumns:ArrayBuffer[String] = DMUserActionHelper.selectDMUserActionColumns()

			val userActionCondition:Column = col( "bdp_day" ) === lit( bdp_date )

			val customerReleaseDF:DataFrame = SparkHelper.readTableData( spark,
			                                                             "dm_user_action",
			                                                             userActionColumns ).where( userActionCondition ).persist()

			val customerSourceDMDF:DataFrame = customerReleaseDF.groupBy( col( "user" ) ).agg( sum( "uscore" ) ).withColumn(
				"bdp_day",
				lit( bdp_date ) )
			                                                    //2.4查询出字段结果
			                                                    .selectExpr( userActionColumns:_* )
			//测试,输出到控制台
			customerSourceDMDF.show( 10, truncate = false )
		} catch {
			case ex:Exception => logger.error( ex.getMessage, ex )
		} finally {
			if( spark != null ) {
				spark.stop()
			}
		}
	}
}
