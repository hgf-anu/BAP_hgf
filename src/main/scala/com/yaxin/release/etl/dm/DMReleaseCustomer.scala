package com.yaxin.release.etl.dm

import com.yaxin.release.constant.ReleaseConstant
import com.yaxin.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

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
	  * 和dm层的main函数类似
	  *
	  * @param args 系统参数
	  */
	def main(args:Array[String]):Unit ={
		val appName = "dm_release_job"
		val bdp_day_begin = "20190924"
		val bdp_day_end = "20190924"
		// 执行Job
		handleJobs( appName, bdp_day_begin, bdp_day_end )
	}

	//和dw层类似,处理job,spark环境的准备,和dw层类似
	def handleJobs(appName:String, bdp_day_begin:String, bdp_day_end:String):Unit ={
		var spark:SparkSession = null
		try {
			// 配置Spark参数
			val conf = new SparkConf().set( "hive.exec.dynamic.partition", "true" ).set(
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
			val timeRange = SparkHelper.rangeDates( bdp_day_begin, bdp_day_end )
			// 循环参数
			for( bdp_day <- timeRange ) {
				val bdp_date = bdp_day.toString
				handleReleaseJob( spark, appName, bdp_date )
			}
		} catch {
			case ex:Exception => {
				logger.error( ex.getMessage, ex )
			}
		} finally {
			if( spark != null ) {
				spark.stop()
			}
		}
	}

	/**
	  * 具体实现job内部的代码逻辑,业务逻辑
	  * 这里主要实现两个指标:统计渠道指标和统计目标客户多维度指标
	  *
	  * @param spark    sparksession
	  * @param appName  appname
	  * @param bdp_date 当前日期
	  */
	def handleReleaseJob(spark:SparkSession, appName:String, bdp_date:String):Unit ={
		// 导入内置函数和隐式转换
		import spark.implicits._
		import org.apache.spark.sql.functions._
		val begin:Long = System.currentTimeMillis()
		try {
			// 缓存级别
			val saveMode = SaveMode.Overwrite
			val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
			//1.获取DW层的表数据
			// 1.1获取DW层的目标客户的原始表数据
			val customerColumns:ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
			//1.2获取那一天的数据
			val customerCondition:Column = col( s"${ReleaseConstant.DEF_SOURCE_PARTITION}" ) === lit( bdp_date )
			val customerReleaseDF:DataFrame = SparkHelper.readTableData( spark,
			                                                             ReleaseConstant.DW_RELEASE_CUSTOMER,
			                                                             customerColumns )
			                                  //加上前面写的条件1.2
			                                  .where( customerCondition )
			                                  //把DW层的表进行缓存,不用每次执行的时候都去查询一遍
			                                  .persist()
			//1.3测试数据是否正确
			//customerReleaseDF.show( 10, truncate = false)
			println( "DW========================" )

			//2.统计渠道指标
			//2.1把需要分组的列放入Seq里,这种写法比较规范,也可以使用多个col(字段名)..来代替这种方式
			// ^编译错误收集:s"${ReleaseConstant.COL_RELEASE_SOURCES}"是类型不匹配的错误,因为这里要存储的类型是Column,而这里参数是String
			//解决方案:把s改为$,表示后面的参数是代表一列,一种语法糖,记住即可
			val customerSourceGroupColumns:Seq[Column] = Seq[Column]( $"${ReleaseConstant.COL_RELEASE_SOURCES}",
			                                                          $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
			                                                          $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}" )


			val customerSourceDMDF:DataFrame = customerReleaseDF
			                                   //这里的_*表示集合的每一个元素
			                                   .groupBy( customerSourceGroupColumns:_* )
			                                   //2.2按照设备号进行聚合
			                                   .agg( countDistinct( ReleaseConstant.COL_RELEASE_DEVICE_NUM ).alias( s"${ReleaseConstant.COL_RELEASE_USER_COUNT}" ),
			                                         count( ReleaseConstant.COL_RELEASE_DEVICE_NUM ).alias( s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}" ) )
			                                   //2.3查询条件,和where类似,但是它能容错,如果传入的参数是null则不造成影响
			                                   .withColumn( s"${ReleaseConstant.DEF_SOURCE_PARTITION}",
			                                                lit( bdp_date ) )
			                                   //2.4查询出字段结果
			                                   .selectExpr( customerColumns:_* )
			println( "DM_Source=============================" )
			//测试,输出到控制台
			customerSourceDMDF.show( 10, false )
			//2.5写入到hive表中
			//SparkHelper.writeTableData(customerSourceDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCE,saveMode)
			//3.统计目标客户多维度指标（和前面有大量重复的代码）
			//3.1 把分组条件封装成字段数组,注意名字是没有Source的
			val customerGroupColumns:Seq[Column] = Seq[Column]( $"${ReleaseConstant.COL_RELEASE_SOURCES}",
			                                                    $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
			                                                    $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
			                                                    $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
			                                                    $"${ReleaseConstant.COL_RELEASE_GENDER}",
			                                                    $"${ReleaseConstant.COL_RELEASE_AREA_CODE}" )

			//3.2 返回表需要的字段数组
			val customerCubeColumns:ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMCustomerCudeColumns()

			//3.3 限制条件和聚合操作和分组操作
			val customerCubeDF:DataFrame = customerReleaseDF.groupBy( customerGroupColumns:_* ).agg( countDistinct( col(
				ReleaseConstant.COL_RELEASE_DEVICE_NUM ) ).alias( s"${ReleaseConstant.COL_RELEASE_USER_COUNT}" ),
			                                                                                         count( col(
				                                                                                         ReleaseConstant.COL_RELEASE_DEVICE_NUM ) ).alias(
				                                                                                         s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}" ) ).withColumn(
				s"${ReleaseConstant.DEF_SOURCE_PARTITION}",
				lit( bdp_date ) ).selectExpr( customerCubeColumns:_* )

			//3.4存入hive表中
			//SparkHelper.writeTableData(customerCubeDF,ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE,saveMode)
		} catch {
			case ex:Exception => {
				logger.error( ex.getMessage, ex )
			}
		} finally {
			if( spark != null ) {
				spark.stop()
			}
		}
	}
}
