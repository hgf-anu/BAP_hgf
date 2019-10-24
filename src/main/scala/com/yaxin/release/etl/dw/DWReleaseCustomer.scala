package com.yaxin.release.etl.dw

import com.yaxin.release.constant.ReleaseConstant
import com.yaxin.release.enums.ReleaseStatusEnum
import com.yaxin.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class DWReleaseCustomer{

}

/**
  * 目标客户
  * release_status=01
  */
object DWReleaseCustomer{
	// 获得日志处理对象
	private val logger:Logger = LoggerFactory.getLogger( DWReleaseCustomer.getClass )

	/**
	  * 入口函数
	  *
	  * @param args 系统设置参数,使用jar包方式的可以在命令行输入
	  */
	def main(args:Array[String]):Unit ={
		val appName = "dw_release_job"
		val bdp_day_begin = "20190924"
		val bdp_day_end = "20190924"
		//执行job作业
		handleJobs( appName, bdp_day_begin, bdp_day_end )
	}

	/**
	  * 投放目标客户
	  *
	  * @param appName       appName
	  * @param bdp_day_begin 开始的日期
	  * @param bdp_day_end   结束的日期
	  *                      注意:左闭右开
	  */
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
			for ( bdp_day <- timeRange ) {
				val bdp_date:String = bdp_day
				handleReleaseJob( spark, appName, bdp_date )
			}
		} catch {
			//收集错误信息到日志文件
			case ex:Exception => logger.error( ex.getMessage, ex )
		} finally {
			if ( spark != null ) {
				spark.stop()
			}
		}
	}

	/**
	  * 处理表数据
	  *
	  * @param spark    SparkSession
	  * @param appName  appName
	  * @param bdp_date 分区键:当前日期,每次处理一次日期[步长可以是自定义]
	  */
	def handleReleaseJob(spark:SparkSession, appName:String, bdp_date:String):Unit ={
		//1.一些准备工作
		//1.1导入隐式转换,有SparkSession的参数就要先做好准备
		import spark.implicits._
		import org.apache.spark.sql.functions._
		//1.2获取当前时间,用来计算处理任务的时长,可以观察是是否存在数据倾斜
		val begin:Long = System.currentTimeMillis()
		try {
			//1.3设置缓存级别/模式
			//1.3.1设置缓存级别:使用常量类进行封装,在dw层到dm层中使用
			//val storagelevel:StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
			//1.3.2设置缓存模式:SaveMode枚举类,有四种状态
			//Append,Overwrite,ErrorIfExists,Ignore
			val saveMode:SaveMode = SaveMode.Overwrite

			//1.4 获取日志字段数据
			val customerColumns:ArrayBuffer[String] = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()

			// 2.设置where条件:(1)日期,(2)发布状态:01
			//col()是隐式转换函数,这里常量指的是release_status
			//lit()是把一个String转换为对应的字段名,或者scala的标签转换为字段名
			//'===' 三等于号是指
			val customerReleaseCondition:Column = ( col( s"${ReleaseConstant.DEF_PARTITION}" ) === lit( bdp_date ) and col(
				s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}" )
			  //ReleaseStatusEnum是枚举类,CUSTOMER.getCode对应的是目标客户的代号01
			  === lit( ReleaseStatusEnum.CUSTOMER.getCode ) )

			//3.编写工具类读取一张表的数据,返回值是一个DataFrame
			val customerReleaseDF:Dataset[Row] = SparkHelper.readTableData( spark,
			                                                                ReleaseConstant.ODS_RELEASE_SESSION,
			                                                                customerColumns )
			                                                //注意:为什么不传递条件到函数里?因为可能出现多个限制条件,写在外面不影响封装性.会变动的代码放外面
			                                                .where( customerReleaseCondition )
			                                                //重分区,根据默认分区数进行分区,会产生shuffle
			                                                .repartition( ReleaseConstant.DEF_SOURCE_PARTITION )

			//查看结果
			customerReleaseDF.show( 10, truncate = false )
			//4.编写工具类函数,把过滤数据存储到hive表dw层中,支持设置存储模式
			SparkHelper.writeTableData( customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode )

			println( "DWReleaseDF=====================================" )

		} catch {
			// 错误信息处理
			case ex:Exception => logger.error( ex.getMessage, ex )
		} finally {
			println( s"任务处理时长:$appName,bdp_day = $bdp_date,${System.currentTimeMillis() - begin}" )
		}
	}

}
