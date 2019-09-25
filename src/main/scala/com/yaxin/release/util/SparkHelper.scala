package com.yaxin.release.util

import com.sun.xml.bind.v2.TODO
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Spark工具类:
  * (1)创建sparksession
  * (2)生成时间长度的数组
  * (3)读hive表中的数据
  * (4)写入hive表中的数据
  *
  */
object SparkHelper{

	/**
	*日志对象
	  */
	private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

	/**
	  * 创建SparkSession
	  *
	  * @param conf 配置文件
	  * @return
	  */
	def createSpark(conf:SparkConf):SparkSession = {
		val spark: SparkSession = SparkSession.builder()
		            .config(conf)
		            .enableHiveSupport()
		            .getOrCreate()

		// TODO 加载自定义函数


		spark
	}

	/**
	*   时间参数校验
	* @param begin 开始时间
	* @param end 结束时间
	* @return 返回值是一个包含一个或者多个日期的集合
	*/
	def rangeDates(begin:String, end:String) :Seq[String]= {
		val bdp_days = new ArrayBuffer[String]()
		try{
			//1.使用自定义工具类函数,获得规范的时间
			val bdp_date_begin:String= DateUtil.dateFormat4String(begin)
			val bdp_date_end:String = DateUtil.dateFormat4String(end)
			// 2.1判断如果结束时间小于开始时间则返回空列表
			if(DateUtil.dateDiff(begin,end)){
				return Nil
			}

			//2.2如果两个时间相等,说明时间跨度为一天
			if(bdp_date_begin.equals(bdp_date_end)){
				//2.2.1相等则把这一天加入列表
				bdp_days+=bdp_date_begin
			}else{
				//2.2.2不相等则需要想办法把这个时间段加入列表
				//使用临时变量记录每一次变化的开始时间
				var tempDay: String = bdp_date_begin
				while(tempDay.equals(bdp_date_end)){
					bdp_days+=tempDay
					//使用时间工具类,使时间每次按照固定的步长(这里是1)移动
					val diffDay: String = DateUtil.dateFormat4StringDiff(tempDay,1)
					//注意:循环关键条件,让临时的日期变为移动后的日期,每一次开始的日期都在变化,直到开始日期和结束日期相等就不进入循环,所以日期队列是左闭右开的
					tempDay=diffDay
				}
			}
		}catch {
			case ex:Exception =>
				println("参数不匹配")
				logger.error(ex.getMessage,ex)

		}
		bdp_days
	}

	/**
	  * 读取表的数据
	  * @param spark SparkSession
	  * @param tableName 表名->ods层log表
	  * @param colNames 被设置条件的列
	  * @return DataFrame 返回一个DataFrame
	  */
	def readTableData(spark: SparkSession, tableName: String, colNames: ArrayBuffer[String]) :DataFrame= {
		//注意:有sparksession的地方最好要隐式导入,防止出现未知的错误,高版本的idea会自动提示
		//通过spark获取表数据,read的结果是一个DataFrame
		val tableDF: DataFrame = spark.read.table(tableName).selectExpr(colNames:_*)
		tableDF
	}

	/**
	* 从传来的df插入到hive中
	* @param sourceDF 源 DataFrame
	* @param tableName 表名
	* @param saveMode 存储模式
	  */
	def writeTableData(sourceDF: Dataset[Row], tableName: String, saveMode: SaveMode) :Unit = {
		//df插入到表中
		sourceDF.write.mode(saveMode).insertInto(tableName)
	}

}
