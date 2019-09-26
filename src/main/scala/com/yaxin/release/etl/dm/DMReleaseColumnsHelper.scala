package com.yaxin.release.etl.dm

import scala.collection.mutable.ArrayBuffer

/**
  * ODS到DM每个表需要的字段
  */
object DMReleaseColumnsHelper{

	/**
	  * 1.目标用户的字段
	  * 这里出现的字段要求在调用者(df)的中有的字段
	  * @return
	  */
	def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
		//这里操作的表是基于ods层的表进行的
		val columns = new ArrayBuffer[String]()
		columns.+=("release_session")
		columns.+=("release_status")
		columns.+=("device_num")
		columns.+=("device_type")
		columns.+=("sources")
		columns.+=("channels")
		columns.+=("idcard")
		columns.+=("age")
		//通过udf操作列
		columns.+=("getAgeRange(age) as age_range")
		columns.+=("gender")
		columns.+=("area_code")
		columns.+=("ct")
		columns.+=("bdp_day")

		columns
	}

	/**
	  * 2.渠道指标字段:是在1表的基础上实现的,根据渠道和通道分组,,这里的字段指的是最后留下的字段
	  * user_count针对device_num进行唯一统计,需要去重,统计用户的个数
	  * total_count不用去重,根据session数统计即可
	  * 因为一个用户有多个session
	  */
	def selectDMCustomerSourceColumns():ArrayBuffer[String] ={
		val columns = new ArrayBuffer[String]()
		columns.+=("sources")
		columns.+=("channels")
		columns.+=("device_type")
		columns.+=("user_count")
		columns.+=("total_count")
		columns.+=("bdp_day")
		columns
	}

	/**
	  * 目标客户多维度分析统计cube,对多个维度进行分组统计
	  * @return
	  */
	def selectDMCustomerCudeColumns():ArrayBuffer[String]={
		val columns = new ArrayBuffer[String]()
		columns.+=("sources")
		columns.+=("channels")
		columns.+=("device_type")
		columns.+=("age_range")
		columns.+=("gender")
		columns.+=("area_code")
		columns.+=("user_count")
		columns.+=("total_count")
		columns.+=("bdp_day")
		columns
	}

}
