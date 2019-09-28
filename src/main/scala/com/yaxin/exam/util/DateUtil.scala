package com.yaxin.exam.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.commons.lang3.StringUtils

/**
  * 时间工具类
  */
object DateUtil{

	/**
	  * String类型的时间规范化
	  *
	  * @param date 需要规范格式的时间
	  * @param formater 规范模型,有默认值是"yyyyMMdd"
	  * @return 转化后的时间
	  */
	def dateFormat4String(date:String, formater:String="yyyyMMdd") :String= {
		if(date==null){
			return null
		}
		//获得时间范式
		val formatt: DateTimeFormatter = DateTimeFormatter.ofPattern(formater)
		//根据参数转换为符合时间范式的时间
		val dt: LocalDate = LocalDate.parse(date,formatt)
		//转换为String类型
		dt.format(formatt)
	}


	/**
	  *
	  * @param beign 开始时间
	  * @param end 结束时间
	  * @return 返回开始时间是否大于结束时间
	  */
	def dateDiff(beign:String,end:String):Boolean={
		// TODO

		true
	}

	/**
	  * 让日期向后偏移某步长
	  * @param sourceDay 源数据日期
	  * @param diff 步长
	  * @param formater 规范
	  */
	def dateFormat4StringDiff(sourceDay:String, diff:Int , formater:String = "yyyyMMdd"):String = {
		//如果参数无效则返回null
		if(StringUtils.isBlank(sourceDay)){
			return null
		}
		val formator: DateTimeFormatter = DateTimeFormatter.ofPattern(formater)
		//根据参数转换为符合时间范式的时间
		val dt: LocalDate = LocalDate.parse(sourceDay,formator)
		//让源日期加diff步长的日期
		val resultDt: LocalDate = dt.plusDays(diff)

		resultDt.format(formator)
	}


}
