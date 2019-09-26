package com.yaxin.release.etl.udf

import com.yaxin.release.enums.AgeRangerEnum
import org.slf4j.{Logger, LoggerFactory}

object UDFAgeRange{

	//日志
	private val logger:Logger = LoggerFactory.getLogger( UDFAgeRange.getClass )

	/**
	  * 返回年龄段的区间
	  * @param age 年龄
	  * @return
	  */
	def getAgeRange(age:String):String ={

		var ageRange = ""
		try {
			val ageInt:Int = Integer.valueOf( age )
			if( AgeRangerEnum.AGE_18.getBegin <= ageInt && ageInt <= AgeRangerEnum.AGE_18.getEnd ) {
				ageRange = AgeRangerEnum.AGE_18.getCode
			} else if( AgeRangerEnum.AGE_18_25.getBegin <= ageInt && ageInt <= AgeRangerEnum.AGE_18_25.getEnd ) {
				ageRange = AgeRangerEnum.AGE_18_25.getCode
			} else if( AgeRangerEnum.AGE_26_35.getBegin <= ageInt && ageInt <= AgeRangerEnum.AGE_26_35.getEnd ) {
				ageRange = AgeRangerEnum.AGE_26_35.getCode
			} else if( AgeRangerEnum.AGE_36_45.getBegin <= ageInt && ageInt <= AgeRangerEnum.AGE_36_45.getEnd ) {
				ageRange = AgeRangerEnum.AGE_36_45.getCode
			} else if( AgeRangerEnum.AGE_45.getBegin <= ageInt && ageInt <= AgeRangerEnum.AGE_45.getEnd ) {
				ageRange = AgeRangerEnum.AGE_45.getCode
			}
		} catch {
			case e:Exception => logger.error( "getAgeRange.error:" + e.getMessage )
		}
		ageRange
	}

}
