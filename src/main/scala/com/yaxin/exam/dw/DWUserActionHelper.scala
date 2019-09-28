package com.yaxin.exam.dw

import scala.collection.mutable.ArrayBuffer

object DWUserActionHelper{
	def selectDWUserActionColumns():ArrayBuffer[String]={
		val columns = new ArrayBuffer[String]()
		columns += ("user")
		columns += ("""CASE WHEN ACTION='01' THEN 1
		                   WHEN ACTION='02' THEN 2
		                   WHEN ACTION='03' THEN 2
		                   WHEN ACTION='04' THEN 3
		                   WHEN ACTION='05' THEN 5
		              END as uscore""" )

		columns
	}
}
