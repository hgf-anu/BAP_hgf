package com.yaxin.exam.dm

import scala.collection.mutable.ArrayBuffer

object DMUserActionHelper{
	def selectDMUserActionColumns():ArrayBuffer[String]={
		val columns = new ArrayBuffer[String]()
		columns +=("user")
		columns +=("uscore")

		columns
	}

}
