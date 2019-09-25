package com.yaxin.release.util

import org.apache.spark.storage.StorageLevel

/**
  * 常量类
  */
object SparkHelper{

	// partition
	val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
	val DEF_PARTITION:String = "bdp_day"
	val DEF_SOURCE_PARTITION = 4


	// 维度列
	val COL_RLEASE_SESSION_STATUS:String = "release_status"

	// ods================================
	val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

	// dw=================================
	val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

}
