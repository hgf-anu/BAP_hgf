package com.yaxin.release.constant

import org.apache.spark.storage.StorageLevel

object ReleaseConstant{
	// partition,分区字段,分区个数,存储级别
	val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
	val DEF_PARTITION:String = "bdp_day"
	val DEF_SOURCE_PARTITION = 4


	// 维度列:字段名
	val COL_RLEASE_SESSION_STATUS:String = "release_status"
	val COL_RELEASE_SOURCES = "sources"
	val COL_RELEASE_CHANNELS = "channels"
	val COL_RELEASE_DEVICE_TYPE = "device_type"
	val COL_RELEASE_DEVICE_NUM = "device_num"
	val COL_RELEASE_USER_COUNT = "user_count"
	val COL_RELEASE_TOTAL_COUNT = "total_count"
	val COL_RELEASE_AGE_RANGE = "age_range"
	val COL_RELEASE_GENDER = "gender"
	val COL_RELEASE_AREA_CODE = "area_code"

	// ods表名================================
	val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

	// dw表名=================================
	val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

	// dm表名=================================
	val DM_RELEASE_CUSTOMER_SOURCE = "dm_release.dm_customer_sources"
	val DM_RELEASE_CUSTOMER_CUBE = "dm_release.dm_customer_cube"
}
