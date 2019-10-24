package com.yaxin.release.test
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test{
	def main(args:Array[String]):Unit ={
		val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("lala")
		val sc = new SparkContext(conf)
		sc.setLogLevel("error")
		val arr1 = ('A' to 'Z').toArray
		val arr2 = (1 to 26).toArray
		val rdd1: RDD[Char] = sc.makeRDD(arr1)
		val rdd2 = sc.makeRDD(arr2)
		println(rdd1.getNumPartitions,rdd2.partitions.length)
		val rdd3: RDD[(Int, Char)] = rdd1.map(char=>(char.toInt-64,char))
		val rdd4 = rdd2.map(n=>(n,n))
		val rdd5: RDD[(Int, (Char, Int))] = rdd3.join(rdd4)
		println(rdd1.getNumPartitions,rdd2.partitions.length,rdd3.getNumPartitions,rdd4.getNumPartitions,rdd5.getNumPartitions)
		println(rdd1.partitioner,rdd2.partitioner,rdd3.partitioner,rdd4.partitioner,rdd5.partitioner)
		rdd5.foreach(println)
		sc.stop()
	}
}
