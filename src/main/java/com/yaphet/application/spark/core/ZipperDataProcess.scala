package com.yaphet.application.spark.core

import java.io.PrintWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ZipperDataProcess {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("ReadCSVFiles").setMaster("local[*]")
        val sc = new SparkContext(conf)
        // 定义数据目录路径
        val dataDir = "src/main/data/NS_S1MME_4G/QH"
        val outputDir= "src/main/output"

        // 定义一个函数来读取数据文件并返回 RDD[String]
        def readFile(hour: String): RDD[String] = {
            val filePath = s"$dataDir/$hour/$hour" + "_data.csv"
            sc.textFile(filePath).zipWithIndex().filter(_._2 > 0).map(_._1)
        }

        var hour: String = ""
        for (i <- 0 to 23) {
            hour = f"20230528%%02d".format(i)
            val Rdds = readFile(hour)
            var parsedData: RDD[Array[String]] = Rdds.map(line => line.split(","))
            // 先添加需要的endTime属性，初始值为startTime
            parsedData = parsedData.map(arr => arr :+ arr(3))

            // 依照imsi为每小时的数据划分
            // imsiRDDs是一个  RDD[(String, Array[Array[String]])]
            // 其中元组的第一个元素是 imsi 值，第二个元素是包含该 imsi 值数据的数组。
            val imsiRDDs = parsedData.groupBy(arr => arr(0)).mapValues(_.toArray)
            // 先按imsi分组，分到每一个key对应的array中，再按照时间排序数据
            val sortedRDD = imsiRDDs.mapValues(arr => arr.sortBy(_(3)))

            // 遍历数组进行拉链，imsi为key，arrs为Array[Array[String]]，存放每一个imsi的一条数据，也对应一个csv文件
            sortedRDD.foreach { case (imsi, arrs) =>

                // 归并处理

                var index = 0
                var i = 0
                var mergeList = List[Array[String]]()
                var lacCell = arrs(index)(4) + arrs(index)(5)
                // arr是一条数据
                arrs.foreach(arr => {
                    val tempLacCell = arr(4) + arr(5)
                    // 迭代量+1
                    i = i + 1
                    // 遇到新的基站
                    if (lacCell != tempLacCell) {
                        // 保存当前数据，并更新起始点为当前读到的数据，迭代量不增加
                        mergeList = mergeList :+ arrs(index)
                        lacCell = tempLacCell
                        index = i - 1
                        // 如果新基站恰好是最后一条数据
                        if (i == arrs.length) {
                            // 直接将此条数据保存
                            mergeList = mergeList :+ arrs(arrs.length - 1)
                        }
                    }
                    // 遇到相同的基站
                    else {
                        // 修改endTime
                        arrs(index)(9) = arr(3)
                        if (i == arrs.length) {
                            // 直接将此条数据保存
                            mergeList = mergeList :+ arrs(arrs.length - 1)
                        }
                    }
                })
                val mergeArray = mergeList.toArray
                // 乒乓处理
                // ① i-1和i+1的基站laccell是否相同，相同则转入②，反之将i放入栈中
                // ② i-1和i时间差是否大于5分钟，否则转入③，反之将i放入栈中
                // ③ i+1和i时间差是否大于5分钟，否则i为乒乓切换基站，不做处理转入④，反之将i放入栈中
                // ④ 下一轮遍历，直到处理所有数据
                // 由乒乓处理数据的流程可知，数据的第一项和最后一项不在处理范围内
                // 也即下标范围为1 to arrs.length - 2
                var pingPongList = List[Array[String]]()
                pingPongList = pingPongList :+ mergeArray(0)
                // 只有多于两条数据才需要进行乒乓处理
                if (mergeArray.length > 2) {
                    var i = 0
                    mergeArray.foreach(arr => {
                        // 排除首尾
                        if (i != 0 && i != mergeArray.length - 1) {
                            val preTime = mergeArray(i - 1)(9).toLong
                            val periStartTime = mergeArray(i)(3).toLong
                            val periEndTime = mergeArray(i)(9).toLong
                            val postTime = mergeArray(i + 1)(3).toLong
                            if ((mergeArray(i - 1)(4) + mergeArray(i - 1)(5)) != (mergeArray(i + 1)(4) + mergeArray(i + 1)(5))) {
                                pingPongList = pingPongList :+ mergeArray(i)
                            }
                            else if ((periStartTime - preTime) / 1000 > 300) {
                                pingPongList = pingPongList :+ mergeArray(i)
                            }
                            else if ((postTime - periEndTime) / 1000 > 300) {
                                pingPongList = pingPongList :+ mergeArray(i)
                            }
                        }
                        i = i + 1
                    })
                }
                if (mergeArray.length >= 2) {
                    pingPongList = pingPongList :+ mergeArray(mergeArray.length - 1)
                }

                //输出拉链数据
                val filePath = s"$outputDir/$hour/"
                val fileName = filePath + imsi + ".csv"
                val csvData = pingPongList.map(_.mkString(",")).mkString("\n")
                val pw = new PrintWriter(fileName)
                pw.write(csvData)
                pw.close()
            }
        }
    }
}