package com.yaphet.application.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

object CsvReader {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("CsvReader").setMaster("local[*]")
        val sc = new SparkContext(conf)

        def readHourData(hour: Int): RDD[String] = {
            val path = f"src/main/data/NS_S1MME_4G/QH/20230528$hour%02d/"
            val csvData: RDD[String] = sc.textFile(path + f"20230528$hour%02d_data.csv")
            csvData
        }

        val hourDataList: IndexedSeq[RDD[String]] = (0 to 23).map(readHourData)
        val mergedData: RDD[String] = hourDataList.reduce((data1, data2) => data1.union(data2))
        val parsedData: RDD[Array[String]] = mergedData.map(line => line.split(","))
        val keyValueData: RDD[(String, Array[String])] = parsedData.map(line => (line(0), line))

        val groupedData: RDD[(String, Iterable[Array[String]])] = keyValueData.groupByKey()

        /**
         *  这里使用了while循环来实现相邻两组数据的比较和属性的添加
         *  在while循环中，我们先将当前数据的laccell存储到变量中，然后从当前位置开始遍历后面的数据
         *  如果后面的数据的laccell与当前数据相同，就将后面数据的第三个属性添加到当前数据的新增属性中，并将后面的数据删除
         *  如果后面的数据的laccell不同，就将新增属性设置为空字符串，并将当前数据写入到文件中
         *  然后将index设置为后面数据的位置，继续遍历后面的数据
         */

        groupedData.foreach { case (key, values) =>
            val file = new File(s"src/main/output/$key.csv")

            val writer = new PrintWriter(file)
            // 将values转换为List，并按照第三列进行排序
            val sortedValues: List[Array[String]] = values.toList.sortBy(_(3))
            // 遍历sortedValues中的每个元素，并将其转换为CSV格式的字符串，写入到文件中
            var index = 0
            while (index < sortedValues.length) {
                var laccell = sortedValues(index)(4) + sortedValues(index)(5)
                var newAttr = ""
                var i = index
                while (i < sortedValues.length && sortedValues(i)(4) + sortedValues(i)(5) == laccell) {
                    if (i > index) {
                        sortedValues(index) :+= sortedValues(i)(2)
                    }
                    i += 1
                }
                sortedValues(index) :+= newAttr
                val csvLine: String = sortedValues(index).mkString(",") + "\n"
                writer.write(csvLine)
                index = i
            }
            writer.close()
        }
    }
}