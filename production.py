from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    sc = SparkContext(appName='production')
    path = '/staging/shuguang/rec_parse/feature_extraction/battle_total/19/4.2.0/20210601-20211122'
    rdd = sc.newAPIHadoopFile(path=path, inputFormatClass="cn.jj.simulation.utils.newHadoopApi.ListFileInputFormat",
                              keyClass="org.apache.hadoop.io.Text", valueClass="org.apache.hadoop.io.NullWritable")
    print(rdd.collect())