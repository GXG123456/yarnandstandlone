import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.collection.Iterator;

/*
* 这个类是将代码提交到yarn上面直接运行的，通过spark读取hive表当中的数据，然后
* 在做相应的计算，然后进行输出，如果你的数据集不是很大的情况下可以采用下面的那种方式把数据集直接
* 输出在driver端也就是你的本地。
* 注意(1)这个项目一定要打包，可以是一个空包，但是一定要把主类打进去。其实就相当于spark-submit的方式将
* 代码提交到集群是一样的。同时要spark的包上传，这里直接放在了HDFS实现文件的共享，不需要再次上传。
* 切记每次提交要自动打包一次，因为有代码要更新
*
* */

public class Sparkonyarn {


    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf().setAppName("spark-onyarn-test").setMaster("yarn-client")
                .set("yarn.resourcemanager.hostname", "vm250-240")
                .set("spark.yarn.preserve.staging.files","false")
                .set("spark.executor.extraClassPath"," /opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hive/lib/*")
                .set("spark.yarn.jar", "hdfs://vm250-240:8020/user/root/spark-assembly-1.6.0-cdh5.14.0-hadoop2.6.0-cdh5.14.0.jar")
                .set("spark.executor.instances","10")
                .set("spark.executor.memory","2048M")
                .set("spark.executor.cores","2")
              .setJars(new String[]{"E:\\datahouse\\yarnandstandlone.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());
        hiveContext.sql("use oracle_table");
        DataFrame sql = hiveContext.sql("select * from loan_base_dsj lbd");
        //这里是进行先分区然后分别将数据进行打印数据，对应做自己的操作
        sql.repartition(100).foreachPartition(new JavaForeachPartitionFunc(){
                                                  @Override
                                                  public void call(Iterator<Row> it) {
                                                      while (it.hasNext()){
                                                          System.out.println(it.next().toString());
                                                      }
                                                  }
                                              }
        );
     //这个只是为了看spark执行的时候的执行情况，不让页面消除

            Thread.sleep(1000000);

            //如果结果集不是很大的情况下可以采用下面这种将数据输出在driver端，然后做操作。
/*
        List<Object> collect =   sql.collectAsList().stream().map(x->x.toString()).collect(Collectors.toList());
        System.out.println(collect);*/
        sc.close();

    }


}
