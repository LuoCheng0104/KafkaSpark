import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.sql.*;
import scala.Tuple2;
import java.util.*;
public class Receiver {
    public static void main(String[] args) throws Exception{
        Map<String,Object> kafkaParams=new HashMap<>();
        kafkaParams.put("bootstrap.servers","localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer",StringDeserializer.class);
        kafkaParams.put("group.id","use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);
        Collection<String> topic= Arrays.asList("topicA","topicB");
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        SparkConf conf=new SparkConf().setAppName("spark&kafka_user_log");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaStreamingContext streamingContext=new JavaStreamingContext(sc, Durations.seconds(10));
        final JavaInputDStream<ConsumerRecord<String,String>> stream= KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topic,kafkaParams)
        );
        JavaRDD<String> ip=sc.textFile(args[0]);
        List<String> ipArray=ip.collect();
        Broadcast<List<String>> bd=sc.broadcast(ipArray);
        JavaPairDStream<String,String> s=stream.mapToPair(x->new Tuple2<>(x.key(),binarySearch(bd.value(),ip2Long(x.key()))+" "+x.value()));
        JavaPairDStream<String,Integer>jpds=s.mapToPair(x->new Tuple2<>(x._1,1)).reduceByKey((x1,y1)->x1+y1).filter(x->x._2>=50);
        jpds.print();
        jpds.foreachRDD(rdd->{
            String url="jdbc:mysql://10.199.160.171:3306/lc";
            Properties connpro=new Properties();
            connpro.put("user","root");
            connpro.put("password","123");
            connpro.put("driver","com.mysql.jdbc.Driver");
            JavaRDD<Row> log=rdd.map(new Function<Tuple2<String, Integer>, Row>() {
                @Override
                public Row call(Tuple2<String, Integer> line) throws Exception {
                    return RowFactory.create(line._1,line._2);
                }
            });
            ArrayList<StructField> filed=new ArrayList<>();
            filed.add(DataTypes.createStructField("ip",DataTypes.StringType,true));
            filed.add(DataTypes.createStructField("num",DataTypes.IntegerType,true));
            StructType type=DataTypes.createStructType(filed);
            Dataset<Row> ipDF=spark.createDataFrame(log,type);
            ipDF.write().mode("append").jdbc(url,"blockIP",connpro);
        });
        s.foreachRDD(rdd->{
            String url="jdbc:mysql://10.199.160.171:3306/lc";
            Properties connpro=new Properties();
            connpro.put("user","root");
            connpro.put("password","123");
            connpro.put("driver","com.mysql.jdbc.Driver");
            JavaRDD<Row> log=rdd.map(new Function<Tuple2<String, String>, Row>() {
                @Override
                public Row call(Tuple2<String, String> line) throws Exception {
                     return RowFactory.create(line._1,line._2.split(" ")[0]+" "+line._2.split(" ")[1]);
                }
            });
            ArrayList<StructField> filed=new ArrayList<>();
            filed.add(DataTypes.createStructField("ip",DataTypes.StringType,true));
            filed.add(DataTypes.createStructField("adds",DataTypes.StringType,true));
            StructType type=DataTypes.createStructType(filed);
            Dataset<Row> ipDF=spark.createDataFrame(log,type);
            //ipDF.write().mode("append").insertInto();
            ipDF.write().mode("append").jdbc(url,"adress",connpro);

        });
        s.print(5);
        jpds.print(5);
        streamingContext.start();
        streamingContext.awaitTermination();
    }
    static  long ip2Long(String s){
        String[] str=s.split("\\.");
        long ip=0;
        for(int i=0;i<str.length;i++){
            ip=ip+Long.parseLong(str[i])*(int)Math.pow(256,3-i);
        }
      return ip;
    }
    static String binarySearch( List<String> s,Long ip){
        int l=0;
        int h=s.size()-1;
        while(l<=h){
            int m=(l+h)/2;
            if(ip>=Long.parseLong(s.get(m).split(",")[1])&&ip<=Long.parseLong(s.get(m).split(",")[2])){
                return s.get(m).split(",")[3]+" "+s.get(m).split(",")[4];
            }else if(ip<Long.parseLong(s.get(m).split(",")[1])){
                h=m-1;
            }else {
                l=m+1;
            }
        }
        return null+" "+null;
    }
}
class Log implements java.io.Serializable{
    private String ip;
    private String adress;
    private String log;
    public void setIp(String ip){
        this.ip=ip;
    }
    public String getIP(){
        return this.ip;
    }
    public void setAdress(String adress){
       this.adress=adress;
    }
    public String getAdress(){
        return this.adress;
    }
    public void setLog(String log){
        this.log=log;
    }
    public String getLog(){
        return this.log;
    }
}
class BlockLog implements java.io.Serializable{
    private String ip;
    private String adress;
    public void setIp(String ip){
        this.ip=ip;
    }
    public String getIP(){
        return this.ip;
    }
    public void setAdress(String adress){
        this.adress=adress;
    }
    public String getAdress(){
        return this.adress;
    }
}


