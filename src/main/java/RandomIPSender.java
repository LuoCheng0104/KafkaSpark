import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class RandomIPSender {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();//broker地址
        props.put("bootstrap.servers", "localhost:9092");//请求时候需要验证
        props.put("acks", "all");//请求失败时候需要重试
        props.put("retries", 0);//内存缓存区大小
        props.put("buffer.memory", 33554432);//指定消息key序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//指定消息本身的序列化方式
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String filename=args[0];
        FileReader fr=new FileReader(new File(filename));
        BufferedReader bf=new BufferedReader(fr);
        String line=bf.readLine();
        while(true) {
            String str=randomip();
            producer.send(new ProducerRecord<>("topicA",str, str));
            System.out.println(str);
            //line=bf.readLine();
        }
       // System.out.println("Message sent successfully");
       // producer.close();
    }
    static String randomip(){
        Random rand=new Random();
        String ip=String.valueOf(rand.nextInt(256))+"."
                +String.valueOf(rand.nextInt(256))+"."
                +String.valueOf(rand.nextInt(256))+"."
                +String.valueOf(rand.nextInt(256));
        return ip;
    }
}
