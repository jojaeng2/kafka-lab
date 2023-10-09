package kafka.demo.chapter3.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProduceFireForgot {

    public void produce() {
        // Properties Object 생성
        Properties props = new Properties();
        // Broker list 정의
        props.put("bootstrap.servers", "host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094");
        // Message Key와 Value가 문자열 타입이므로, Kafka의 기본 StringSerializer를 지정
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        // Properties 객체를 전달해 새 프로듀서 생성
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i=0; i<3; i++) {
                // ProducerRecord 객체 생성
                ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "Apache Kafka Hello world!! @cc " + i);
                // send() 메서드를 사용해 메시지를 전송한다.
                // Future 객체로 RecordMetadata를 return 받을 수 있지만,
                // 무시하고 있기 때문에 메시지가 성공적으로 전송됐는지 알 수 없다.
                producer.send(record);
            }
        } catch (Exception e) {
            // 브로커에게 메시지를 전송한 후의 에러는 무시하지만, 전송 전에 에러가 발생하면 예외 처리 가능
            log.error("[ProduceFireForgot.class.produce()] = " + e.getMessage());
        } finally {
            // 프로듀서 종료
            producer.close();
        }
    }
}
