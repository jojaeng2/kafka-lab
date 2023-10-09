package kafka.demo.chapter3.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConsumerAsync {

    public void consume() {

        // Properties Object 생성
        Properties props = new Properties();
        // Broker list 정의
        props.put("bootstrap.servers", "host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094");
        // Consumer Group ID 정의
        props.put("group.id", "comsumer01");
        // Auto Commit 사용 X
        props.put("enable.auto.commit", "false");
        // Consumer Offset을 찾지 못하는 경우, latest로 초기화하며 가장 최근부터 메시지를 가져옴
        props.put("auto.offset.reset", "latest");
        // Message Key와 Value가 문자열 타입이므로, Kafka의 기본 StringSerializer를 지정
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        // Properties 객체를 전달해 새 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 구독할 토픽 지정
        consumer.subscribe(Arrays.asList("topic01"));

        try {
            // 무한 루프 시작, 메시지를 가져오기 위해 카프카에 지속적으로 poll()을 수행함.
            while (true) {
                // Consumer는 폴링하는 것을 계속 유지하며, 타임아웃 주기를 설정.
                ConsumerRecords<String, String> records = consumer.poll(1000);
                // poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로 반복문 처리
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                // 현재 배치를 통해 읽은 모든 메시지를 처리한 후, 추가 메시지를 폴링하기 전 현재의 offset을 async commit
                consumer.commitAsync();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            // consumer 종료
            consumer.close();
        }
    }
}
