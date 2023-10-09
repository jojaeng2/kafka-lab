package kafka.demo.chapter3.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.AllArgsConstructor;

// callback을 사용하기 위해 org.apache.kafka.clients.producer.Callback 구현체가 필요
@AllArgsConstructor
public class ProducerCallback implements Callback {
    private ProducerRecord<String, String> record;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            // 카프카가 exception을 return하면, 처리할 부분
            exception.printStackTrace();
        } else {
            System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Received Message: %s\n", metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
        }
    }
}
