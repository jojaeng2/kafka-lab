package kafka.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import kafka.demo.chapter3.producer.ProducerSync;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
		ProducerSync producerSync = new ProducerSync();
		producerSync.produce();
	}

}
