package com.example.notificationservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.retry.annotation.Backoff;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootApplication
public class NotificationserviceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationserviceApplication.class, args);
    }

    @Bean
    JsonMessageConverter converter() {
        return new JsonMessageConverter();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaOperations<String, Object> template) {
        // 1000L = 1s ,retry 2 time
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
    }


    @RetryableTopic(attempts = "5",dltTopicSuffix = "-dlt", backoff = @Backoff(delay = 2_000,multiplier = 2))
    public void retryListenMultipleTime(MessageDTO messageDTO) {
        System.out.println("receive message: ");
        System.out.println(messageDTO);
        //if fail  send to retry topic first after then
        // if retry > 5 they will go
//        throw new Runtime.....
    }

    @KafkaListener(id = "notificationGroup", topics = "notification")
    public void listen(MessageDTO messageDTO) {
        System.out.println("receive message: ");
        System.out.println(messageDTO);
    }
    @KafkaListener(id = "dltGroup", topics = "notification.DLT")
    public void dltListen(MessageDTO messageDTO) {
        System.out.println("receive message DLT: ");
        System.out.println(messageDTO);
    }
}
