package com.example.accountservice;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AccountserviceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountserviceApplication.class, args);
    }

    @Bean
    NewTopic notification() {
        //topic name, partitions number,  replication number
        return new NewTopic(
                "notification2",
                2,
                (short)3);
    }

    @Bean
    NewTopic statistic() {
        return new NewTopic("statistic", 1, (short) 3);
    }
}
