package com.example.accountservice.controller;

import com.example.accountservice.AccountDTO;
import com.example.accountservice.MessageDTO;
import com.example.accountservice.StatisticDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.function.BiConsumer;

@RestController
@RequestMapping("/account")
public class AccountController {
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/new")
    public AccountDTO create(@RequestBody AccountDTO accountDTO) {
        StatisticDTO statisticDTO = new StatisticDTO();
        statisticDTO.setMessage("Account " + accountDTO.getEmail() + " is created");
        statisticDTO.setCreatedDate(new Date());

        MessageDTO messageDTO = new MessageDTO();
        messageDTO.setTo(accountDTO.getEmail());
        messageDTO.setToName(accountDTO.getName());
        messageDTO.setSubject("subject 1");
        messageDTO.setContent("content 1");
        kafkaTemplate.send("notification", messageDTO).whenComplete( (stringObjectSendResult, throwable) -> {
                    if (throwable != null) {
                        // hanldler failure
                        throwable.printStackTrace();
                    } else {
                        System.out.println(stringObjectSendResult.getRecordMetadata().partition());
                    }
                }
        );
        kafkaTemplate.send("statistic", messageDTO);
        return accountDTO;
    }
}
