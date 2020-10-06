package com.example.kafkademo.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service("myErrorHandler")
public class MyKafkaListenerErrorHandler implements KafkaListenerErrorHandler {


    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        System.out.println("failed");
        System.out.println(message.getPayload().toString());
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException e) {
        System.out.println("failed");
        System.out.println(message.getPayload().toString());
        return null;
    }
}
