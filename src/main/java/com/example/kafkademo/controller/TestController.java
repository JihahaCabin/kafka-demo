package com.example.kafkademo.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RestController
public class TestController {

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @Autowired
    private KafkaProperties kafkaProperties;

    // 发送消息，异步
    @GetMapping("/send/{input}")
    public void sendFoo(@PathVariable String input)  {
        this.template.send("topic_input", input).addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("I am failure");
            }

            @Override
            public void onSuccess(SendResult<Object, Object> objectObjectSendResult) {
                System.out.println(System.currentTimeMillis());
                System.out.println("I am success");
            }
        });
        System.out.println(System.currentTimeMillis());
    }

    // 发送消息，同步获取发送结果
    @GetMapping("/sendSync/{input}")
    public void sendFooSync(@PathVariable String input)  {
        ListenableFuture<SendResult<Object, Object>> future = this.template.send("topic_input", input);
        try{
            SendResult<Object, Object> sendResult = future.get();
            System.out.println(sendResult);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // 监听消息
    @KafkaListener(id = "webGroup", topics = "topic_input",concurrency = "3",errorHandler = "myErrorHandler")
    public void listen(String input, Acknowledgment acknowledgment) {
        System.out.println("input value: "+ input);
        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //手动提交偏移量
        acknowledgment.acknowledge();

    }

    //创建topic
    @GetMapping("/topic/create")
    public void createTopic(@RequestParam("name") String name,@RequestParam("partition") Integer partition,@RequestParam("replica") Short replica){
        AdminClient client = AdminClient.create(kafkaProperties.buildAdminProperties());
        if (client!=null){
            try{
                List<NewTopic> newTopics = new ArrayList<>(1);
                newTopics.add(new NewTopic(name,partition,replica));
                client.createTopics(newTopics);
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                client.close();
            }
        }
    }



}
