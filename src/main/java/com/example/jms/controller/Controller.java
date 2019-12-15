package com.example.jms.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.jms.JmsException;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;

@RestController
@RequiredArgsConstructor
public class Controller {

    private static final String DEV_QUEUE_NAME = "DEV.QUEUE.1";
    private static final String DEV_QUEUE_NAME_2 = "DEV.QUEUE.2";

    private final JmsTemplate jmsTemplate;

    @PostConstruct
    public void init() {
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setTimeToLive(10_000);
        jmsTemplate.setReceiveTimeout(10_000);
    }

    @GetMapping("send/{message}")
    String send(@PathVariable("message") String message) {
        try {
            ThreadLocal<Message> messageThreadLocal = new ThreadLocal<>();
            jmsTemplate.convertAndSend(DEV_QUEUE_NAME, message, m -> {
                messageThreadLocal.set(m);
                return m;
            });
            return messageThreadLocal.get().getJMSMessageID();
        } catch (JmsException | JMSException ex) {
            ex.printStackTrace();
            return "FAIL";
        }
    }

    @GetMapping("recv/{id}")
    String recv(@PathVariable("id") String id) throws JMSException {
        try {
            String messageSelector = String.format("JMSCorrelationID='%s'", id);
            Message message = jmsTemplate.receiveSelected(DEV_QUEUE_NAME_2, messageSelector);
            return message == null ? "TIMEOUT" : message.getBody(String.class);
        } catch (JmsException ex) {
            ex.printStackTrace();
            return "FAIL";
        }
    }

    @JmsListener(destination = DEV_QUEUE_NAME)
    public void listener(Message message) throws JMSException {
        System.out.println(message.getJMSMessageID());
        message.setJMSCorrelationID(message.getJMSMessageID());
        jmsTemplate.convertAndSend(DEV_QUEUE_NAME_2, message);
    }
}
