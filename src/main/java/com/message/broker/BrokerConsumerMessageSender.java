package com.message.broker;

import com.tcpmanager.Message.Message;

import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BrokerConsumerMessageSender implements  Runnable {

    private ObjectOutputStream os;
    private  MessageQueue mq;

    public  BrokerConsumerMessageSender(ObjectOutputStream os, MessageQueue mq) {
        this.os = os;
        this.mq = mq;
    }

    public void run() {

        System.out.println("Started messagesender thread");

        boolean flag = false;
        try {
            while (true) {
                while (mq.getQ().isEmpty());
                //  System.out.println("1111");
                Message message = mq.poll();
                // System.out.println("222");
                if (message != null) {
                    os.writeObject(message);
                    if (!flag) {
                        System.out.println("broker sending 1st msg  at " + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                        flag = true;
                    }
                }
            }

        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }

    }
}
