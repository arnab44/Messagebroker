package com.message.broker;

import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class BrokerConsumer implements Runnable {

    private Socket socket   = null;
    private ServerSocket server   = null;
    private List<Socket> consumerChannels;
    private MessageQueue mq = null;
    private String BrokerIp= "127.0.0.1";
    public BrokerConsumer(MessageQueue mq, List<Socket>consumeChannels) {
     //   this.brokerServer = BrokerServer.getBrokerServer();
        this.mq = mq;
        this.consumerChannels = consumeChannels;
        try {
            this.server = new ServerSocket(4000);
        }
        catch (IOException ex) {
            System.out.println(ex.toString());
        }

    }
    public void run() {
        try {
            System.out.println("waiting for consumer...");
            socket = server.accept();
            System.out.println("Accepted consumer connection");
            // consumerChannels.add(socket);
            DataInputStream is = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            boolean flag = false;
            while (true) {
                //while (mq.getQ().isEmpty());
              //  System.out.println("1111");
                Message message = mq.poll();
               // System.out.println("222");
                if(message!=null) {
                    os.writeObject(message);
                    if(!flag) {
                        System.out.println("broker sending 1st msg  at " + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
                        flag =true;
                    }
                }
            }
        }   catch (Exception ex) {
            System.out.println("ex "+ex.toString());
            ex.printStackTrace();
        }

    }
}
