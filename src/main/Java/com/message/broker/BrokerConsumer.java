package com.message.broker;

import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BrokerConsumer implements Runnable {

    private Socket socket   = null;
    private ServerSocket server   = null;
    private List<Socket> consumerChannels;
    private MessageQueue mq = null;
    private List<ObjectOutputStream> os;

  //  private String BrokerIp= "127.0.0.1";
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
        this.os= new ArrayList<ObjectOutputStream>() ;

    }
    public void run() {
        while(true) {
            try {
                System.out.println("waiting for consumer...");
                Socket  socket = server.accept();
                System.out.println("Accepted consumer connection");
                DataInputStream is = new DataInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                ObjectOutputStream os1 = new ObjectOutputStream(socket.getOutputStream());
                this.os.add(os1);
                Thread messageSender = new Thread(new BrokerConsumerMessageSender(os1, mq));
                messageSender.start();
            }
            catch (Exception ex){
                System.out.println(ex.toString());
            }
        }
    }
}
