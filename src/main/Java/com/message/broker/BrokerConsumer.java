package com.message.broker;

import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
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
            this.server = new ServerSocket(6000);
        }
        catch (IOException ex) {
            System.out.println(ex.toString());
        }

    }
    public void run() {
        while (true){
            try {
                socket = server.accept();
               // consumerChannels.add(socket);
                DataInputStream is = new DataInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
                System.out.println("Connection with consumer established");
                while (true) {
                    while(mq.getQ().isEmpty());
                    Message message = mq.poll();
                    os.writeObject(message);
                    os.flush();

                }
            }
            catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }

    }
}
