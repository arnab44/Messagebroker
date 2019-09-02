package com.message.broker;

import com.tcpmanager.BrokerServer.BrokerServer;
import com.tcpmanager.Message.Message;

import javax.inject.Singleton;
import java.net.*;
import java.io.*;
import java.util.List;

@Singleton
public class Broker implements  Runnable
{
    //initialize socket and input stream
    private Socket          socket   = null;
    private ServerSocket    server   = null;
    private BrokerServer brokerServer;
    private List<String> consumerIP;
    private MessageQueue mq = null;


    public Broker(MessageQueue mq, List<String>consumerIP){
        this.brokerServer = BrokerServer.getBrokerServer();
        this.mq = mq;
        this.consumerIP = consumerIP;

    }
    public void run() {
        receiveAndProcessMessage();
    }
    public void receiveAndProcessMessage()
    {
        // starts server and waits for a connection
        try
        {
            server = brokerServer.getServerSocket();
            System.out.println("Waiting for a client ...");
            socket = server.accept();
            System.out.println("Client accepted");

            ObjectInputStream in =new ObjectInputStream(socket.getInputStream());

            boolean flag = true;
            while (flag)
            {
                try
                {
                    Message msg = (Message)in.readObject();
                    if(msg.getHeader().getSize()<0) {
                        flag = false;
                        enQueueMessage(msg);
                        break;
                    }
                //TODO Remove later
                    System.out.println("----------");
                    System.out.println(msg.getHeader().getFileName()+" "+ msg.getHeader().getSize());
                    enQueueMessage(msg);

                }
                catch(IOException i)
                {
                    System.out.println(i);
                }
                catch (Exception e){
                    System.out.println(e.toString());
                }
            }
            System.out.println("Closing connection");

            // close connection
            socket.close();
            in.close();

        }
        catch(IOException i)
        {
            System.out.println(i);
        }
    }

    public void enQueueMessage(Message msg) {
       boolean isenQueued =  mq.push(msg);
       if(!isenQueued) {
           System.out.println("Message not enQueued successfully "+ msg.getHeader().getFileName());
       }
    }

    public Message pollMessage() {
        while(this.mq.getQ().isEmpty());
        return this.mq.getQ().poll();
    }

    //public void ()
}