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
        try {
            this.server = new ServerSocket(5002);
        }
        catch(Exception e){
            System.out.println(e.toString());
        }


    }
    public void run() {
        receiveAndProcessMessage();
    }
    public void receiveAndProcessMessage()
    {
        // starts server and waits for a connection
        try
        {
          //  server = brokerServer.getServerSocket();

            System.out.println("Waiting for a producer ...");
            socket = server.accept();
            System.out.println("producer connection accepted at "+ brokerServer.getListenPort());

            ObjectInputStream in =new ObjectInputStream(socket.getInputStream());
            Message msg;
            boolean flag = true;
            while (flag)
            {
                try
                {
                    msg = (Message)in.readObject();
                    if(msg.getHeader().getSize()<0) {
                        flag = false;
                        enQueueMessage(msg);
                        break;
                    }
                //TODO Remove later
                //    System.out.println("----------");
                  //  System.out.println(msg.getHeader().getFileName()+" "+ msg.getHeader().getSize());
                    enQueueMessage(msg);

                    long minRunningMemory = (5*1024*1024);

                    Runtime runtime = Runtime.getRuntime();
                    System.out.println("free= "+runtime.freeMemory() +"  max= "+ runtime.maxMemory());
                    if(runtime.freeMemory()<minRunningMemory) {
                        System.gc();
                    }

                }
                catch(IOException i)
                {
                    System.out.println(i);
                }
                catch (Exception e){
                    System.out.println("non io exception "+ e.toString());
                    System.gc();
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

}