import com.message.broker.Broker;
import com.message.broker.BrokerConsumer;
import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageBrokerApplication {
    public  static  void  main(String[] args) {
      //  Queue<Message> queue =

        MessageQueue mq = new MessageQueue("msg", new ConcurrentLinkedQueue<Message>(), 0L, null);
        //Broker server = new Broker(mq, null);
        Thread producerServer = new Thread(new Broker(mq, null));
        producerServer.start();
        System.out.println("Broker prod started");

        Thread consumerServer = new Thread(new BrokerConsumer(mq, null));
        consumerServer.start();
        System.out.println("Broker consumer started");


    }
}
