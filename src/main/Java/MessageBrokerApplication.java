import com.message.broker.Broker;
import com.message.broker.BrokerConsumer;
import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageBrokerApplication {
    private static final int MAX_SIZE = 2*1024*1024*1024;
    public  static  void  main(String[] args) {
      //  ArrayBlockingQueue<Message> abq
        //        = new ArrayBlockingQueue<Message>(215000000);
        MessageQueue mq = new MessageQueue("msg", new ConcurrentLinkedQueue<Message>(), 0L, null);
      //  MessageQueue mq = new MessageQueue("msg", new ArrayBlockingQueue<Message>(215000000), 0L, null);
        //Broker server = new Broker(mq, null);
        Thread producerServer = new Thread(new Broker(mq, null));
        producerServer.start();
        System.out.println("Broker prod started");

        Thread consumerServer1 = new Thread(new BrokerConsumer(mq, null));
        consumerServer1.start();
        System.out.println("Broker consumer1 started");

    }
}
