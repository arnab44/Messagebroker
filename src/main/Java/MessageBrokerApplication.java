import com.message.broker.Broker;
import com.message.broker.BrokerConsumer;
import com.message.broker.MessageQueue;
import com.tcpmanager.Message.Message;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageBrokerApplication {
    public  static  void  main(String[] args) {
      //  Queue<Message> queue =

        MessageQueue mq = new MessageQueue("msg", new ConcurrentLinkedQueue<Message>(), 0L, null);
     //   Broker server = new Broker(mq, null);
        Thread server = new Thread(new Broker(mq, null));
        server.start();

        Thread consumerServer = new Thread(new BrokerConsumer(mq, null));
        consumerServer.start();
       // server.receiveAndProcessMessage();
    }
}
