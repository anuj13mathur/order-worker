package demo.presentation.orderworker.messaging.consumer;

import demo.presentation.orderworker.messaging.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

import static demo.presentation.orderworker.messaging.Constants.*;
import static java.time.LocalDateTime.now;

@Service
public class Retry1Consumer {

    private final Logger logger = LoggerFactory.getLogger(Retry1Consumer.class);

    @Autowired
    private Producer producer;

    @KafkaListener(topics = RETRY_1_TOPIC, groupId = ORDER_WORKER_CONSUMER_GROUP)
    public void consume(String message, Acknowledgment ack) {
        induceDelay();
        logger.info(String.format("Starting to process message from topic %s -> %s", RETRY_1_TOPIC, message));
        try {
            new RestTemplate().postForObject(SHIPPING_SERVICE_URL, null, String.class);
            logger.info(String.format("Processed message from topic %s -> %s", RETRY_1_TOPIC, message));
            ack.acknowledge();
        } catch (Throwable e) {
            logger.error(String.format("Error processing message from topic %s -> %s", RETRY_1_TOPIC, message));
            moveToRetry2Topic(message);
            logger.info(String.format("Moved to %s topic -> %s", RETRY_2_TOPIC, message));
            ack.acknowledge();
        }
    }

    private void moveToRetry2Topic(String message) {
        producer.sendMessage(message, RETRY_2_TOPIC);
    }

    private void induceDelay() {
        LocalDateTime delay = now().plusSeconds(60);
        while (now().isBefore(delay)) ;
    }
}

