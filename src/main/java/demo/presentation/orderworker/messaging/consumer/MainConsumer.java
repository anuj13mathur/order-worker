package demo.presentation.orderworker.messaging.consumer;

import demo.presentation.orderworker.messaging.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static demo.presentation.orderworker.messaging.Constants.*;

@Service
public class MainConsumer {
    private final Logger logger = LoggerFactory.getLogger(Retry1Consumer.class);

    @Autowired
    private Producer producer;

    @KafkaListener(topics = MAIN_TOPIC, groupId = "order-worker")
    public void consume(String message, Acknowledgment ack) {
        logger.info(String.format("Starting to process message from topic %s -> %s", MAIN_TOPIC, message));
        try {
            new RestTemplate().postForObject(SHIPPING_SERVICE_URL, null, String.class);
            logger.info(String.format("Processed message from topic %s -> %s", MAIN_TOPIC, message));
            ack.acknowledge();
        } catch (Throwable e) {
            logger.error(String.format("Error processing message from topic %s -> %s", MAIN_TOPIC, message));
            moveToRetry1Topic(message);
            logger.info(String.format("Moved to %s topic -> %s", RETRY_1_TOPIC, message));
            ack.acknowledge();
        }
    }

    private void moveToRetry1Topic(String message) {
        producer.sendMessage(message, RETRY_1_TOPIC);
    }
}

