package demo.presentation.orderworker.messaging;

public class Constants {
    public static final String SHIPPING_SERVICE_URL = "http://localhost:8086/shipping/orders";
    public static final String MAIN_TOPIC = "orders";
    public static final String RETRY_1_TOPIC = "orders.retry.1";
    public static final String RETRY_2_TOPIC = "orders.retry.2";
    public static final String DLQ_TOPIC = "orders.dlq";

    public static final String ORDER_WORKER_CONSUMER_GROUP = "order-worker";
}
