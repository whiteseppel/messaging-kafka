package io.tpd.kafkaexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@RestController
public class HelloKafkaController {

    private static final Logger logger =
            LoggerFactory.getLogger(HelloKafkaController.class);

    private final KafkaTemplate<String, Object> template;
    private final String topicName;
    private final int messagesPerRequest;
    private CountDownLatch latch;
    private int allMessages;
    private int validMessages;
    private ObjectMapper mapper;

    public HelloKafkaController(
            final KafkaTemplate<String, Object> template,
            @Value("${tpd.topic-name}") final String topicName,
            @Value("${tpd.messages-per-request}") final int messagesPerRequest) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
        this.allMessages = 0;
        this.validMessages = 0;
        this.mapper = new ObjectMapper();
    }

    @GetMapping("/transaction")
    public String transaction(@RequestParam String amount) throws Exception {
        latch = new CountDownLatch(messagesPerRequest);
        this.template.send("money-transaction", String.valueOf(1), new MoneyTransaction(Integer.parseInt(amount)));
        logger.info("New transaction with value {} incoming", amount);
        return "Your transaction will be processed";
    }

    // consumer
    @KafkaListener(topics = "money-transaction", clientIdPrefix = "string",
            containerFactory = "kafkaListenerStringContainerFactory")
    public void listenForTransaction(ConsumerRecord<String, String> cr,
                                     @Payload String payload) throws JsonProcessingException {
        logger.info("Transaction is checked for money laundering");

        Map<String, Integer> map = mapper.readValue(payload, Map.class);
        var transactionAmount = (map.get("amount"));

        this.allMessages++;
        if(transactionAmount<1000){
            this.validMessages++;
        }
        this.template.send("money-transaction-valid", String.valueOf(1), new TotalTransactions(this.allMessages, this.validMessages));
    }

    @KafkaListener(topics = "money-transaction-valid",clientIdPrefix = "string",
            containerFactory = "kafkaListenerStringContainerFactory")
    public void listenForValidTransaction(ConsumerRecord<String, String> cr,
                                          @Payload String payload) throws JsonProcessingException {
        logger.info("New info for total transactions received");

        Map<String, Integer> map = mapper.readValue(payload, Map.class);

        int totalTransactions = (map.get("totalAmount"));
        int validTransactions = (map.get("validTransactions"));

        logger.info("Total number of transactions: {}, total number of valid transactions: {}", totalTransactions, validTransactions);
    }
}
