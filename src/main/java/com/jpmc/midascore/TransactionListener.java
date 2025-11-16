package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;

@Service
public class TransactionListener {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
        topics = "${midascore.kafka.topic}", 
        groupId = "midas-core-group"
    )
    public void handleTransaction(String message) {
        try {
            // Replace Transaction.class with the actual transaction POJO provided in the codebase
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            // For Task 2, you only need to confirm receiving/deserializing transactions, not further processing.
            System.out.println(transaction);
        } catch (Exception e) {
            // Log or handle deserialization errors
            e.printStackTrace();
        }
    }
}
