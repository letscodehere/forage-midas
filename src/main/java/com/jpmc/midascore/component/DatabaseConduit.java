package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Service
public class TransactionListener {
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private TransactionRecordRepository transactionRecordRepository;
    
    @Autowired
    private RestTemplate restTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    
    @KafkaListener(topics = "${midascore.kafka.topic}", groupId = "midas-core-group")
    public void handleTransaction(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            
            // Validate transaction
            Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
            Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());
            
            if (senderOpt.isPresent() && recipientOpt.isPresent()) {
                UserRecord sender = senderOpt.get();
                UserRecord recipient = recipientOpt.get();
                
                // Check if sender has sufficient balance
                if (sender.getBalance() >= transaction.getAmount()) {
                    
                    // Call Incentive API
                    Incentive incentive = restTemplate.postForObject(
                        INCENTIVE_API_URL,
                        transaction,
                        Incentive.class
                    );
                    
                    float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;
                    
                    // Update balances
                    sender.setBalance(sender.getBalance() - transaction.getAmount());
                    recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
                    
                    // Save updated balances
                    userRepository.save(sender);
                    userRepository.save(recipient);
                    
                    // Create and save transaction record with incentive
                    TransactionRecord record = new TransactionRecord(
                        transaction.getAmount(),
                        incentiveAmount,
                        sender,
                        recipient
                    );
                    transactionRecordRepository.save(record);
                }
                // If balance is insufficient, discard transaction
            }
            // If sender or recipient is invalid, discard transaction
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
