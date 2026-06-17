package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    // Listen for message from Kafka
    @KafkaListener(topics="${general.kafka-topic}", groupId = "transaction-group")
    public void listen(Transaction transaction){
        // when message received, check if parties are in DB and amount is payable

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        float amount = transaction.getAmount();

        if(sender != null & recipient != null && sender.getBalance()>=amount){
            // if so, save record of transaction in DB
            System.out.println("Valid transaction recognized.");
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);
            transactionRepository.save(transactionRecord);

            // and update records in both parties' accounts
            float prevSenderAmount = sender.getBalance();
            float prevRecipientAmount = recipient.getBalance();

            sender.setBalance(prevSenderAmount - amount);
            recipient.setBalance(prevRecipientAmount + amount);

            userRepository.save(sender);
            userRepository.save(recipient);
        }
        else {
            System.out.println("Invalid transaction discarded.");
        }
    }
}
