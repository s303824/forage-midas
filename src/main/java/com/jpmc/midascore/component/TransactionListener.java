package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    @Autowired
    private DatabaseConduit databaseConduit;

    private final RestTemplate rest = new RestTemplate();
    private final String incentiveUrl = "http://localhost:8080/incentive";

    // Listen for message from Kafka
    @KafkaListener(topics="${general.kafka-topic}", groupId = "transaction")
    public void listen(Transaction transaction){
        // when message received, check if parties are in DB and amount is payable

        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        float amount = transaction.getAmount();

        if(sender != null && recipient != null && sender.getBalance()>=amount){
            // if so, save record of transaction in DB
            System.out.println("Transaction received (VALID): " + transaction);
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);
            databaseConduit.save(transactionRecord);

            // and update records in both parties' accounts
            float prevSenderAmount = sender.getBalance();
            float prevRecipientAmount = recipient.getBalance();

            float incentive = getIncentive(transaction).getAmount();

            sender.setBalance(prevSenderAmount - amount);
            recipient.setBalance(prevRecipientAmount + amount + incentive);

            databaseConduit.save(sender);
            databaseConduit.save(recipient);
        }
        else {
            System.out.println("Transaction received (INVALID): " + transaction);
        }
    }

    public Incentive getIncentive(Transaction transaction){
        return rest.postForObject(incentiveUrl, transaction, Incentive.class);
    }
    public static class Incentive {
        private float amount;
        public float getAmount(){
            return amount;
        }
    }
}
