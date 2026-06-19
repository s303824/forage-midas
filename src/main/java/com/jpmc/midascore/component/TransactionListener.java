package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    @Autowired
    private DatabaseConduit databaseConduit;

    private final RestTemplate rest;

    public TransactionListener(RestTemplateBuilder builder) {
        this.rest = builder.build();
    }


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

            System.out.println("About to save transaction record " + transactionRecord);
            databaseConduit.save(transactionRecord);
            System.out.println("Saved transaction record");

            // and update records in both parties' accounts
            float prevSenderAmount = sender.getBalance();
            float prevRecipientAmount = recipient.getBalance();

            // TODO: set incentive to 0 if testing
            float incentive = getIncentive(transaction).getAmount();
            //float incentive = 0;

            System.out.println("SENDER{"+sender.getName()+"}: " + prevSenderAmount + " -> "+ (prevSenderAmount-amount));
            System.out.println("RECIPIENT{"+recipient.getName()+"}: " + prevRecipientAmount + " -> "+ (prevRecipientAmount + amount + incentive));
            System.out.println("INCENTIVE: "+ incentive);

            sender.setBalance(prevSenderAmount - amount);
            recipient.setBalance(prevRecipientAmount + amount + incentive);

            System.out.println("About to save user records");
            databaseConduit.save(sender);
            databaseConduit.save(recipient);
            System.out.println("Saved user records");
        }
        else {
            System.out.println("Transaction received (INVALID): " + transaction);
        }
    }

    // returns POST response as an Incentive object class instance
    public Incentive getIncentive(Transaction transaction){
        String incentiveUrl = "http://localhost:8080/incentive";
        try {
            return rest.postForObject(incentiveUrl, transaction, Incentive.class);
        }catch (Exception e) {
            System.out.println("IncentiveError: POST request failed, set amount to zero.");
            return new Incentive();
        }
    }
}
