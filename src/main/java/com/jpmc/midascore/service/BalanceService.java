package com.jpmc.midascore.service;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceService {

    private final DatabaseConduit databaseConduit;
    public BalanceService(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @GetMapping(path = "/balance")
    public Balance getBalance(@RequestParam  String userId) {
        long id = Long.parseLong(userId);
        UserRecord user = databaseConduit.findById(id);
        if (user == null) {
            System.out.println("User not found for id {" + id + "}");
            return new Balance(0);
        }
        return new Balance(user.getBalance());
    }}
