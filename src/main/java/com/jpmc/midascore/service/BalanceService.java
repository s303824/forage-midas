package com.jpmc.midascore.service;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceService {

    @Autowired
    DatabaseConduit databaseConduit;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam String stringId) {
        long id = Long.parseLong(stringId);
        try{
            UserRecord user = databaseConduit.findById(id);
            return new Balance(user.getBalance());
        }catch (Exception e){
            System.out.println("BalanceService could not find user, return balance set to 0");
            return new Balance(0);
        }
    }
}
