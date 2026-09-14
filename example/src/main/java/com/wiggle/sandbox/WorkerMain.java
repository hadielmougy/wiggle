package com.wiggle.sandbox;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;

public class WorkerMain {


    public interface Account {

    }

    public class Steps {
        Account withdraw(Account account, double amount) {
            return account;
        }


    }

    public static void main(String[] args) {
        try(CoordinatedConnection conn = WiggleConnection.coordinator("127.0.0.1:18099")) {

            var client = conn.clientForNamespace("abc");


            FlowSpec spec = FlowSpec.define("test-flow",Account.class, Steps.class, (f, s) -> {


                return null;
            });

            client.register(spec);
        }
    }
}
