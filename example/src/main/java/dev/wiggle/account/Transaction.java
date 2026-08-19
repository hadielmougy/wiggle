package dev.wiggle.account;

public record Transaction(String from, String to) {

    Transaction withdraw() {
        System.out.println("Withdrawing transaction...");
        return new Transaction(from, to);
    }

    Transaction deposit() {
        System.out.println("Depositing transaction...");
        return new Transaction(from, to);
    }
}
