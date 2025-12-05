package com.jeevankumar.app.event;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {
}
