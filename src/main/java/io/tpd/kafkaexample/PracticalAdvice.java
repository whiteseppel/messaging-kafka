package io.tpd.kafkaexample;

import com.fasterxml.jackson.annotation.JsonProperty;

record PracticalAdvice(@JsonProperty("message") String message,
                       @JsonProperty("identifier") int identifier) {
}

record MoneyTransaction(@JsonProperty("amount") int amount) {
}

record TotalTransactions(@JsonProperty("totalAmount") int amount,
                         @JsonProperty("validTransactions") int validTransactions) {
}