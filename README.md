# Assignment Messaging

## Verwendung

Starten des Docker Containers
Starten des RestControllers (run KafkaExampleApplication)

Für das Senden von Nachrichten in das Payment-Topic:
Höhe der Zahlung ist in ${amount} festzulegen

	curl an http://localhost:8080/transaction?amount=${amount}

Ergebnisse werden im Logger angezeigt