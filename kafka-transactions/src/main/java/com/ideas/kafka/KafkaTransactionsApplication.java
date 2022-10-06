package com.ideas.kafka;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.ideas.kafka.model.Transaction;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@EnableScheduling
@SpringBootApplication
public class KafkaTransactionsApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransactionsApplication.class);

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private ElasticsearchClient elasticsearchClient;

	public static void main(String[] args) {
		SpringApplication.run(KafkaTransactionsApplication.class, args);
	}

	/**
	 * Method that allows to listen to the messages to consume of a Kafka topic
	 * and index on Elasticsearch.
	 *
	 * @param messages A {@link List} of type {@link ConsumerRecord}
	 *
	 * @throws IOException If an error occurs to index the message on Elasticsearch
	 */
	@KafkaListener(topics = "ideas-topic-1", groupId = "ideas-group",
			containerFactory = "kafkaListenerContainerFactory",
			properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
	public void listenBatch(List<ConsumerRecord<String, String>> messages)
			throws IOException {
		LOGGER.info("Start reading messages");
		for (ConsumerRecord<String, String> message : messages) {
			Transaction transaction = objectMapper.readValue(message.value(), Transaction.class);
			IndexRequest<Transaction> indexRequest = buildIndexRequest(String.format("%s-%s-%s",
					message.partition(), message.key(), message.offset()), transaction);
			elasticsearchClient.index(indexRequest);
		}
		LOGGER.info("Batch complete");
	}

	/**
	 * Method that allows to send a message of a Kafka topic.
	 *
	 * @throws JsonProcessingException
	 */
	@Scheduled(fixedRate = 10000)
	public void sendMessages() throws JsonProcessingException {
		Faker faker = new Faker();
		for (int i = 0; i < 1; i++) {
			Transaction transaction = new Transaction();
			transaction.setName(faker.name().firstName());
			transaction.setLastName(faker.name().lastName());
			transaction.setUserName(faker.name().username());
			transaction.setAmount(faker.number().randomDouble(4, 0, 50000));
			// send message
			kafkaTemplate.send("ideas-topic-1", transaction.getUserName(),
					objectMapper.writeValueAsString(transaction));
		}
	}

	/**
	 * Method that allows to create a IndexRequest object with key and value.
	 *
	 * @param key {@link String} object with the key
	 * @param value {@link Transaction} object with the value
	 *
	 * @return A {@link IndexRequest} of type {@link Transaction}
	 */
	private IndexRequest<Transaction> buildIndexRequest(String key, Transaction value) {
		return IndexRequest.of(index -> index.index("ideas-transactions")
				.id(key)
				.document(value));
	}

}
