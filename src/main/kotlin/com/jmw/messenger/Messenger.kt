package com.jmw.messenger

import com.google.cloud.spring.pubsub.core.PubSubTemplate
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage
import com.google.pubsub.v1.PubsubMessage
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

open class Messenger(
        subscription: String,
        private val topic: String,
        private val pubSubTemplate: PubSubTemplate
) {

    private val messageBook = ConcurrentHashMap<String, CompletableFuture<BasicAcknowledgeablePubsubMessage>>()

    private val messageConsumer = Consumer { message: BasicAcknowledgeablePubsubMessage ->
        val messengerId = message.pubsubMessage.attributesMap["messenger-id"] ?: message.nack()
        messageBook[messengerId]?.complete(message) ?: message.nack()
    }

    init {
        pubSubTemplate.subscribe(subscription, messageConsumer)
    }

    fun relayMessage(pubsubMessage: PubsubMessage) : CompletableFuture<BasicAcknowledgeablePubsubMessage> {
        val messengerId = UUID.randomUUID().toString()
        pubsubMessage.attributesMap["messenger-id"] = messengerId
        pubSubTemplate.publish(topic, pubsubMessage).get()
        val future = CompletableFuture<BasicAcknowledgeablePubsubMessage>()
        messageBook[messengerId] = future
        return future
    }

}