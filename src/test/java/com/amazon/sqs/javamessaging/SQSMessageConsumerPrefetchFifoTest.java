/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.Message.Builder;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageConsumerPrefetchTest class
 */
@RunWith(Parameterized.class)
public class SQSMessageConsumerPrefetchFifoTest {

    private static final String NAMESPACE = "123456789012";

    private static final String QUEUE_NAME = "QueueName.fifo";

    private static final String QUEUE_URL = NAMESPACE + "/" + QUEUE_NAME;

    private Acknowledger acknowledger;

    private NegativeAcknowledger negativeAcknowledger;

    private SQSSessionCallbackScheduler sqsSessionRunnable;

    private SQSMessageConsumerPrefetch consumerPrefetch;

    private ExponentialBackoffStrategy backoffStrategy;

    private AmazonSQSMessagingClientWrapper amazonSQSClient;

    @Parameters
    public static List<Object[]> getParameters() {

        return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 5 }, { 10 }, { 15 } });
    }

    private final int numberOfMessagesToPrefetch;

    public SQSMessageConsumerPrefetchFifoTest(int numberOfMessagesToPrefetch) {

        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;
    }

    @Before
    public void setup() {

        this.amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        SQSConnection parentSQSConnection = mock(SQSConnection.class);
        when(parentSQSConnection.getWrappedAmazonSQSClient()).thenReturn(this.amazonSQSClient);

        this.sqsSessionRunnable = mock(SQSSessionCallbackScheduler.class);

        this.acknowledger = mock(Acknowledger.class);

        this.negativeAcknowledger = mock(NegativeAcknowledger.class);

        this.backoffStrategy = mock(ExponentialBackoffStrategy.class);

        SQSQueueDestination sqsDestination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        this.consumerPrefetch =
            spy(new SQSMessageConsumerPrefetch(
                this.sqsSessionRunnable,
                this.acknowledger,
                this.negativeAcknowledger,
                sqsDestination,
                this.amazonSQSClient,
                this.numberOfMessagesToPrefetch));

        this.consumerPrefetch.backoffStrategy = this.backoffStrategy;
    }

    /**
     * Test one full prefetch operation works as expected
     */
    @Test
    public void testOneFullPrefetch()
        throws InterruptedException,
        JMSException {

        /*
         * Set up consumer prefetch and mocks
         */

        final int numMessages = this.numberOfMessagesToPrefetch > 0 ? this.numberOfMessagesToPrefetch : 1;
        List<software.amazon.awssdk.services.sqs.model.Message> messages = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            messages.add(createValidFifoMessage(i, "G" + i).build());
        }

        // First start the consumer prefetch
        this.consumerPrefetch.start();

        // Mock SQS call for receive message and return messages
        final int receiveMessageLimit = Math.min(10, numMessages);
        when(this.amazonSQSClient.receiveMessage(argThat(new ArgumentMatcher<ReceiveMessageRequest>() {
            @Override
            public boolean matches(
                Object argument) {

                if (!(argument instanceof ReceiveMessageRequest)) {
                    return false;
                }
                ReceiveMessageRequest other = (ReceiveMessageRequest) argument;

                return other.queueUrl().equals(QUEUE_URL)
                    && other.maxNumberOfMessages() == receiveMessageLimit
                    && other.messageAttributeNames().size() == 1
                    && other.messageAttributeNames().get(0).equals(SQSMessageConsumerPrefetch.ALL)
                    && other.waitTimeSeconds() == SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS
                    && other.receiveRequestAttemptId() != null
                    && other.receiveRequestAttemptId().length() > 0;
            }
        }))).thenReturn(ReceiveMessageResponse.builder().messages(messages).build());

        // Mock isClosed and exit after a single prefetch loop
        when(this.consumerPrefetch.isClosed()).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(true);

        /*
         * Request a message (only relevant when prefetching is off).
         */
        this.consumerPrefetch.requestMessage();

        /*
         * Run the prefetch
         */
        this.consumerPrefetch.run();

        /*
         * Verify the results
         */

        // Ensure Consumer was started
        verify(this.consumerPrefetch).waitForStart();

        // Ensure Consumer Prefetch backlog is not full
        verify(this.consumerPrefetch).waitForPrefetch();

        // Ensure no message was nack
        verify(this.negativeAcknowledger).action(QUEUE_URL, new ArrayList<String>());

        // Ensure retries attempt was not increased
        assertEquals(0, this.consumerPrefetch.retriesAttempted);

        // Ensure message queue was filled with expected messages
        assertEquals(numMessages, this.consumerPrefetch.messageQueue.size());
        int index = 0;
        for (SQSMessageConsumerPrefetch.MessageManager messageManager : this.consumerPrefetch.messageQueue) {
            software.amazon.awssdk.services.sqs.model.Message mockedMessage = messages.get(index);
            SQSMessage sqsMessage = (SQSMessage) messageManager.getMessage();
            assertEquals("Receipt handle is the same", mockedMessage.receiptHandle(), sqsMessage.getReceiptHandle());
            assertEquals("Group id is the same",
                mockedMessage.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
                sqsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
            assertEquals("Sequence number is the same",
                mockedMessage.attributesAsStrings().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
                sqsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
            assertEquals("Deduplication id is the same",
                mockedMessage.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
                sqsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));

            index++;
        }
    }

    /**
     * Test ConvertToJMSMessage when message type is not set in the message attribute
     */
    @Test
    public void testConvertToJMSMessageNoTypeAttribute()
        throws JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        Builder createValidFifoMessage = createValidFifoMessage(1, "G");
        // Return message attribute with no message type attribute
        createValidFifoMessage.body("MessageBody");

        Message message = createValidFifoMessage.build();
        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jmsMessage = this.consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jmsMessage instanceof SQSTextMessage);
        assertEquals(((SQSTextMessage) jmsMessage).getText(), "MessageBody");
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /**
     * Test ConvertToJMSMessage with byte message type
     */
    @Test
    public void testConvertToJMSMessageByteTypeAttribute()
        throws JMSException,
        IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Builder createValidFifoMessage = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'BYTE'
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING).build();
        Map<String, MessageAttributeValue> messageAttributeMap = new HashMap<>();
        messageAttributeMap.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        createValidFifoMessage.body(Base64.getEncoder().encodeToString(byteArray));
        createValidFifoMessage.messageAttributes(messageAttributeMap);
        software.amazon.awssdk.services.sqs.model.Message message = createValidFifoMessage.build();
        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jmsMessage = this.consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jmsMessage instanceof SQSBytesMessage);
        for (byte b : byteArray) {
            assertEquals(b, ((SQSBytesMessage) jmsMessage).readByte());
        }
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /**
     * Test ConvertToJMSMessage with byte message that contains illegal sqs message body
     */
    @Test
    public void testConvertToJMSMessageByteTypeIllegalBody()
        throws JMSException,
        IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Builder createValidFifoMessage = createValidFifoMessage(1, "G");
        createValidFifoMessage.body("Text Message");

        // Return message attributes with message type 'BYTE'
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING).build();
        Map<String, MessageAttributeValue> map = new HashMap<>();
        map.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        createValidFifoMessage.messageAttributes(map);
        // Return illegal message body for byte message type
        software.amazon.awssdk.services.sqs.model.Message message = createValidFifoMessage.build();

        /*
         * Convert the SQS message to JMS Message
         */
        try {
            this.consumerPrefetch.convertToJMSMessage(message);
            fail("Expect JMSException");
        } catch (JMSException jmse) {
            // Expected JMS exception
        }
    }

    /**
     * Test ConvertToJMSMessage with an object message
     */
    @Test
    public void testConvertToJMSMessageObjectTypeAttribute()
        throws JMSException,
        IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Builder validFifoMessage = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'OBJECT'
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING).build();
        Map<String, MessageAttributeValue> map = new HashMap<>();
        map.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        // Encode an object to byte array
        Integer integer = new Integer("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        validFifoMessage.messageAttributes(map);
        validFifoMessage.body(Base64.getEncoder().encodeToString(array.toByteArray()));

        software.amazon.awssdk.services.sqs.model.Message message = validFifoMessage.build();
        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jmsMessage = this.consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jmsMessage instanceof SQSObjectMessage);
        assertEquals(integer, ((SQSObjectMessage) jmsMessage).getObject());
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /**
     * Test ConvertToJMSMessage with an object message that contains illegal sqs message body
     */
    @Test
    public void testConvertToJMSMessageObjectIllegalBody()
        throws JMSException,
        IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Builder validFifoMessage = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'OBJECT'
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING).build();
        Map<String, MessageAttributeValue> map = new HashMap<>();
        map.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        validFifoMessage.body("Some text that does not represent an object");
        validFifoMessage.messageAttributes(map);

        software.amazon.awssdk.services.sqs.model.Message message = validFifoMessage.build();

        /*
         * Convert the SQS message to JMS Message
         */
        ObjectMessage jmsMessage = (ObjectMessage) this.consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        try {
            jmsMessage.getObject();
            fail("Expect JMSException");
        } catch (JMSException jmse) {
            // Expected JMS exception
        }
    }

    /**
     * Test ConvertToJMSMessage with text message with text type attribute
     */
    @Test
    public void testConvertToJMSMessageTextTypeAttribute()
        throws JMSException,
        IOException {

        /*
         * Set up consumer prefetch and mocks
         */
        Builder validFifoMessage = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'OBJECT'
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().stringValue(SQSMessage.TEXT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING).build();
        Map<String, MessageAttributeValue> map = new HashMap<>();
        map.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        validFifoMessage.messageAttributes(map);

        validFifoMessage.body("MessageBody");

        software.amazon.awssdk.services.sqs.model.Message message = validFifoMessage.build();

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jmsMessage = this.consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jmsMessage instanceof SQSTextMessage);
        assertEquals(message.body(), "MessageBody");
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.attributesAsStrings().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
            jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /*
     * Utility functions
     */

    private Builder createValidFifoMessage(
        int messageNumber,
        String groupId) {

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, "10000000000000000000" + messageNumber);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, "d" + messageNumber);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId);

        return software.amazon.awssdk.services.sqs.model.Message.builder().receiptHandle("r" + messageNumber)
            .attributesWithStrings(mapAttributes);
    }

}
