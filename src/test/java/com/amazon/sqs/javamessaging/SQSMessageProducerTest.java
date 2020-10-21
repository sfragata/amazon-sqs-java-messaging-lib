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
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageProducerTest class
 */
public class SQSMessageProducerTest {

    public static final String QUEUE_URL = "QueueUrl";

    public static final String QUEUE_NAME = "QueueName";

    public static final String MESSAGE_ID_1 = "MessageId1";

    public static final String MESSAGE_ID_2 = "MessageId2";

    private SQSMessageProducer producer;

    private SQSQueueDestination destination;

    private SQSSession sqsSession;

    private SQSConnection sqsConnection;

    private AmazonSQSMessagingClientWrapper amazonSQSClient;

    private Acknowledger acknowledger;

    @Before
    public void setup()
        throws JMSException {

        this.amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        this.sqsConnection = mock(SQSConnection.class);

        this.acknowledger = mock(Acknowledger.class);

        this.sqsSession = mock(SQSSession.class);
        this.destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        this.producer = spy(new SQSMessageProducer(this.amazonSQSClient, this.sqsSession, this.destination));
    }

    /**
     * Test check If Destination Already Set
     */
    @Test
    public void testCheckIfDestinationAlreadySet()
        throws JMSException {

        try {
            this.producer.checkIfDestinationAlreadySet();
        } catch (UnsupportedOperationException uoe) {
            assertEquals("MessageProducer already specified a destination at creation time.", uoe.getMessage());
        }

        this.producer = spy(new SQSMessageProducer(this.amazonSQSClient, this.sqsSession, null));
        this.producer.checkIfDestinationAlreadySet();
    }

    /**
     * Test check closed
     */
    @Test
    public void testCheckClosed()
        throws JMSException {

        /*
         * Check exception is thrown when producer is closed
         */
        this.producer.isClosed().set(true);
        try {
            this.producer.checkClosed();
        } catch (IllegalStateException ise) {
            assertEquals("The producer is closed.", ise.getMessage());
        }

        /*
         * Check no op when producer is closed
         */
        this.producer.isClosed().set(false);
        this.producer.checkClosed();
    }

    /**
     * Test propertyToMessageAttribute with empty messages of different type
     */
    @Test
    public void testPropertyToMessageAttributeWithEmpty()
        throws JMSException {

        /*
         * Test Empty text message default attribute
         */
        SQSMessage sqsText = new SQSTextMessage();
        Map<String, MessageAttributeValue> messageAttributeText = this.producer.propertyToMessageAttribute(sqsText);

        assertEquals(0, messageAttributeText.size());

        /*
         * Test Empty object message default attribute
         */
        SQSMessage sqsObject = new SQSObjectMessage();
        Map<String, MessageAttributeValue> messageAttributeObject = this.producer.propertyToMessageAttribute(sqsObject);

        assertEquals(0, messageAttributeObject.size());

        /*
         * Test Empty byte message default attribute
         */
        MessageAttributeValue messageAttributeValueByte =
            MessageAttributeValue.builder().dataType("String").stringValue("byte").build();

        SQSMessage sqsByte = new SQSBytesMessage();
        Map<String, MessageAttributeValue> messageAttributeByte = this.producer.propertyToMessageAttribute(sqsByte);

        assertEquals(0, messageAttributeObject.size());
    }

    /**
     * Test propertyToMessageAttribute with messages of different type
     */
    @Test
    public void testPropertyToMessageAttribute()
        throws JMSException {

        internalTestPropertyToMessageAttribute(new SQSTextMessage());

        internalTestPropertyToMessageAttribute(new SQSObjectMessage());

        internalTestPropertyToMessageAttribute(new SQSBytesMessage());
    }

    public void internalTestPropertyToMessageAttribute(
        SQSMessage sqsText)
        throws JMSException {

        /*
         * Setup JMS message property
         */
        String booleanProperty = "BooleanProperty";
        String byteProperty = "ByteProperty";
        String shortProperty = "ShortProperty";
        String intProperty = "IntProperty";
        String longProperty = "LongProperty";
        String floatProperty = "FloatProperty";
        String doubleProperty = "DoubleProperty";
        String stringProperty = "StringProperty";
        String objectProperty = "ObjectProperty";

        sqsText.setBooleanProperty(booleanProperty, true);
        sqsText.setByteProperty(byteProperty, (byte) 1);
        sqsText.setShortProperty(shortProperty, (short) 2);
        sqsText.setIntProperty(intProperty, 3);
        sqsText.setLongProperty(longProperty, 4L);
        sqsText.setFloatProperty(floatProperty, (float) 5.0);
        sqsText.setDoubleProperty(doubleProperty, 6.0);
        sqsText.setStringProperty(stringProperty, "seven");
        sqsText.setObjectProperty(objectProperty, new Integer(8));

        MessageAttributeValue messageAttributeValueBoolean =
            MessageAttributeValue.builder().dataType("Number.Boolean").stringValue("1").build();

        MessageAttributeValue messageAttributeValueByte =
            MessageAttributeValue.builder().dataType("Number.byte").stringValue("1").build();

        MessageAttributeValue messageAttributeValueShort =
            MessageAttributeValue.builder().dataType("Number.short").stringValue("2").build();

        MessageAttributeValue messageAttributeValueInt =
            MessageAttributeValue.builder().dataType("Number.int").stringValue("3").build();

        MessageAttributeValue messageAttributeValueLong =
            MessageAttributeValue.builder().dataType("Number.long").stringValue("4").build();

        MessageAttributeValue messageAttributeValueFloat =
            MessageAttributeValue.builder().dataType("Number.float").stringValue("5.0").build();

        MessageAttributeValue messageAttributeValueDouble =
            MessageAttributeValue.builder().dataType("Number.double").stringValue("6.0").build();

        MessageAttributeValue messageAttributeValueString =
            MessageAttributeValue.builder().dataType("String").stringValue("seven").build();

        MessageAttributeValue messageAttributeValueObject =
            MessageAttributeValue.builder().dataType("Number.int").stringValue("8").build();

        MessageAttributeValue messageAttributeValueJMSSQSMessageType =
            MessageAttributeValue.builder().dataType("String").stringValue("text").build();

        /*
         * Convert property to sqs message attribute
         */
        Map<String, MessageAttributeValue> messageAttribute = this.producer.propertyToMessageAttribute(sqsText);

        /*
         * Verify results
         */
        assertEquals(messageAttributeValueBoolean, messageAttribute.get(booleanProperty));
        assertEquals(messageAttributeValueByte, messageAttribute.get(byteProperty));
        assertEquals(messageAttributeValueShort, messageAttribute.get(shortProperty));
        assertEquals(messageAttributeValueInt, messageAttribute.get(intProperty));
        assertEquals(messageAttributeValueLong, messageAttribute.get(longProperty));
        assertEquals(messageAttributeValueFloat, messageAttribute.get(floatProperty));
        assertEquals(messageAttributeValueDouble, messageAttribute.get(doubleProperty));
        assertEquals(messageAttributeValueString, messageAttribute.get(stringProperty));
        assertEquals(messageAttributeValueObject, messageAttribute.get(objectProperty));

    }

    /**
     * Test sendInternal input of Non SQS message
     */
    @Test
    public void testSendInternalNonSQSMessage()
        throws JMSException {

        Message msg = mock(Message.class);

        try {
            this.producer.sendInternal(this.destination, msg);
            fail();
        } catch (JMSException jmse) {
            // expected
        }
    }

    /**
     * Test sendInternal input of Non SQS message
     */
    @Test
    public void testSendInternalAlreadyClosed()
        throws JMSException {

        this.producer.isClosed().set(true);
        SQSMessage msg = mock(SQSMessage.class);

        try {
            this.producer.sendInternal(this.destination, msg);
            fail();
        } catch (JMSException jmse) {
            // expected
        }
    }

    /**
     * Test sendInternal input that SQSMessage is not a valid input
     */
    @Test
    public void testSendInternalNoMessageBody()
        throws JMSException {

        SQSMessage msg = mock(SQSMessage.class);

        try {
            this.producer.sendInternal(this.destination, msg);
            fail();
        } catch (JMSException jmse) {
            // expected
        }

        verify(msg).setJMSDestination(this.destination);
    }

    /**
     * Test sendInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessage()
        throws JMSException {

        String messageBody1 = "MyText1";
        String messageBody2 = "MyText2";
        SQSTextMessage msg = spy(new SQSTextMessage(messageBody1));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("text");

        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build())
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_2).build());

        this.producer.sendInternal(this.destination, msg);

        /*
         * Re send the message
         */
        msg.setText(messageBody2);
        this.producer.sendInternal(this.destination, msg);

        List<String> messagesBody = Arrays.asList(messageBody1, messageBody2);
        verify(this.amazonSQSClient, times(2))
            .sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody, messageAttributes)));
        verify(msg, times(2)).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessageFromReceivedMessage()
        throws JMSException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().dataType(SQSMessagingClientConstants.STRING)
                .stringValue(SQSMessage.TEXT_MESSAGE_TYPE).build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        software.amazon.awssdk.services.sqs.model.Message message =
            software.amazon.awssdk.services.sqs.model.Message.builder().messageAttributes(mapMessageAttributes)
                .attributesWithStrings(mapAttributes).body("MessageBody").build();

        SQSTextMessage msg = spy(new SQSTextMessage(this.acknowledger, QUEUE_URL, message));

        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build());

        this.producer.sendInternal(this.destination, msg);

        List<String> messagesBody = Arrays.asList("MessageBody");
        verify(this.amazonSQSClient)
            .sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody, mapMessageAttributes)));
        verify(msg).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test sendInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessage()
        throws JMSException {

        HashSet<String> set1 = new HashSet<String>();
        set1.add("data1");
        HashSet<String> set2 = new HashSet<String>();
        set2.add("data2");

        SQSObjectMessage msg = spy(new SQSObjectMessage(set1));
        String megBody1 = msg.getMessageBody();

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build())
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_2).build());

        this.producer.sendInternal(this.destination, msg);

        /*
         * Re send the message
         */
        msg.clearBody();
        msg.setObject(set2);
        String megBody2 = msg.getMessageBody();
        this.producer.sendInternal(this.destination, msg);

        ArgumentCaptor<SendMessageRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(this.amazonSQSClient, times(2)).sendMessage(argumentCaptor.capture());

        assertEquals(megBody1, argumentCaptor.getAllValues().get(0).messageBody());
        assertEquals(megBody2, argumentCaptor.getAllValues().get(1).messageBody());
        verify(msg, times(2)).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessageFromReceivedMessage()
        throws JMSException,
        IOException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();

        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().dataType(SQSMessagingClientConstants.STRING)
                .stringValue(SQSMessage.OBJECT_MESSAGE_TYPE).build();
        ;
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        // Encode an object to byte array
        Integer integer = new Integer("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        String messageBody = Base64.getEncoder().encodeToString(array.toByteArray());
        software.amazon.awssdk.services.sqs.model.Message message =
            software.amazon.awssdk.services.sqs.model.Message.builder().messageAttributes(mapMessageAttributes)
                .attributesWithStrings(mapAttributes).body(messageBody).build();

        SQSObjectMessage msg = spy(new SQSObjectMessage(this.acknowledger, QUEUE_URL, message));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build())
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_2).build());

        this.producer.sendInternal(this.destination, msg);

        verify(this.amazonSQSClient).sendMessage(
            argThat(new sendMessageRequestMatcher(QUEUE_URL, Arrays.asList(messageBody), messageAttributes)));
        verify(msg).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test sendInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessage()
        throws JMSException {

        SQSBytesMessage msg = spy(new SQSBytesMessage());
        msg.writeByte((byte) 0);
        msg.reset();

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("byte");

        String messageId = "MessageId";
        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build())
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_2).build());

        this.producer.sendInternal(this.destination, msg);

        /*
         * Re send the message
         */
        msg.clearBody();
        msg.writeInt(42);
        this.producer.sendInternal(this.destination, msg);

        List<String> messagesBody = Arrays.asList("AA==", "AAAAKg==");
        verify(this.amazonSQSClient, times(2))
            .sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody, messageAttributes)));

        verify(msg, times(2)).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessageFromReceivedMessage()
        throws JMSException,
        IOException {

        /*
         * Set up non JMS sqs message
         */
        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().dataType(SQSMessagingClientConstants.STRING)
                .stringValue(SQSMessage.BYTE_MESSAGE_TYPE).build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        String messageBody = Base64.getEncoder().encodeToString(byteArray);
        software.amazon.awssdk.services.sqs.model.Message message =
            software.amazon.awssdk.services.sqs.model.Message.builder().messageAttributes(mapMessageAttributes)
                .attributesWithStrings(mapAttributes).body(messageBody).build();

        SQSObjectMessage msg = spy(new SQSObjectMessage(this.acknowledger, QUEUE_URL, message));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(this.amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build())
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_2).build());

        this.producer.sendInternal(this.destination, msg);

        verify(this.amazonSQSClient).sendMessage(
            argThat(new sendMessageRequestMatcher(QUEUE_URL, Arrays.asList(messageBody), messageAttributes)));
        verify(msg).setJMSDestination(this.destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test getQueue
     */
    @Test
    public void testGetQueue()
        throws JMSException {

        assertEquals(this.destination, this.producer.getQueue());
    }

    /**
     * Test send with non SQSDestination
     */
    @Test
    public void testSendNonSQSDestination()
        throws JMSException {

        Queue queue = mock(Queue.class);

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        try {
            this.producer.send(queue, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }

        Destination destination = mock(Destination.class);
        try {
            this.producer.send(destination, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            this.producer.send(null, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    /**
     * Test send when destination already specified
     */
    @Test
    public void testSendDestinationAlreadySpecified()
        throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        try {
            this.producer.send(this.destination, msg);
            fail();
        } catch (UnsupportedOperationException ide) {
            // expected
        }

        verify(this.producer).checkIfDestinationAlreadySet();
    }

    /**
     * Test send with destination
     */
    @Test
    public void testSendWithDestination()
        throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));
        this.producer = spy(new SQSMessageProducer(this.amazonSQSClient, this.sqsSession, null));

        doNothing().when(this.producer).sendInternal(this.destination, msg);

        this.producer.send(this.destination, msg);

        verify(this.producer).checkIfDestinationAlreadySet();
        verify(this.producer).sendInternal(this.destination, msg);
    }

    /**
     * Test send API with unsupported feature are not used
     */
    @Test
    public void testSendDropUnsupportedFeatures()
        throws JMSException {

        int deliveryMode = 1;
        int priority = 1;
        long timeToLive = 1;
        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        doNothing().when(this.producer).send(this.destination, msg);
        doNothing().when(this.producer).send(msg);

        this.producer.send(this.destination, msg, deliveryMode, priority, timeToLive);
        this.producer.send(msg, deliveryMode, priority, timeToLive);

        verify(this.producer).send(this.destination, msg);
        verify(this.producer).send(msg);
    }

    /**
     * Test send without destination
     */
    @Test
    public void testSendWithoutDestination()
        throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        doNothing().when(this.producer).sendInternal(this.destination, msg);

        this.producer.send(msg);
        verify(this.producer).sendInternal(this.destination, msg);
    }

    /**
     * Test Close when producer is already closed
     */
    @Test
    public void testCloseAlreadyClosed()
        throws JMSException {

        this.producer.isClosed().set(true);

        this.producer.close();

        verify(this.sqsSession, never()).removeProducer(this.producer);
    }

    /**
     * Test Close
     */
    @Test
    public void testClose()
        throws JMSException {

        this.producer.close();

        verify(this.sqsSession).removeProducer(this.producer);
    }

    @Test
    public void testSetDeliveryDelay()
        throws JMSException {

        assertEquals(0, this.producer.getDeliveryDelay());

        this.producer.setDeliveryDelay(2000);

        assertEquals(2000, this.producer.getDeliveryDelay());

        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        when(this.amazonSQSClient.sendMessage(requestCaptor.capture()))
            .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID_1).build());

        SQSTextMessage msg = new SQSTextMessage("Sorry I'm late!");
        this.producer.send(msg);

        assertEquals(2, requestCaptor.getValue().delaySeconds().intValue());
    }

    @Test
    public void testSetDeliveryDelayInvalidDelays()
        throws JMSException {

        try {
            this.producer.setDeliveryDelay(-1);
            fail();
        } catch (IllegalArgumentException ide) {
            // expected
        }

        try {
            this.producer.setDeliveryDelay(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
            fail();
        } catch (IllegalArgumentException ide) {
            // expected
        }

        try {
            this.producer.setDeliveryDelay(20);
            fail();
        } catch (IllegalArgumentException ide) {
            // expected
        }
    }

    private Map<String, MessageAttributeValue> createMessageAttribute(
        String type) {

        MessageAttributeValue messageAttributeValue =
            MessageAttributeValue.builder().dataType("String").stringValue(type).build();

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        messageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        return messageAttributes;
    }

    private class sendMessageRequestMatcher
        extends ArgumentMatcher<SendMessageRequest> {

        private String queueUrl;

        private List<String> messagesBody;

        private Map<String, MessageAttributeValue> messageAttributes;

        private sendMessageRequestMatcher(
            String queueUrl,
            List<String> messagesBody,
            Map<String, MessageAttributeValue> messageAttributes) {

            this.queueUrl = queueUrl;
            this.messagesBody = messagesBody;
            this.messageAttributes = messageAttributes;
        }

        @Override
        public boolean matches(
            Object argument) {

            if (!(argument instanceof SendMessageRequest)) {
                return false;
            }

            SendMessageRequest reqeust = (SendMessageRequest) argument;
            assertEquals(this.queueUrl, reqeust.queueUrl());
            assertTrue(this.messagesBody.contains(reqeust.messageBody()));
            assertEquals(this.messageAttributes, reqeust.messageAttributes());
            return true;
        }
    }
}
