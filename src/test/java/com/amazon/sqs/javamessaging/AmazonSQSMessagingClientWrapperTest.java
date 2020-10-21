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

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test the AmazonSQSMessagingClientWrapper class
 */
public class AmazonSQSMessagingClientWrapperTest {

    private final static String QUEUE_NAME = "queueName";

    private static final String OWNER_ACCOUNT_ID = "accountId";

    private SqsClient amazonSQSClient;

    private AmazonSQSMessagingClientWrapper wrapper;

    @Before
    public void setup()
        throws JMSException {

        this.amazonSQSClient = mock(SqsClient.class);
        this.wrapper = new AmazonSQSMessagingClientWrapper(this.amazonSQSClient);
    }

    /*
     * Test constructing client with null amazon sqs client
     */
    @Test(expected = JMSException.class)
    public void testNullSQSClient()
        throws JMSException {

        new AmazonSQSMessagingClientWrapper(null);
    }

    /*
     * Test set endpoint
     */
    @Test
    @Ignore("deprecated")
    public void testSetEndpoint()
        throws JMSException {

    }

    /*
     * Test set endpoint wrap amazon sqs client exception
     */
    @Test(expected = JMSException.class)
    @Ignore("deprecated")
    public void testSetEndpointThrowIllegalArgumentException()
        throws JMSException {

    }

    /*
     * Test set region
     */
    @Test
    @Ignore("deprecated")
    public void testSetRegion()
        throws JMSException {

    }

    /*
     * Test set region wrap amazon sqs client exception
     */
    @Test(expected = JMSException.class)
    @Ignore("deprecated")
    public void testSetRegionThrowIllegalArgumentException()
        throws JMSException {

    }

    /*
     * Test delete message wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowSdkException()
        throws JMSException {

        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .deleteMessage(eq(deleteMessageRequest));

        this.wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonServiceException()
        throws JMSException {

        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .deleteMessage(eq(deleteMessageRequest));

        this.wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowSdkException()
        throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .deleteMessageBatch(eq(deleteMessageBatchRequest));

        this.wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonServiceException()
        throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .deleteMessageBatch(eq(deleteMessageBatchRequest));

        this.wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowSdkException()
        throws JMSException {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .sendMessage(eq(sendMessageRequest));

        this.wrapper.sendMessage(sendMessageRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonServiceException()
        throws JMSException {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .sendMessage(eq(sendMessageRequest));

        this.wrapper.sendMessage(sendMessageRequest);
    }

    /*
     * Test getQueueUrl with queue name input
     */
    @Test
    public void testGetQueueUrlQueueName()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();

        this.wrapper.getQueueUrl(QUEUE_NAME);
        verify(this.amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name and owner account id input
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountId()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest =
            GetQueueUrlRequest.builder().queueName(QUEUE_NAME).queueOwnerAWSAccountId(OWNER_ACCOUNT_ID).build();

        this.wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
        verify(this.amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowSdkException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonServiceException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameThrowQueueDoesNotExistException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameWithAccountIdThrowQueueDoesNotExistException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest =
            GetQueueUrlRequest.builder().queueName(QUEUE_NAME).queueOwnerAWSAccountId(OWNER_ACCOUNT_ID).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
    }

    /*
     * Test getQueueUrl
     */
    @Test
    public void testGetQueueUrl()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();

        this.wrapper.getQueueUrl(getQueueUrlRequest);
        verify(this.amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowSdkException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(getQueueUrlRequest);
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonServiceException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test queue exist
     */
    @Test
    public void testQueueExistsWhenQueueIsPresent()
        throws JMSException {

        assertTrue(this.wrapper.queueExists(QUEUE_NAME));
        verify(this.amazonSQSClient).getQueueUrl(eq(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build()));
    }

    /*
     * Test queue exist when amazon sqs client throws QueueDoesNotExistException
     */
    @Test
    public void testQueueExistsThrowQueueDoesNotExistException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        assertFalse(this.wrapper.queueExists(QUEUE_NAME));
    }

    /*
     * Test queue exist when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowSdkException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.queueExists(QUEUE_NAME);
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowAmazonServiceException()
        throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .getQueueUrl(eq(getQueueUrlRequest));

        this.wrapper.queueExists(QUEUE_NAME);
    }

    /*
     * Test create queue with name input
     */
    @Test
    public void testCreateQueueWithName()
        throws JMSException {

        this.wrapper.createQueue(QUEUE_NAME);
        verify(this.amazonSQSClient).createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build());
    }

    /*
     * Test create queue when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowSdkException()
        throws JMSException {

        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        this.wrapper.createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowAmazonServiceException()
        throws JMSException {

        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        this.wrapper.createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue
     */
    @Test
    public void testCreateQueue()
        throws JMSException {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();

        this.wrapper.createQueue(createQueueRequest);
        verify(this.amazonSQSClient).createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowSdkException()
        throws JMSException {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .createQueue(eq(createQueueRequest));

        this.wrapper.createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonServiceException()
        throws JMSException {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .createQueue(eq(createQueueRequest));

        this.wrapper.createQueue(createQueueRequest);
    }

    /*
     * Test receive message
     */
    @Test
    public void testReceiveMessage()
        throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        this.wrapper.receiveMessage(getQueueUrlRequest);
        verify(this.amazonSQSClient).receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowSdkException()
        throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .receiveMessage(eq(getQueueUrlRequest));

        this.wrapper.receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonServiceException()
        throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .receiveMessage(eq(getQueueUrlRequest));

        this.wrapper.receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test change message visibility
     */
    @Test
    public void testChangeMessageVisibility()
        throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
            ChangeMessageVisibilityRequest.builder().build();
        this.wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
        verify(this.amazonSQSClient).changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowSdkException()
        throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
            ChangeMessageVisibilityRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .changeMessageVisibility(eq(changeMessageVisibilityRequest));

        this.wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonServiceException()
        throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
            ChangeMessageVisibilityRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .changeMessageVisibility(eq(changeMessageVisibilityRequest));

        this.wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility batch
     */
    @Test
    public void testChangeMessageVisibilityBatch()
        throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest =
            ChangeMessageVisibilityBatchRequest.builder().build();
        this.wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        verify(this.amazonSQSClient).changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws SdkException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowSdkException()
        throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest =
            ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(SdkException.builder().message("ace").build()).when(this.amazonSQSClient)
            .changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        this.wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException()
        throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest =
            ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(AwsServiceException.builder().message("ase").build()).when(this.amazonSQSClient)
            .changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        this.wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test get amazon SQS client
     */
    @Test
    public void testGetAmazonSQSClient() {

        assertEquals(this.amazonSQSClient, this.wrapper.getAmazonSQSClient());
    }
}
