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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import org.junit.Assert;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Parent class for the Acknowledger tests
 */
public class AcknowledgerCommon {

    protected String baseQueueUrl = "queueUrl";

    protected Acknowledger acknowledger;

    protected AmazonSQSMessagingClientWrapper amazonSQSClient;

    protected List<SQSMessage> populatedMessages = new ArrayList<SQSMessage>();

    /*
     * Generate and populate the list with sqs message from different queues
     */
    public void populateMessage(
        int populateMessageSize)
        throws JMSException {

        String queueUrl = this.baseQueueUrl + 0;
        for (int i = 0; i < populateMessageSize; i++) {
            // Change queueUrl depending on how many messages there are.
            if (i == 11) {
                queueUrl = this.baseQueueUrl + 1;
            } else if (i == 22) {
                queueUrl = this.baseQueueUrl + 2;
            } else if (i == 33) {
                queueUrl = this.baseQueueUrl + 3;
            } else if (i == 44) {
                queueUrl = this.baseQueueUrl + 4;
            }

            // Add mock Attributes
            Map<String, String> mockAttributes = new HashMap<String, String>();
            mockAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "2");
            Message sqsMessage =
                Message.builder().receiptHandle("ReceiptHandle" + i).messageId("MessageId" + i)
                    .attributesWithStrings(mockAttributes).build();

            SQSMessage message = new SQSTextMessage(this.acknowledger, queueUrl, sqsMessage);

            this.populatedMessages.add(message);
            this.acknowledger.notifyMessageReceived(message);
        }
        Assert.assertEquals(populateMessageSize, this.acknowledger.getUnAckMessages().size());
    }
}
