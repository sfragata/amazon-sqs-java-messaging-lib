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

import java.net.URI;

import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

/**
 * A ConnectionFactory object encapsulates a set of connection configuration
 * parameters for <code>AmazonSQSClient</code> as well as setting
 * <code>numberOfMessagesToPrefetch</code>.
 * <P>
 * The <code>numberOfMessagesToPrefetch</code> parameter is used to size of the
 * prefetched messages, which can be tuned based on the application workload. It
 * helps in returning messages from internal buffers(if there is any) instead of
 * waiting for the SQS <code>receiveMessage</code> call to return.
 * <P>
 * If more physical connections than the default maximum value (that is 50 as of
 * today) are needed on the connection pool,
 * {@link com.amazonaws.ClientConfiguration} needs to be configured.
 * <P>
 * None of the <code>createConnection</code> methods set-up the physical
 * connection to SQS, so validity of credentials are not checked with those
 * methods.
 */

public class SQSConnectionFactory
    implements QueueConnectionFactory {
    private final ProviderConfiguration providerConfiguration;

    private final AmazonSQSClientSupplier amazonSQSClientSupplier;

    /*
     * At the time when the library will stop supporting Java 7, this can be removed and Supplier<T> from Java 8 can be
     * used directly.
     */
    private interface AmazonSQSClientSupplier {
        SqsClient get();
    }

    /*
     * Creates a SQSConnectionFactory that uses AmazonSQSClientBuilder.standard() for creating AmazonSQS client
     * connections. Every SQSConnection will have its own copy of AmazonSQS client.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration) {

        this(providerConfiguration, SqsClient.builder());
    }

    /*
     * Creates a SQSConnectionFactory that uses the provided AmazonSQS client connection. Every SQSConnection will use
     * the same provided AmazonSQS client.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final SqsClient client) {

        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (client == null) {
            throw new IllegalArgumentException("AmazonSQS client cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public SqsClient get() {

                return client;
            }
        };
    }

    /*
     * Creates a SQSConnectionFactory that uses the provided AmazonSQSClientBuilder for creating AmazonSQS client
     * connections. Every SQSConnection will have its own copy of AmazonSQS client created through the provided builder.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final SqsClientBuilder clientBuilder) {

        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (clientBuilder == null) {
            throw new IllegalArgumentException("AmazonSQS client builder cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public SqsClient get() {

                return clientBuilder.build();
            }
        };
    }

    private SQSConnectionFactory(final Builder builder) {

        this.providerConfiguration = builder.providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public SqsClient get() {

                SqsClientBuilder sqsClientBuilder =
                    SqsClient.builder().credentialsProvider(builder.awsCredentialsProvider)
                        .overrideConfiguration(builder.clientConfiguration);
                if (builder.region != null) {
                    sqsClientBuilder.region(builder.region);
                }
                if (builder.endpoint != null) {
                    sqsClientBuilder.endpointOverride(URI.create(builder.endpoint));
                }
                // if (builder.signerRegionOverride != null) {
                // amazonSQSClient.setSignerRegionOverride(builder.signerRegionOverride);
                // }
                return sqsClientBuilder.build();
            }
        };
    }

    @Override
    public SQSConnection createConnection()
        throws JMSException {

        try {
            SqsClient amazonSQS = this.amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, null);
        } catch (RuntimeException e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    @Override
    public SQSConnection createConnection(
        String awsAccessKeyId,
        String awsSecretKey)
        throws JMSException {

        AwsBasicCredentials basicAWSCredentials = AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey);
        return createConnection(basicAWSCredentials);
    }

    public SQSConnection createConnection(
        AwsCredentials awsCredentials)
        throws JMSException {

        AwsCredentialsProvider awsCredentialsProvider = StaticCredentialsProvider.create(awsCredentials);
        return createConnection(awsCredentialsProvider);
    }

    public SQSConnection createConnection(
        AwsCredentialsProvider awsCredentialsProvider)
        throws JMSException {

        try {
            SqsClient amazonSQS = this.amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, awsCredentialsProvider);
        } catch (Exception e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    private SQSConnection createConnection(
        SqsClient amazonSQS,
        AwsCredentialsProvider awsCredentialsProvider)
        throws JMSException {

        AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper =
            new AmazonSQSMessagingClientWrapper(amazonSQS, awsCredentialsProvider);
        return new SQSConnection(amazonSQSClientJMSWrapper, this.providerConfiguration.getNumberOfMessagesToPrefetch());
    }

    @Override
    public QueueConnection createQueueConnection()
        throws JMSException {

        return createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(
        String userName,
        String password)
        throws JMSException {

        return createConnection(userName, password);
    }

    /**
     * Deprecated. Use one of the constructors of this class instead and provide either AmazonSQS client or AmazonSQSClientBuilder.
     * @return
     */
    @Deprecated
    public static Builder builder() {

        return new Builder();
    }

    /**
     * Deprecated. Use one of the constructors of SQSConnectionFactory instead.
     * @return
     */
    @Deprecated
    public static class Builder {
        private Region region;

        private String endpoint;

        private String signerRegionOverride;

        private ClientOverrideConfiguration clientConfiguration;

        private AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();

        private ProviderConfiguration providerConfiguration;

        public Builder(Region region) {

            this();
            this.region = region;
        }

        /** Recommended way to set the AmazonSQSClient is to use region
         * @param region region */
        public Builder(String region) {

            this(Region.of(region));
        }

        public Builder() {

            this.providerConfiguration = new ProviderConfiguration();
            this.clientConfiguration = ClientOverrideConfiguration.builder().build();
        }

        public Builder withRegion(
            Region region) {

            setRegion(region);
            return this;
        }

        public Builder withRegionName(
            String regionName)
            throws IllegalArgumentException {

            setRegion(Region.of(regionName));
            return this;
        }

        public Builder withEndpoint(
            String endpoint) {

            setEndpoint(endpoint);
            return this;
        }

        /**
         * An internal method used to explicitly override the internal signer region
         * computed by the default implementation. This method is not expected to be
         * normally called except for AWS internal development purposes.
         */
        public Builder withSignerRegionOverride(
            String signerRegionOverride) {

            setSignerRegionOverride(signerRegionOverride);
            return this;
        }

        public Builder withAWSCredentialsProvider(
            AwsCredentialsProvider awsCredentialsProvider) {

            setAwsCredentialsProvider(awsCredentialsProvider);
            return this;
        }

        public Builder withClientConfiguration(
            ClientOverrideConfiguration clientConfig) {

            setClientConfiguration(clientConfig);
            return this;
        }

        public Builder withNumberOfMessagesToPrefetch(
            int numberOfMessagesToPrefetch) {

            this.providerConfiguration.setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
            return this;
        }

        public SQSConnectionFactory build() {

            return new SQSConnectionFactory(this);
        }

        public Region getRegion() {

            return this.region;
        }

        public void setRegion(
            Region region) {

            this.region = region;
            this.endpoint = null;
        }

        public void setRegionName(
            String regionName) {

            setRegion(Region.of(regionName));
        }

        public String getEndpoint() {

            return this.endpoint;
        }

        public void setEndpoint(
            String endpoint) {

            this.endpoint = endpoint;
            this.region = null;
        }

        public String getSignerRegionOverride() {

            return this.signerRegionOverride;
        }

        public void setSignerRegionOverride(
            String signerRegionOverride) {

            this.signerRegionOverride = signerRegionOverride;
        }

        public ClientOverrideConfiguration getClientConfiguration() {

            return this.clientConfiguration;
        }

        public void setClientConfiguration(
            ClientOverrideConfiguration clientConfig) {

            this.clientConfiguration = clientConfig;
        }

        public int getNumberOfMessagesToPrefetch() {

            return this.providerConfiguration.getNumberOfMessagesToPrefetch();
        }

        public void setNumberOfMessagesToPrefetch(
            int numberOfMessagesToPrefetch) {

            this.providerConfiguration.setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
        }

        public AwsCredentialsProvider getAwsCredentialsProvider() {

            return this.awsCredentialsProvider;
        }

        public void setAwsCredentialsProvider(
            AwsCredentialsProvider awsCredentialsProvider) {

            this.awsCredentialsProvider = awsCredentialsProvider;
        }
    }

}
