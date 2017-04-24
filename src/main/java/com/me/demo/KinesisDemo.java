package com.me.demo;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KinesisDemo {

    private static Logger LOGGER = null;
    private static LambdaLogger LAMBDA_LOGGER = null;

    private static final String STREAM_NAME = "best-stream-in-the-world";

    private AmazonKinesis kinesis = null;

    private void run() throws UnsupportedEncodingException {

        kinesis = AmazonKinesisClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        try {
            DescribeStreamResult result = kinesis.describeStream(STREAM_NAME);
            logIt("Stream State: " + result.getStreamDescription().getStreamStatus());
        } catch(ResourceNotFoundException e) {
            logIt("Please create stream: " + STREAM_NAME + " with AWS console");
            System.exit(1);
        }

        setupConsumer();

        produce();
    }

    private void produce() throws UnsupportedEncodingException {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        config.setRegion(Regions.US_WEST_2.getName());

        KinesisProducer kinesis = new KinesisProducer(config);
        FutureCallback<UserRecordResult> myCallback =
                new FutureCallback<UserRecordResult>() {
                    @Override
                    public void onFailure(Throwable t) {
                        logIt("FAIL");
                        System.out.println("fail");
                    }

                    @Override
                    public void onSuccess(UserRecordResult result) {
                        logIt("SUCCESS");
                        System.out.println("success");
                    }
                };

        for (int i = 0; i < 10; ++i) {
            ByteBuffer data = ByteBuffer.wrap(("SomeRecordData_" + i).getBytes("UTF-8"));
            String partitionKey = "key" + i;
            ListenableFuture<UserRecordResult> f =
                    kinesis.addUserRecord(STREAM_NAME, partitionKey, data);

            // If the Future is complete by the time we call addCallback,
            // the callback will be invoked immediately.
            Futures.addCallback(f, myCallback);
        }
    }

    private void setupConsumer() {

        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(
                        "KinesisDemoConsumer",
                        STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        "KinesisDemoConsumer")
                        .withRegionName(Regions.US_WEST_2.getName())
                        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        final IRecordProcessorFactory recordProcessorFactory = new BestRecordProcessorFactory();

        new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build()
                .run();

    }

    static class BestRecordProcessor implements IRecordProcessor {

        @Override
        public void initialize(InitializationInput initializationInput) {
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {

            processRecordsInput.getRecords().stream().forEach(record -> {
                logIt("Received record: " + createString(record.getData()));
            });
        }

        private String createString(ByteBuffer data) {
            return StandardCharsets.UTF_8.decode(data).toString();
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
        }
    }

    static class BestRecordProcessorFactory implements IRecordProcessorFactory {

        @Override
        public IRecordProcessor createProcessor() {
            return new BestRecordProcessor();
        }
    }

    // Trying to get logging to work in Lambda
    private static void logIt(String s) {
//        System.out.println(s);
        if (LAMBDA_LOGGER != null) {
            LAMBDA_LOGGER.log(s);
        }
        if (LOGGER != null) {
            LOGGER.info(s);
        }
    }

    // For running locally
    public static void main(String[] args) throws Exception {
        LOGGER = LoggerFactory.getLogger(KinesisDemo.class.getName());
        KinesisDemo demo = new KinesisDemo();
        demo.run();
    }

    // For running as lambda
    public String myHandler(Map<String, Object> input, Context context) {
        LAMBDA_LOGGER = context.getLogger();
        logIt("Lambda started");
        try {
            run();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return String.format("Hello %s. log stream = %s", input.values(), context.getLogStreamName());
    }
}