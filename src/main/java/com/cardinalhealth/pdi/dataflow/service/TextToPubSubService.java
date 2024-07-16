package com.cardinalhealth.pdi.dataflow.service;

import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
import com.cardinalhealth.pdi.dataflow.options.TextToPubSubPipelineOptions;
import com.cardinalhealth.pdi.dataflow.transformations.KVtoStringFn;
import com.cardinalhealth.pdi.dataflow.transformations.PrefixLineWithFileHashFn;
import com.cardinalhealth.pdi.dataflow.transformations.PrintElementFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;


public class TextToPubSubService {
    private static final Logger logger = LoggerFactory.getLogger(TextToPubSubService.class);


    public void process(TextToPubSubPipelineOptions options) throws Exception {
        logger.info("text to pubsub process started");

        validateOptions(options);

        Pipeline pipeline = Pipeline.create(options);
        String inputBucketPath = options.getInputBucketPath().get();

        CloudStorageHelper metaDataHelper = new CloudStorageHelper();

        TupleTag<List<String>> mainData = new TupleTag<>();
        TupleTag<KV<String, String>> failedData = new TupleTag<>();

        PCollection<FileIO.ReadableFile> allFiles = pipeline.apply("Read All Files",
                        FileIO.match().filepattern(inputBucketPath)
                                .continuously(
                                        Duration.standardSeconds(30),
                                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardHours(1))))
                .apply(FileIO.readMatches());

        PCollectionTuple tupleData = allFiles.apply("Prefix Content With File Hash",
                ParDo.of(new PrefixLineWithFileHashFn(metaDataHelper, failedData))
                        .withOutputTags(mainData, TupleTagList.of(failedData)));

        tupleData.get(mainData)
                .setCoder(ListCoder.of(StringUtf8Coder.of()))
                .apply("Flatten the lines", FlatMapElements.into(strings()).via((List<String> lines) -> lines))
                .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

      PCollection<String> bounded =  tupleData.get(failedData)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .apply("KVToString", ParDo.of(new KVtoStringFn()))
//                .apply("ApplyWindowing",
//                        Window.<String>into(new GlobalWindows())
//                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
//                                        .plusDelayOf(Duration.standardSeconds(5))))
//                        .withAllowedLateness(Duration.standardDays(1))
//                        .discardingFiredPanes()
//                );

              .apply("ApplyWindowing",
                      Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                              .withAllowedLateness(Duration.standardMinutes(10))
                              .discardingFiredPanes()
                              .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

                bounded.apply("Write Failed URIs to Bucket", TextIO.write()
                        .to(inputBucketPath.replaceAll("\\*","")+"failed/")
                        .withSuffix(".txt")
                                .withWindowedWrites());


        pipeline.run();
    }

    private void validateOptions(TextToPubSubPipelineOptions options) {
        logger.info("validating options");
        Predicate<String> isValidBucketName = (bucketName) -> !bucketName.isEmpty();
        Predicate<String> isValidTopicName = topic -> !topic.isEmpty();

        if (options == null) {
            throw new IllegalArgumentException("Options cannot be null");
        }

        Optional.ofNullable(options.getInputBucketPath())
                .map(ValueProvider::get)
                .filter(isValidBucketName)
                .orElseThrow(() -> new IllegalArgumentException("Invalid bucket name"));

        Optional.ofNullable(options.getOutputTopic())
                .map(ValueProvider::get)
                .filter(isValidTopicName)
                .orElseThrow(() -> new IllegalArgumentException("Invalid pubsub topic name"));
    }
}
