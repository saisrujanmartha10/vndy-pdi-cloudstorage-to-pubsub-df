package com.cardinalhealth.pdi.dataflow.app;

import com.cardinalhealth.pdi.dataflow.annotations.Template;
import com.cardinalhealth.pdi.dataflow.annotations.TemplateCategory;
import com.cardinalhealth.pdi.dataflow.options.TextToPubSubPipelineOptions;

import com.cardinalhealth.pdi.dataflow.service.TextToPubSubService;

import org.apache.beam.sdk.options.PipelineOptionsFactory;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Template(
        name = "GCS_Text_to_Cloud_PubSub",
        category = TemplateCategory.BATCH,
        displayName = "Cloud Storage Text File to Pub/Sub (Batch)",
        description = {
                "This template creates a batch pipeline that reads records from text files stored in Cloud Storage and publishes them to a Pub/Sub topic. "
                        + "The template can be used to publish records in a newline-delimited file containing JSON records or CSV file to a Pub/Sub topic for real-time processing. "
                        + "You can use this template to replay data to Pub/Sub.\n",
                "This template does not set any timestamp on the individual records. The event time is equal to the publishing time during execution. "
                        + "If your pipeline relies on an accurate event time for processing, you must not use this pipeline."
        },
        optionsClass = TextToPubSubPipelineOptions.class,
        documentation =
                "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-pubsub",
        contactInformation = "https://cloud.google.com/support",
        requirements = {
                "The files to read need to be in newline-delimited JSON or CSV format. Records spanning multiple lines in the source files might cause issues downstream because each line within the files will be published as a message to Pub/Sub.",
                "The Pub/Sub topic must exist before running the pipeline."
        })
public class TextToPubSubApplication {
    private static final Logger logger = LoggerFactory.getLogger(TextToPubSubApplication.class);

    public static void main(String[] args)  {
        logger.info("Started app");
        TextToPubSubPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TextToPubSubPipelineOptions.class);
        TextToPubSubService textToPubSubService = new TextToPubSubService();
        try {
            textToPubSubService.process(options);
        }catch (Exception exception){
            logger.error("Exception thrown while running Cloud Storage Text File to Pub/Sub", exception);
        }

    }

}