package com.cardinalhealth.pdi.dataflow.options;

import com.cardinalhealth.pdi.dataflow.annotations.TemplateParameter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface TextToPubSubPipelineOptions extends PipelineOptions {
        @TemplateParameter.GcsReadFile(
                order = 1,
                groupName = "Input Cloud Storage Bucket Path",
                description = "Input Cloud Storage Bucket Path",
                helpText = "The input path to read from.",
                example = "gs://bucket-name/files")
        @Validation.Required
        ValueProvider<String> getInputBucketPath();

        void setInputBucketPath(ValueProvider<String> value);

        @TemplateParameter.PubsubTopic(
                order = 2,
                groupName = "Target PubSub Topic",
                description = "Output Pub/Sub topic",
                helpText =
                        "The Pub/Sub input topic to write to. The name must be in the format `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.",
                example = "projects/your-project-id/topics/your-topic-name")
        @Validation.Required
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> value);
    }