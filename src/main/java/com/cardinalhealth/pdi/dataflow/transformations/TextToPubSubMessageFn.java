package com.cardinalhealth.pdi.dataflow.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextToPubSubMessageFn extends DoFn<String, String>
{

    private static final Logger logger = LoggerFactory.getLogger(TextToPubSubMessageFn.class);

    private final String fileHashToBePrefixed;
    public TextToPubSubMessageFn(String filehash)
    {
         this.fileHashToBePrefixed = filehash;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        logger.info("processing element");
        String line = context.element();
        long lineNumber = context.pane().getIndex();
        if(null == line || line.isEmpty()) {
            logger.warn("file has empty line : {}",lineNumber);
        }

        String messageId = fileHashToBePrefixed+"-"+lineNumber;

        context.output(messageId);
    }
}
