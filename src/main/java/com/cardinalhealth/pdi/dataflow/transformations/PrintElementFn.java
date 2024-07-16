package com.cardinalhealth.pdi.dataflow.transformations;

import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class PrintElementFn extends DoFn<Object, Void>
{

    private static final Logger logger = LoggerFactory.getLogger(PrintElementFn.class);

    private CloudStorageHelper helper;


    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        logger.info(context.element().toString());
    }
}
