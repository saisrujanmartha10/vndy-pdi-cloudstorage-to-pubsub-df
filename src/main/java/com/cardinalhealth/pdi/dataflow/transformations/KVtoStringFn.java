package com.cardinalhealth.pdi.dataflow.transformations;

import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KVtoStringFn extends DoFn<KV<String,String>, String>
{

    private static final Logger logger = LoggerFactory.getLogger(KVtoStringFn.class);

    private CloudStorageHelper helper;


    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        if(context.element()!=null)
        {
            context.output(context.element().toString());
        }
    }
}
