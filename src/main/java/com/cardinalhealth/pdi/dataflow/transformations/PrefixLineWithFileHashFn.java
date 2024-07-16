package com.cardinalhealth.pdi.dataflow.transformations;

import com.cardinalhealth.pdi.dataflow.exception.PdiException;
import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PrefixLineWithFileHashFn extends DoFn<FileIO.ReadableFile, List<String>>
{

    private static final Logger logger = LoggerFactory.getLogger(PrefixLineWithFileHashFn.class);

    private  CloudStorageHelper helper;

    private TupleTag<KV<String,String>> failedGcBucketURIs= null;

    public PrefixLineWithFileHashFn(CloudStorageHelper helper,
                                    TupleTag<KV<String,String>> failedGcBucketURIs) {
        this.helper = helper;
        this.failedGcBucketURIs = failedGcBucketURIs;
    }


    @ProcessElement
    public void processElement(ProcessContext context) {
        List<String> fileHashPrefixedContent = new ArrayList<>();
        FileIO.ReadableFile gcsBucketFile = context.element();
        if(gcsBucketFile==null)
        {
            logger.error("GCS BucketFile is null");
            context.output(failedGcBucketURIs,KV.of(gcsBucketFile.toString(),"GCS BucketFile is null"));
        }
        else {
            String gcsBucketBlobURI = gcsBucketFile.getMetadata().resourceId().toString();
            logger.info("Processing GCS Blob element: {}", gcsBucketBlobURI);
            try {
                fileHashPrefixedContent = helper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
                logger.info("FileHash prefix completed for GCS Blob URI : {}", gcsBucketBlobURI);
                context.output(fileHashPrefixedContent);
            }
            catch (Exception e)
            {
                logger.error("Error occured while processing element : {} ",e.getMessage());
                context.output(failedGcBucketURIs,KV.of(gcsBucketBlobURI,e.getMessage()));
            }
        }

    }


}
