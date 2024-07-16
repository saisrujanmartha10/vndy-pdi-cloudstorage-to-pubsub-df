package com.cardinalhealth.pdi.dataflow.helper;


import com.cardinalhealth.pdi.dataflow.constants.AppConstants;
import com.cardinalhealth.pdi.dataflow.exception.PdiException;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.*;

import java.io.BufferedReader;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudStorageHelper implements Serializable  {
    private static final Logger logger = LoggerFactory.getLogger(CloudStorageHelper.class);

    public Storage getStorage() {
        return StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId()).build().getService();
    }

    public List<String> readContentAndPrefixEachLineWithFileHash(String gcsBucketBlobURI){

        String path = gcsBucketBlobURI.split("://")[1];
        String[] split = path.split("/");
        String bucketName = split[0];
        String blobName= Arrays.stream(split).skip(1).collect(Collectors.joining("/"));

        logger.info("GCS Bucket name : {}",bucketName);
        logger.info("GCS Blob name : {}",blobName);

        String fileHash = null;

        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = null;
        try {
            blob = getStorage().get(blobId);
        }
        catch (StorageException e)
        {
            logger.error(e.getMessage());
            throw e;
        }

        if(blob == null || !blob.exists() || blob.getSize() == 0){
            throw new PdiException("GCS Blob is empty/null for URI : ".concat(gcsBucketBlobURI));
        }
        Map<String, String> fileMetadata = blob.getMetadata();
        if (fileMetadata == null || fileMetadata.isEmpty()) {
            throw new PdiException("GCS Blob Metadata is empty/null for URI : ".concat(gcsBucketBlobURI));
        }
        for (Map.Entry<String, String> entry : fileMetadata.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(AppConstants.FILE_HASH_KEY)) {
                fileHash = entry.getValue();
                logger.info("Metadata {} = {} found for Blob URI : {}",
                        AppConstants.FILE_HASH_KEY,
                        fileHash,gcsBucketBlobURI);
                if(fileHash.contains("/")) {
                    fileHash = fileHash.replaceAll("/", "%2F");
                    logger.info("{} after replacing (/) with (%2F) is : {} for Blob URI: {}",
                            AppConstants.FILE_HASH_KEY,
                            fileHash,
                            gcsBucketBlobURI);
                }
            }
        }

        if(fileHash == null) {
            throw new PdiException(String.format("Metadata key : %s not found for Blob URI : %s",
                    AppConstants.FILE_HASH_KEY,gcsBucketBlobURI));
        }

        return readAndPrefixContentWithFileHash(blob,fileHash);
    }

    private List<String> readAndPrefixContentWithFileHash(Blob blob,String fileHash)
    {
        ArrayList<String> list = new ArrayList<>();

        try {
            ReadChannel readChannel = blob.reader();
            BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                list.add(fileHash+"-"+line);
            }
        }
        catch (Exception e)
        {
            logger.error("error occured while PrefixContentWithFileHash");
            throw new PdiException(e.getMessage());
        }
        return list;
    }


}