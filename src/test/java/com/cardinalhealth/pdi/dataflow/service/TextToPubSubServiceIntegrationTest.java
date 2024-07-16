//package com.cardinalhealth.pdi.dataflow.service;//package com.cardinalhealth.pdi.dataflow.service;
//
//import com.cardinalhealth.pdi.dataflow.app.TextToPubSubApplication;
//import com.cardinalhealth.pdi.dataflow.exception.PdiException;
//import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
//import com.cardinalhealth.pdi.dataflow.transformations.PrefixLineWithFileHashFn;
//import com.google.cloud.storage.StorageException;
//import org.apache.beam.sdk.io.FileIO;
//import org.apache.beam.sdk.testing.PAssert;
//import org.apache.beam.sdk.testing.TestPipeline;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.PCollection;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import org.junit.runner.RunWith;
//import org.mockito.Mockito;
//import org.mockito.junit.MockitoJUnit;
//import org.mockito.junit.MockitoJUnitRunner;
//import org.mockito.junit.MockitoRule;
//import org.mockito.quality.Strictness;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//
//import static org.apache.beam.sdk.transforms.FlatMapElements.into;
//import static org.apache.beam.sdk.values.TypeDescriptors.strings;
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.lenient;
//
//
//public class TextToPubSubServiceIntegrationTest {
//
//
//
//
//    @Test
//    public void shouldPrefixFileContentWithMetadata() throws Exception {
//
////        String[] args = "--inputBucketPath=gs://filehash/input/*\n" +
////                "--outputTopic=projects/pharmacy-data-np-cah/topics/filehashtest\n" +
////                "--runner=DirectRunner"
////        TextToPubSubApplication.main(args);
//
//
//    }
//
//    @Test
//    public void shouldNotPrefixFileContentAndReturnEmptyContent_When_PdiException() throws Exception {
//            }
//
//    @Test
//    public void shouldNotPrefixFileContent_When_FileHashNotAttachedToMetadata() throws Exception {
//
//    }
//
//    @Test
//    public void testEmptyFile() throws Exception {
//
//    }
//
//    @Test
//    public void testFileNotAbleToBeRead() throws Exception {
//           }
//
//    @Test
//    public void testFileNotInFolder() throws Exception {
//
//    }
//}
