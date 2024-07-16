//package com.cardinalhealth.pdi.dataflow.service;
//
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
//@RunWith(MockitoJUnitRunner.class)
//public class TextToPubSubServiceTest {
//    @Rule
//    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);
//
//    @Rule
//   public final transient TestPipeline pipeline = TestPipeline.create();
//    private final CloudStorageHelper mockCloudStorageHelper = Mockito.mock(CloudStorageHelper.class,
//            Mockito.withSettings().serializable());
//    @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
//    @Before
//    public void setup() throws IOException {
//
//        Path firstPath = tmpFolder.newFile("first.csv").toPath();
//        int firstSize = 37;
//        Files.write(firstPath, new byte[firstSize]);
//    }
//
//    @Test
//    public void shouldPrefixFileContentWithMetadata() throws Exception {
//
//        List<String> expectedElements = Arrays.asList("hash1-line1", "hash1-line2");
//        // Happy Path: Valid file with hash and content
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenReturn(expectedElements);
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply(into(strings()).via((List<String> lines) -> lines));
//
//
//        PAssert.that(result).containsInAnyOrder(expectedElements);
//        pipeline.run().waitUntilFinish();
//    }
//
//    @Test
//    public void shouldNotPrefixFileContentAndReturnEmptyContent_When_PdiException() throws Exception {
//        // Error while splitting the file
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenThrow(new PdiException("GCS Blob is empty/null for URI"));
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply(into(strings()).via((List<String> lines) -> lines));
//
//        PAssert.that(result).empty();
//        pipeline.run().waitUntilFinish();
//    }
//
//    @Test
//    public void shouldNotPrefixFileContent_When_FileHashNotAttachedToMetadata() throws Exception {
//        // File hash not attached to metadata
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenThrow(new PdiException("GCS Blob Metadata is empty/null for URI"));
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply(into(strings()).via((List<String> lines) -> lines));
//
//        PAssert.that(result).empty();
//        pipeline.run().waitUntilFinish();
//    }
//
//    @Test
//    public void testEmptyFile() throws Exception {
//        // Empty file
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenReturn(Collections.emptyList());
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply(into(strings()).via((List<String> lines) -> lines));
//
//        PAssert.that(result).empty();
//        pipeline.run().waitUntilFinish();
//    }
//
//    @Test
//    public void testFileNotAbleToBeRead() throws Exception {
//        // File not able to be read
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenThrow(new StorageException(403,"exception"));
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply("Flatten", into(strings()).via((List<String> lines) -> lines));
//
//        PAssert.that(result).empty();
//        pipeline.run().waitUntilFinish();
//    }
//
//    @Test
//    public void testFileNotInFolder() throws Exception {
//        // File not in folder
//        lenient().when(mockCloudStorageHelper.readContentAndPrefixEachLineWithFileHash(anyString()))
//                .thenThrow(new StorageException(404,"exception"));
//
//        PCollection<String> result = pipeline
//                .apply(FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath()+"/*"))
//                .apply(FileIO.readMatches())
//                .apply(ParDo.of(new PrefixLineWithFileHashFn(mockCloudStorageHelper)))
//                .apply(into(strings()).via((List<String> lines) -> lines));
//
//        PAssert.that(result).empty();
//        pipeline.run().waitUntilFinish();
//    }
//}
