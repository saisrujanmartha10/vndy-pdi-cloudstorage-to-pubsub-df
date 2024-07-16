//package com.cardinalhealth.pdi.dataflow.helper;
//
//import com.cardinalhealth.pdi.dataflow.constants.AppConstants;
//import com.cardinalhealth.pdi.dataflow.exception.PdiException;
//import com.cardinalhealth.pdi.dataflow.helper.CloudStorageHelper;
//import com.google.api.Page;
//import com.google.cloud.ReadChannel;
//import com.google.cloud.ServiceOptions;
//import com.google.cloud.storage.*;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import java.io.BufferedReader;
//import java.io.ByteArrayInputStream;
//import java.io.InputStreamReader;
//import java.nio.channels.Channels;
//import java.util.*;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.*;
//
//public class CloudStorageHelperTest {
//
//    @Mock
//    private Storage mockStorage;
//
//    @Mock
//    private Blob mockBlob;
//
//    private CloudStorageHelper cloudStorageHelper;
//
//    @Before
//    public void setup() {
//        MockitoAnnotations.initMocks(this);
//        cloudStorageHelper = spy(new CloudStorageHelper());
//        doReturn(mockStorage).when(cloudStorageHelper).getStorage();
//    }
//
//
//
//    @Test
//    public void testReadContentAndPrefixEachLineWithFileHash_Success() throws Exception {
//        // Mock data
//        String gcsBucketBlobURI = "gs://mock-bucket/mock-file.txt";
//        String bucketName = "mock-bucket";
//        String blobName = "mock-file.txt";
//        String fileHash = "mock-hash";
//        String content = "line1\nline2";
//
//        // Mock Blob and metadata
//        Map<String, String> metadata = Collections.singletonMap(AppConstants.FILE_HASH_KEY, fileHash);
//        when(mockBlob.getMetadata()).thenReturn(metadata);
//        when(mockBlob.exists()).thenReturn(true);
//        when(mockBlob.getSize()).thenReturn((long) content.getBytes().length);
//
//        // Mock Storage.get() behavior
//        when(mockStorage.get(eq(BlobId.of(bucketName, blobName)))).thenReturn(mockBlob);
//
//        // Mock Blob.reader() behavior
//        ReadChannel mockReadChannel = mock(ReadChannel.class);
//        when(mockBlob.reader()).thenReturn(mockReadChannel);
//
//        // Mock BufferedReader behavior
//        BufferedReader mockBufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content.getBytes())));
//        when(Channels.newReader(eq(mockReadChannel), eq("UTF-8"))).thenReturn(mockBufferedReader);
//
//        // Call the method under test
//        List<String> result = cloudStorageHelper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
//
//        // Verify the behavior
//        assertEquals(Arrays.asList("mock-hash-line1", "mock-hash-line2"), result);
//        verify(mockStorage, times(1)).get(eq(BlobId.of(bucketName, blobName)));
//        verify(mockBlob, times(1)).reader();
//        verify(mockBufferedReader, times(2)).readLine();
//    }
//
//    @Test(expected = PdiException.class)
//    public void testReadContentAndPrefixEachLineWithFileHash_NoFileHashMetadata() {
//        // Mock data
//        String gcsBucketBlobURI = "gs://mock-bucket/mock-file.txt";
//        String bucketName = "mock-bucket";
//        String blobName = "mock-file.txt";
//
//        // Mock Blob and metadata with no FILE_HASH_KEY
//        Map<String, String> metadata = Collections.emptyMap();
//        when(mockBlob.getMetadata()).thenReturn(metadata);
//        when(mockBlob.exists()).thenReturn(true);
//
//        // Mock Storage.get() behavior
//        when(mockStorage.get(eq(BlobId.of(bucketName, blobName)))).thenReturn(mockBlob);
//
//        // Call the method under test
//        cloudStorageHelper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
//    }
//
//    @Test(expected = PdiException.class)
//    public void testReadContentAndPrefixEachLineWithFileHash_NullBlob() {
//        // Mock data
//        String gcsBucketBlobURI = "gs://mock-bucket/mock-file.txt";
//        String bucketName = "mock-bucket";
//        String blobName = "mock-file.txt";
//
//        // Mock Storage.get() behavior to return null
//        when(mockStorage.get(eq(BlobId.of(bucketName, blobName)))).thenReturn(null);
//
//        // Call the method under test
//        cloudStorageHelper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
//    }
//
//    @Test(expected = PdiException.class)
//    public void testReadContentAndPrefixEachLineWithFileHash_EmptyBlob() {
//        // Mock data
//        String gcsBucketBlobURI = "gs://mock-bucket/mock-file.txt";
//        String bucketName = "mock-bucket";
//        String blobName = "mock-file.txt";
//
//        // Mock Blob with size 0
//        when(mockBlob.exists()).thenReturn(true);
//        when(mockBlob.getSize()).thenReturn(0L);
//
//        // Mock Storage.get() behavior
//        when(mockStorage.get(eq(BlobId.of(bucketName, blobName)))).thenReturn(mockBlob);
//
//        // Call the method under test
//        cloudStorageHelper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testReadContentAndPrefixEachLineWithFileHash_StorageException() {
//        // Mock data
//        String gcsBucketBlobURI = "gs://mock-bucket/mock-file.txt";
//        String bucketName = "mock-bucket";
//        String blobName = "mock-file.txt";
//
//        // Mock Storage.get() behavior to throw StorageException
//        when(mockStorage.get(eq(BlobId.of(bucketName, blobName)))).thenThrow(StorageException.class);
//
//        // Call the method under test
//        cloudStorageHelper.readContentAndPrefixEachLineWithFileHash(gcsBucketBlobURI);
//    }
//
//    private Blob createMockBlob(String blobName) {
//        Blob mockBlob = mock(Blob.class);
//        when(mockBlob.getName()).thenReturn(blobName);
//        return mockBlob;
//    }
//}