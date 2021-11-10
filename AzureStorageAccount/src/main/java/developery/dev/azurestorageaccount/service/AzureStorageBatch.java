package developery.dev.azurestorageaccount.service;

import java.util.List;
import java.io.IOException;

import org.springframework.stereotype.Service;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.queue.models.QueueMessageItem;

import developery.dev.azurestorageaccount.common.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class AzureStorageBatch {


    Logger log = LoggerFactory.getLogger(AzureStorageBatch.class);

    @Autowired
    @Qualifier("serviceB.queueService")
    AzureStorageQueueService queueService;

    @Autowired
    @Qualifier("serviceB.blobService")
    AzureStorageBlobService blobService;
    
    @Scheduled(fixedDelay = 5 * 1000L)
    private void run() {
    	push(16);
    	log.info("====batch end====");
    }
    
    public String push(int count) {
    	     
        List<QueueMessageItem> item = queueService.popQueueMessage(count);
        int num = blobService.uploadBlobBySasCredential(item);
        return num + "";
    }

}
