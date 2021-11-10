package developery.dev.azurestorageaccount.service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import com.azure.storage.queue.models.PeekedMessageItem;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.SendMessageResult;

import developery.dev.azurestorageaccount.common.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service("serviceB.blobService")
public class AzureStorageBlobService {
	
	final String END_POINT = "https://yerpark.blob.core.windows.net/";
	final String QUEUE_END_POINT = END_POINT + "/demo";
	final String containerName = "yericon";
	String accessKey = "DefaultEndpointsProtocol=https;AccountName=yerpark;AccountKey=8gJ9KYgZ2VUgInL5zntAqc2bE1IKZcp4TIwnz6LDWAXlfBLibzlUTgIACemYwS2pMwpHgXLq+YayO8Yq4gbkuQ==;EndpointSuffix=core.windows.net";
	String SAS_TOKEN= "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2021-11-10T18:34:38Z&st=2021-11-10T10:34:38Z&spr=https&sig=BsLTu%2BziYb%2FQwkXM2vOym4Dj%2FQnez9KXJwbPQyECMXM%3D";

	int uploadBlobByAccessKeyCredential(List<QueueMessageItem> item) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                                                        .endpoint(QUEUE_END_POINT)
                                                        .connectionString(accessKey)
                                                        .buildClient();
        return uploadBlob(blobServiceClient, item);
    }

    int uploadBlobBySasCredential(List<QueueMessageItem> item) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                                                        .endpoint(END_POINT)
                                                        .sasToken(SAS_TOKEN)
                                                        .buildClient();
        return uploadBlob(blobServiceClient, item);
    }

    private int uploadBlob(BlobServiceClient blobServiceClient, List<QueueMessageItem> item) {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        
        item.stream().forEach((msg) -> {
            BlobClient blobClient = containerClient.getBlobClient(msg.getMessageId());

            //log.info("msg body :"  + msg.getBody());
            //log.info("msg toString :"  + msg);

            blobClient.upload(msg.getBody());
        });
        return item.size();
    }
    
	private String readBlob(BlobServiceClient blobServiceClient, String containerName, String blobName) throws IOException {
		BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
		
		BlobClient blobClient = containerClient.getBlobClient(blobName);
		
		CommonUtils.deleteIfExists("test.txt");
		
		BlobProperties pro = blobClient.downloadToFile("test.txt");
		
		////log.info("blobSize: " + pro.getBlobSize());
		String textInFile = CommonUtils.readStringFromFile(blobName);
		
		return textInFile;
	}

	public String readBlobByAccessKeyCredential(String containerName, String blobName) throws IOException {
		
			
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
			    .endpoint(END_POINT)
			    .connectionString(accessKey)
			    .buildClient();
		
		return readBlob(blobServiceClient, containerName, blobName);		
	}

}
