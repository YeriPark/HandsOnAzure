package developery.dev.azurestorageaccount.service;

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

import developery.dev.azurestorageaccount.common.CommonUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AzureStorageService {
	
	final String END_POINT = "https://yerpark.blob.core.windows.net/";
	
	
	
	public String readBlobBySasCredential(String containerName, String blobName) throws IOException {
		
		String sasToken = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacuptfx&se=2021-07-18T06:56:24Z&st=2021-07-17T22:56:24Z&spr=https&sig=AkXwxAtdcLQOyulq8iSR3PNt1sDNWfSiEkl6rywQqPA%3D";
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
			    .endpoint(END_POINT)			    
			    .sasToken(sasToken)
			    .buildClient();
		
		return readBlob(blobServiceClient, containerName, blobName);		
	}
	
	public String readBlobByServicePrincipalCredential(String containerName, String blobName) throws IOException {
		
		String clientId = "6b812809-90a5-46d1-9283-0005431df738";
		String clientSecret = "dq0TtsdB4.~ND-55.lBLW~01.jmZ4E8~1P";
		String tenantId = "e940aa1f-bb4d-42cd-a3c8-544d3b1d34bd";
		
		ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
				  .clientId(clientId)
				  .clientSecret(clientSecret)
				  .tenantId(tenantId)
				  .build();
		
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
			    .endpoint(END_POINT)
			    .credential(clientSecretCredential)
			    .buildClient();
		
		return readBlob(blobServiceClient, containerName, blobName);	
		
	}
	
	private String readBlob(BlobServiceClient blobServiceClient, String containerName, String blobName) throws IOException {
		BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
		
		BlobClient blobClient = containerClient.getBlobClient(blobName);
		
		CommonUtils.deleteIfExists(blobName);
		
		BlobProperties pro = blobClient.downloadToFile(blobName);
		
		//log.info("blobSize: " + pro.getBlobSize());
		String textInFile = CommonUtils.readStringFromFile(blobName);
		
		return textInFile;
	}

	
	public String readBlobByAccessKeyCredential(String containerName, String blobName) throws IOException {
		
		String accessKey = "DefaultEndpointsProtocol=https;AccountName=yerpark;AccountKey=8gJ9KYgZ2VUgInL5zntAqc2bE1IKZcp4TIwnz6LDWAXlfBLibzlUTgIACemYwS2pMwpHgXLq+YayO8Yq4gbkuQ==;EndpointSuffix=core.windows.net";
			
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
			    .endpoint(END_POINT)
			    .connectionString(accessKey)
			    .buildClient();
		
		return readBlob(blobServiceClient, containerName, blobName);		
	}
	
}
