package developery.dev.azurestorageaccount;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AzureStorageAccountApplication {

	public static void main(String[] args) {
		SpringApplication.run(AzureStorageAccountApplication.class, args);
	}

}
