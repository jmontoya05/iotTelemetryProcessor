package org.cys.iot.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;

import java.util.Map;

public class SyncMain {
    private final String HOST = System.getenv("HOST_COSMOS");
    private final String MASTER_KEY = System.getenv("MASTER_KEY_COSMOS");
    private CosmosClient client;
    private final String databaseName = "CyS";
    private final String containerName = "Telemetry";
    private CosmosDatabase database;
    private CosmosContainer container;

    public void close() {
        client.close();
    }

    public static void publishDataToCosmos(Map<String, Object> data) {
        SyncMain p = new SyncMain();

        try {
            p.publishData(data);
            System.out.println("Complete");
        } catch (Exception e) {
            System.err.printf("Error message: %s%n", e.getMessage());
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
    }

    private void publishData(Map<String, Object> data) throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + HOST);

        //  Create sync client
        client = new CosmosClientBuilder()
                .endpoint(HOST)
                .key(MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildClient();

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        createItem(data);
    }

    private void createDatabaseIfNotExists() throws Exception {
        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseResponse.getProperties().getId());

        System.out.println("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        //  Create container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/partitionKey");

        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties);
        container = database.getContainer(containerResponse.getProperties().getId());

        System.out.println("Checking container " + container.getId() + " completed!\n");
    }

    private void createItem(Map<String, Object> data) throws Exception {
        //  Create item
        CosmosItemResponse<Map<String, Object>> item = container.createItem(data, new PartitionKey(data.get("partitionKey")), new CosmosItemRequestOptions());

        System.out.println("Created  items");
    }
}
