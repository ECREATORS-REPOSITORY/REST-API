package com.amazonaws.lambda.demo;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
	static DynamoDB dynamoDB = new DynamoDB(client);

	public String handleRequest(Object input, Context context) {
		context.getLogger().log("Input: " + input);
		//loadSampleProducts("ecreators_reward_app_user");
		// TODO: implement your handler
		try {
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			String inputJson = ow.writeValueAsString(input);
			loadFromFile(inputJson);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "Hello from Lambda!";
	}
	private void loadFromFile(String inputJson) throws  Exception {
		/*Table table = dynamoDB.createTable("Movies",
				Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition
						// key
						new KeySchemaElement("title", KeyType.RANGE)), // Sort key
				Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
						new AttributeDefinition("title", ScalarAttributeType.S)),
				new ProvisionedThroughput(10L, 10L));
		table.waitForActive();*/
		JsonFactory f = new JsonFactory();
		JsonParser jp = f.createJsonParser(inputJson);
		// advance stream to START_ARRAY first:
		//jp.nextToken();

		System.out.println("inside load");
		Table table2 = dynamoDB.getTable("ecreators_reward_app_user");

		//JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));

		JsonNode rootNode = new ObjectMapper().readTree(jp);
		Iterator<JsonNode> iter = rootNode.iterator();

		ObjectNode currentNode;

		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();
			System.out.println(currentNode);
			String category = currentNode.path("category").asText();
			String userId = currentNode.path("userID").asText();
			long rewards=currentNode.path("rewards").asLong();

			try {
				table2.putItem(new Item().withPrimaryKey("category",category, "userId", userId).withNumber("rewards",rewards));
				//System.out.println("PutItem succeeded: " + year + " " + title); }
			}
			catch (Exception e) {
				//System.err.println("Unable to add movie: " + year + " " + title);
				System.err.println(e.getMessage());
				break;
			}
			System.out.println("ITS AWESOME--->");
			jp.close();
		}
	}
}