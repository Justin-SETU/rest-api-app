import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

// Initialize AJV instance and compile the Movie schema
const ajv = new Ajv();
const isValidBodyParams = ajv.compile(schema.definitions["Movie"] || {});

// Create DynamoDB Document Client for interacting with DynamoDB
const ddbDocClient = createDDbDocClient();

// Lambda function handler for adding a movie
export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    // Log the incoming event for debugging purposes
    console.log("[EVENT]", JSON.stringify(event));

    // Parse the request body if available
    const body = event.body ? JSON.parse(event.body) : undefined;

    // Return an error response if body is missing
    if (!body) {
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing request body" }),
      };
    }

    // Validate the body parameters against the Movie schema
    if (!isValidBodyParams(body)) {
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          message: `Incorrect type. Must match the Movie schema`,
          schema: schema.definitions["Movie"],
        }),
      };
    }

    // Send the item to DynamoDB using PutCommand
    const commandOutput = await ddbDocClient.send(
      new PutCommand({
        TableName: process.env.TABLE_NAME,  // Table name from environment variables
        Item: body,  // The movie data to insert
      })
    );

    // Return success response if movie is added successfully
    return {
      statusCode: 201,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ message: "Movie added" }),
    };
  } catch (error: any) {
    // Log the error and return a 500 error response
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

// Helper function to create a DynamoDB Document Client
function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });

  // Configuration for marshalling and unmarshalling DynamoDB data
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };

  // Return DynamoDB Document Client with configured options
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
