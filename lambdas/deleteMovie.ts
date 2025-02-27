import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand } from "@aws-sdk/lib-dynamodb";

// Create DynamoDB Document Client for interacting with DynamoDB
const ddbDocClient = createDDbDocClient();

// Lambda function handler for deleting a movie by its ID
export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));

    // Extract movie ID from the path parameters
    const movieId = event.pathParameters?.movieId; // Use correct parameter name (movieId instead of movieID)
    
    if (!movieId) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing movie ID" }),
      };
    }

    // Perform the delete operation on DynamoDB
    const commandOutput = await ddbDocClient.send(
      new DeleteCommand({
        TableName: process.env.TABLE_NAME, // The table name from environment variables
        Key: {
          id: parseInt(movieId), // The key to delete the movie by (movie ID)
        },
      })
    );

    // If the movie was not found or deleted, return a 404 response
    if (!commandOutput) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: `Movie with ID ${movieId} not found` }),
      };
    }

    // Return a success response if movie was successfully deleted
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ message: `Movie with ID ${movieId} deleted successfully` }),
    };
  } catch (error: any) {
    // Log the error and return a 500 error response
    console.error("[ERROR]", JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ message: "Failed to delete the movie", error: error.message }),
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
