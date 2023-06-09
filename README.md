# WikiMedia project

## **Description**

This is a system consisting of several servers, which main idea is to provide an API for the user to get statistics on the data from [WikiMedia](https://stream.wikimedia.org/v2/stream/page-create).

The system is supposed to have two kinds of APIs, let API A and API B.

## **Architecture**

There are several servers processing the requests. The reliability of the system  is ensures by adding a queue of requests and repsonses.
The availability of the system as far as its consistency may be questioned, though (you can experience that while using the API) :)

The detailed design documentation can be found in the Wikimedia System Document file.

## **Data models**

The data is preserved in Cassandra database. The data is saved in a way that would be beneficial for getting faster responses for the API calls.

The detailed data models description can be found in the Wikimedia System Document file.

## **Usage**

Clone the repository on your local machine.

Run __start.sh__ in your terminal.

Check that you can run cqlsh in the cassandra container. If yes - good for you, the program has started. If no - wait for 1-2 minutes and run the __start.sh__ again (the cassandra container should be ready by that time, so running the start.sh file will ensure the cql tables are created properly.)

You can now send the requests for the provided API endpoints. There are two ways for that.

1. On your localhost, port 8000, you can set url as one of the endpoints.
2. You can use requests library in python and send the requests to the endpoint urls through the code.

## **Results**

There supposed to be two APIs implemented, although, due to problems with Spark, there is only one API in real life.

You can see the ideal image of how system would look like in the Wikimedia System Document file.


