# challenge
***Description:***

The functional goal of the challenge is to analyse a file with a large
body of text and to count unique words so that the most common and least
common words can be identified.

***Goals Achieved:***

Following goals have been achieved for this challenge

**Mandatory Goals:**

-   Produce the solution using a git repository and demonstrate
    proficiency with using git to develop a software project

-   Create a single program to perform the task, that can be run using
    command line arguments

-   Distributes the workload to run on any number of servers

-   A user should be able to view/query the results (most and least
    common words) of the program

-   Demonstrate an ability to use and understand MongoDB

**Desirable Goals:**

-   The solution should recover gracefully from the failure of a server

-   Notes considering trade offs between efficiency and accuracy of the
    solution

***Assumptions:***

-   File name is unique

-   For the better performance, I am using MappedByteBuffer for loading
    file-block into memory. By using MappedByteBuffer, the accuracy will
    be compromised for boundary words.

-   In mongo parameter, if port is missing or containing invalid number,
    then I am using default port (27017) for mongoDB

-   Setting collection: Default & Configurable parameters are given
    below

    -   File will be processed block by block and Default block size is
        15MB \[BLOCK\_SIZE= 15728640\]

    -   Queue size will be thrice the number of executors
        \[DEFAULT\_QUEUE\_MULTIPLER=3\]

    -   If the memory usage of executor’s JVM reaches 90% then executor
        writes the result in the database.
        \[DEFAULT\_MEMORY\_USED\_PERCENTAGE=90\]

    -   The controller will stop executor if executor does not respond
        for 2 minutes \[WORKER\_MAX\_WAIT\_TIME=122880\]

    -   The controller will stop other executor-controller if it does
        not respond for 2 minutes \[SERVER\_MAX\_WAIT\_TIME=122880\]

-   Removing non-words item during counting process

-   I preferred to use Java driver for MongoDB over Spring-data for
    better performance. I used Spring-data for reporting.

***How to use:***

Following are the Instructions required to successfully run the program
and view the final results

-   Download challenge.jar file from
    “https://github.com/wajidhanif/challenge/downloads”

-   Run the program by giving the following command

    java –Xmx8192m -jar challenge.jar –source dump.xml –mongo
    \[hostname\]:\[port\]

-   When program is done with word count then it will display a message
    on console (File has been processed)

-   Download the report.war file from
    “https://github.com/wajidhanif/challenge/downloads”

-   Deploy report war to tomcat/jetty server

-   Update the connection parameter in “dispatcher-servlet.xml”

    &lt;bean id="mongo"
    class="org.springframework.data.mongodb.core.MongoFactoryBean"&gt;

    &lt;property name="host" value="localhost" /&gt;

    &lt;property name="port" value="27017" /&gt;

    &lt;/bean&gt;

-   Run the tomcat/jetty server

-   Open the browser and go to the following URL

    or localhost&gt;:8081/report/words/list

-   Select the file name from dropdown

-   Provide the number of words you want to see

-   Select Most common/least common in dropdown

-   Click submit to view the result.

***Technologies Used:***

***Challenge***

-   Java 8

-   mongo-java-driver

-   log4j

-   MongoDB

***Report***

-   Java 8

-   Spring MVC 4

-   Spring-data (ORM for MongoDB)

***

***Architecture:***

![alt text](https://github.com/wajidhanif/challenge/downloads/architecture.jpg)


***Executor:***

When the program runs, then executor will start and will perform the
following tasks

-   Registers the executor in database

-   Creates the file and file-blocks object in database, If the source
    parameter exists

-   Marks the executor as candidate for controlling the workload
    distribution, if the source parameter exists

-   Marks executor as worker (getting data from database and process it)
    only, if source parameter is missing

-   Runs the WorkerController and Worker threads.

***Controller:***

The controller is distributing the workload between executors. it will
perform the following functionalities

-   Gets all the available file blocks from database

-   Reads the file-block from file and puts it in the message queue for
    executors to process it, If the message queue is empty or max queue
    size is not reached yet.

-   Calls mongodb mapreduce to calculate the counts, if all blocks have
    been processed by executors.

-   Checks the executor failure: if the executor is not responding for
    default max wait time, then it makes the blocks available for other
    executors which are processed by this executor

***Worker:***

This function is processing file and counting the words. This function
is performing following functionalities

-   Gets file-block data from message queue

-   Calls the assigned CountProcessor (by default word count processor)
    to count words

-   Merges the data with worker hashmap

-   Writes the word counts to database, if all blocks processed or
    memory is full

-   Waits for the controller-executor, if all blocks are not processed
    yet

***MongoDB:***

MongoDB is a common mean for executor to communicate. It contains the
collections and message queue for communication.

***Code Summary***

The challenge application contains the following packages

-   the.floow.challenge.dao: contains DAO for each collection in
    database

-   the.floow.challenge.entity: contains the entity for each collection
    in database

-   the.floow.challenge.enums: contains all the enums

-   the.floow.challenge.excutor: contains executors classes

-   the.floow.challenge.processor: contains the count processors

-   the.floow.challenge.service: contains all service classes for the
    communication with database.

-   the.floow.challenge.util: contains utility functions for the
    program.

***Notes (Efficiency & Accuracy)***

**Efficiency**

-   MappedByteBuffer is used to enhance the performance of the system

-   Java Mongo Driver is used to avoid the overhead of ORM (Spring-data
    or Morphia)

**Accuracy**

-   Using MappedByteBuffer, the boundary words are not catered to by
    program.
