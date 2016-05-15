hz-task
=======

Open source framework(Apache license) for easy distributed task processing built upon the [Hazelcast](https://github.com/hazelcast/hazelcast/) framework.
Just drop the **hz-task** dependencies and start defining the tasks for processing.

### Intro

Hazelcast is a java framework that makes sharing data structures between separate machines/processes very easy.
  
For ex. if we would have a very big Map with millions of entries that would not fit in memory on a single VM,
Hazelcast is able to **shard**(split) the map between multiple cluster nodes to reduce the memory footprint per node(making 
it possible to store large quantities of data, on machines that don't have a high amount of memory), and also backed 
up by making copies which makes the information resilient so even in case of some nodes of the cluster go down,
there will be no information loss. 

This is very useful in today's world where we can cheaply start many instances/containers with low memory.

Hazelcast also offers the possibility to easily build a cluster by specifying where it's members are located
(or autodiscovery if network allows multicast) also a hearbeat mechanism detect when a cluster member leaves.

It's only natural that we try and build a distributed task processing framework on top of it.

### What you need to do to get going

We'll start by trying to solve a realcase problem - implementing a web crawler-.
This is a very time consuming process to do on a single machine because of the latency of the response from the hosts 
or to keep too many connections active to the sites. 

It would be better to have the tasks distributed to many nodes in a cluster and have a master node to which
you submit the list of sites to crawl and let it handle the results.

#### 1. Task - the data holder  

First we define a **Task** class to supply the information for an **Agent** he need to be able to process his task. 
For our web crawler it will need to hold the url of the webpage to be parsed.
 
 ```java
 public class GetWebPageTask extends Task {

     private final String pageUrl;

     public GetWebPageTask(String pageUrl) {
         this.pageUrl = pageUrl;
     }

     public String getPageUrl() {
         return pageUrl;
     }
 }
 ```

#### 2. TaskProcessor - the processor of the Task - deployed on Agents
Implement a **TaskProcessor** this will be used by the agents to do the actual work like retrieving the web page
It contains actual logic on how to process the tasks.

 ```java
 @Component
 public class WebPageRequestTaskProcessor implements TaskProcessor<String> {
    @Inject
    private HttpClient httpClient;

    @Override
    public String process(Task task) {
        GetWebPageTask getWebPageTask = (GetWebPageTask) task;

        HttpGet httpGet = new HttpGet(getWebPageTask.getPageUrl());

        HttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            String responseString = IOUtils.toString(response.getEntity().getContent());

            return responseString;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
 ```
 

 #### 3. Package in jar for deployment on Agents 
 Package the TaskProcessors classes in a jar file along with the **hz-task-agent** dependency and deploy it on as many machines as you want to make up your agents.
 
 Now just start the Agent:
 
 ```java
 public class StartTestClient {

    public static void main(String[] args) {
        AgentConfig agentConfig = new AgentConfig();

        //we register the task processor for GetWebPageTask to be provided through Spring
        //that way our WebPageRequestTaskProcessor         
        agentConfig.registerTaskProcessorFactory(GetWebPageTask.class,
                     new SpringBeanFactory(context, WebPageRequestTaskProcessor));

        new ClusterAgent(agentConfig, hazelcastConfig);
    }
 }
```
 
#### 4. TaskCompletionHandler - handles the result of the processing - to be deployed on Master
We define a **TaskCompletionHandler** on the single **Master** node to handle the result of the processed tasks.

```java
public interface TaskCompletionHandler<T extends Task> {

    public void onSuccess(T task, Object taskResult, String agentName);

    public void onFail(T task, Throwable throwable, String agentName);

}
```

In our crawler example to persist the html to storage, or just output to console like below.


### Concept

A **Task** is a Java object that represents a unit of work for an Agent. 
A **Task holds data**, that the **Agents** active on cluster instances knows how to handle.   

The **Master** assigns the **Task**s to the active **Agents**, by an implementation of a **RoutingStrategy**
   - This is done, because the **Master** can make better decisions where the next task should run for example based on the workload or success rate of the finished tasks on **Agents**. 
   For example if an Agent starts reporting an increasing number of failed tasks, the **Master** **can reassign the failed tasks and future tasks of that type to another** **Agent**.(Imagine the scenario where Agents perform site crawling and a particular machine/**Agent** is banned from the site - the **Master** will schedule all the failed tasks and future crawling tasks for that site on another **Agent**.  

   - If an **Agent** leaves the cluster, the **Master** can reassign the dead **Agent**'s tasks to other available **Agent**s. 
   - If no active **Agent**s have registered with the **Master**, the tasks are kept with the Master until an agent becomes available. 

### FAQs
 
   - Q: How is it different than a **PubSub** solution through an MQ server implementation like **RabbitMQ**?
     A: 
      1. You don't need an external dependency, it's all java, all you need to do is import the library in your project.
      2. Master knows where each task is executing and make better decisions on where to retry the task if certain nodes begin experiencing failures for certain types of tasks or if an agent is dropped from the cluster his work can be reassigned.
      3. Master can also make better decisions based on how "loaded" are all the agents in the cluster. It can also reassign pending tasks to a new member of the cluster.
      
      But for sure with some extra work you could build a similar solution on top of a MQ solution. 

   - Q: Does Hazelcast not have already something related to running tasks on remote nodes?
     A: It does, you just need to look at **IExecutorService .executeOnMember** however we chose to **focus on passing the data for the computation**, **not the computation itself**, because that would limit you to what you can do - imagine passing a computation that would need an http connection to retrieve a web page-. 
     By passing enough data for your computations on the agents you can have the libraries and frameworks of your choice on the that help solve complex scenarios.


### What we should improve

   - Currently no task stealing options. If a new member joins the cluster the master will not distribute a large queue
of tasks that was assigned to an agent. 

   - No multiple masters to standby available.