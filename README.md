hz-task
=======

Open source framework(Apache license) for easy distributed task processing built upon the [Hazelcast]() framework.

### Intro

Hazelcast framework makes sharing data between separate machines/processes very easy - I mean, zero conf if on the same lan,
you can just start two separate process and they will just make up a cluster. It's also easy to detect when a cluster member
joins or leaves.

It's only natural that we can try to build a distributed task processing framework on top of it.

### What you need to do

 - Define a **Task** class to hold information for an **Agent** to process. If we are building a web crawler for example a ParseWebPageTask
 will need to hold the url of the webpage to be parsed.
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

 - Implement a **TaskProcessor** this will be used by the agents to do the actual work like retrieving the web page
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
 Package this class in a jar file along with the **hz-task-agent** and deploy it on as many machines as you want to make up your agents.
 From here to starting the Agent is no complicated than
 ```java
 public class StartTestClient {

    public static void main(String[] args) {
        AgentConfig agentConfig = new AgentConfig();

        agentConfig.registerTaskProcessorFactory(GetWebPageTask.class,
                     new SpringBeanFactory(context, WebPageRequestTaskProcessor));

        new ClusterAgent(agentConfig, hazelcastConfig);
    }
 }
 ```
 You also need to register the

 - Define a **TaskCompletionHandler** on the single **Master** node to handle the result of the processed task like
 maybe for our crawler example to persist the html to a NoSQL storage, or just output to console like below.

 ```

 That is basically
As a sidenote with the addition of cluster nodes, a hazelcast distributed Map can also be **sharded**(split) among cluster nodes to reduce the memory consumption(making it posible to store large quantities of data, on machines that don't have a high amount of memory), and also backed up by making copies which makes the information resilient so even in case of some nodes of the cluster go down, there will be no information loss.

So one might think we can build a nofuss decent task distribution framework on top of this.

### Concept

A **Task** is a Java object that represents a unit of work for an Agent. 
A **Task holds data**, that the **Agents** active on cluster instances knows how to handle.   

The **Master** assigns the **Task**s to the active **Agents**, by an implementation of a **RoutingStrategy**
   - This is done, because the **Master** can make better decisions where the next task should run for example based on the workload or success rate of the finished tasks on **Agents**. 
   For example if an Agent starts reporting an increasing number of failed tasks, the **Master** **can reassign the failed tasks and future tasks of that type to another** **Agent**.(Imagine the scenario where Agents perform site crawling and a particular machine/**Agent** is banned from the site - the **Master** will schedule all the failed tasks and future crawling tasks for that site on another **Agent**.  

   - If an **Agent** leaves the cluster, the **Master** can reassign the dead **Agent**'s tasks to other available **Agent**s. 
   - If no active **Agent**s have registered with the **Master**, the tasks are kept with the Master until one . 

### Quick setup


### FAQs
 - Q: How is it different than a PubSub solution through an MQ server implementation like RabbitMQ?
   A: 1. You don't need an external dependency, it's all java, all you need to do is import the library in your project.
      2. Master knows where each task is executing and make better decisions on where to retry the task if certain nodes
      begin experiencing failures for certain types of tasks or if an agent is dropped from the cluster his work
      can be reassigned.

      But for sure with some extra work you could build

 - Q: Does Hazelcast not have already ?
