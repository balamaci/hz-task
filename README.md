hz-task
=======

Framework for easy distributed task processing with the Hazelcast framework.

### About Hazelcast

Hazelcast framework makes sharing data between separate machines/processes very easy. I mean, if your machines are on the same lan, by using multicast hazelcast instances can discover themselves and setup a cluster with zero configuration.

It's very easy to setup a Hazelcast cluster and distribute a Map of objects between the cluster instances. As a sidenote with the addition of cluster nodes, a hazelcast distributed Map can also be **sharded**(split) among cluster nodes to reduce the memory consumption(making it posible to store large quantities of data, on machines that don't have a high amount of memory), and also backed up by making copies which makes the information resilient so even in case of some nodes of the cluster go down, there will be no information loss. 

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
 - Q: How is it different than a Messaging solution? 
    I mean we could also implement where 
   A: 

 - Q: How is it different than a Messaging solution? 
