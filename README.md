hz-task
=======

Framework for easy distributed task processing with the Hazelcast framework.

Hazelcast framework makes sharing data between separate machines very easy. 
It's very easy to setup a cluster and distribute a Map of objects between the cluster instances.

I wanted to extend this to share units .

A **Task** is a Java object that represents a unit of work for an Agent. 
A **Task holds data**, that the **Agent** active on different machines knows how to handle.   

The **Master** assigns the **Task**s to the active **Agents**, by an implementation of a **RoutingStrategy**
   - This is done, because the **Master** can make better decisions where the next task should run for example based on the workload or success rate .  
 
To keep things simple we talk about a Master and Agents. 
Whenever you want to submit Tasks for processing you talk to the Master.
The job of the Master .

How is it different than a Messaging solution? 

