**1. Project Description**

In this project, you will install and experiment with a distributed database that supports tunable consistency. You may choose MongoDB, Apache Cassandra, ScyllaDB, or another suitable database system. The database may be deployed on several physical or virtual machines, or as multiple Docker containers on one machine.

The project should investigate how different database configurations affect the client-centric consistency observed by applications. The relevant models are

- read-your-writes consistency;
- monotonic-reads consistency;
- monotonic-writes consistency; and
- writes-follow-reads consistency.

**2. Requirements**

This is a group project. Each group should contain no more than three people. Detailed requirements are:

- Set up a replicated distributed database.
- Explore the consistency configurations offered by the chosen database.
- Based on your understanding of the database configurations, predict whether the client-centric consistency models listed above will hold under the configurations that you plan to test.
- Design and carry out your own experiments to test your predictions.
- Conduct the experiments under several scenarios, such as normal operation, the failure of one or more database nodes, and a network partition.

**3. Submission**

Submit a PDF report describing the complete project. The report should:

- describe the chosen database, deployment architecture, software versions, and installation procedure;
- describe the consistency configurations explored, their relevant parameters, and their expected behaviour;
- present the design and rationale for the experiments;
- report and explain the results;
- discuss whether the observations agree with your expectations and identify limitations of the experiments; and
- cite the database documentation and any other sources used (including AI usage).