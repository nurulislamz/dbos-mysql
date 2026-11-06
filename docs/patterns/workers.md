# Workers

> Decide which process may run a piece of work, and how that process gets the clients the work needs.

A workflow runs on a process that registered its name. A queue runs on a process that registered the queue. Clients are ordinary Python objects captured by the step functions. There is no worker-side container for activities beyond that.

## Patterns in this section

- [A queue per host](worker-queues.md). Register a queue on one machine when the work has to happen there. A waiting deadline for a machine that never comes back is not supported yet.
- [Pass clients into steps](dependency-injection.md). Build clients once at startup. Steps close over them. Workflow code stays deterministic, and a step can be called in a test with no database.

## Choosing a pattern

The work can run on any copy of the program: register the same workflows and queues everywhere, and do not pin it.

The work must run on a specific machine: give that machine [its own queue](worker-queues.md).

The step needs a database pool, an HTTP client, or a repository: [close over it](dependency-injection.md). Do not construct it inside the workflow.
