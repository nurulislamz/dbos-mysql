# Entity and lifecycle

> One long-lived workflow stands for one business object. Messages change it, and something has to keep its step list from growing without a bound.

## Patterns in this section

- [Entity workflow](entity-workflow.md). One workflow per account, device, or order. Commands arrive by message, and state is published as events.
- [Continue-as-new](continue-as-new.md). Finish the current run and start a new one under the same ID with an empty step list. Not supported yet.
- [Updatable timer](updatable-timer.md). Wait until a deadline that a later message can move earlier or later.

## Choosing a pattern

The object has an identity and a stream of commands: use an [entity workflow](entity-workflow.md), with the entity ID as the workflow ID.

That entity lives long enough that replaying every command gets slow: you want [continue-as-new](continue-as-new.md). Until that exists, start the next run under a new ID.

The entity is waiting on a time that the outside world can change: use an [updatable timer](updatable-timer.md).
