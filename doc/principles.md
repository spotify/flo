# Prime Principles

## 1. Workflow graph expansion mimics a function call tree
Although with one restriction:
> Calls to functions can not use the return value of another call as a parameter, neither directly nor as a derived value.

This translates to:
> Inputs to a Task can not use the return value of another Task as a parameter, neither directly nor as a derived value.


## 2. Workflow graph expansion is decoupled from node evaluation
This is more or less a result of 1.


## 3. Graph nodes have a state
 * Some states are `complete` and carry a value
 * Some states are `incomplete` and do not carry a state
 * There is a state machine for each node, where the some state changes propagate in the graph
