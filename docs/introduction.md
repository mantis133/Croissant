# What is Croissant

Croissant is a framework for bringing Android activity structure to Rust. 



# Features


## Activities
Activities are a combination of a `struct` as state and a collection of life-cycle methods that are registered with the global Application `struct`

Croissant comes with a few builtin methods 
- on_create: called once on application start-up 
- on_resume: Called when the activity comes into the foreground. This includes the first activity that is run.
- on_pause: Called when an activity is removed from view, via navigation or application exit
- on_destroy: called once per activity on 

## Custom Events


## Background Tasks



## Global State

