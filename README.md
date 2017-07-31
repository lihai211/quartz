
## About
Contains original Quartz Scheduler code with some functionality provided by Evolveum.

Currently the only feature added is "execution capabilities". It allows execution nodes to specify
string tags denoting their capabilities (e.g. "batch jobs", "large RAM" etc). Individual triggers
can specify required capability, which is then taken into account when acquiring and firing
these triggers.

One node can declare 0 or more provided capabilities. Each individual trigger can specify 0 or 1 required capabilities.

Current version is 2.3.0.e1, which is based on 2.3.0. (It also includes some minor fixes committed after 2.3.0 release.)

Requires Java 1.8 to successfully run the tests, because of SER files checking. But should be compilable on 1.7 as well
(though not checked), with some 10 not important test failures.     

Evolveum-specific development is carried out on "evolveum-master" branch. Original "master" branch is kept
intact, except for this one file. 

## Build instructions:

### To compile:
```
  %> ./mvnw install -DskipTests
```

Note:  the final Quartz jar is found under quartz/target 

### To build Quartz distribution kit:
```
  %> cd distribution
  %> ./mvnw package
```

