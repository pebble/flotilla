# Flotilla - The Petit Fleet

[![Build Status](https://travis-ci.org/pebble/flotilla.svg?branch=master)](https://travis-ci.org/pebble/flotilla)

Flotilla is an alternative to [fleet](https://github.com/coreos/fleet) that uncouples from CoreOS/etcd and couples to AWS.

Flotilla uses:

* DynamoDb - for persistence, locking and communication
* ELB - to reroute traffic during an upgrade
* IAM - for access control
* KMS - for encryption of environment variables
* CloudFormation - for provisioning resources
* SQS - for communication between components

Like fleet, flotilla distributes [systemd units](http://www.freedesktop.org/software/systemd/man/systemd.unit.html) across a cluster of machines.

Unlike fleet, flotilla manages environment variables for scheduled services, and supports weighted distributions of services (for canaries, A/B, load balancing, etc).

## Scheduling

Flotilla introduces the concept of a service to distinguish worker instances. An instance is born into a service and advertises membership in DynamoDb.

A scheduler running in the cluster periodically checks for:

* Which units+configurations are defined for each service? What are their weights?
* How many instances are currently being used for this service?

Given these inputs, the scheduler updates assignments for instances to satsify the current configuration.

Currently there is a single scheduler for the entire cluster. DynamoDb parallel scans are the intended scaling path for multiple schedulers.

## Deployment

Flotilla workers periodically check for assignments in DynamoDb. If a worker's assignment is changed, it executes the following steps:

1. Acquire deployment lock for service.
1. If ELB is defined:
   1. Request deregistration from ELB.
   1. Wait for ELB deregistration (i.e. drain connections)
1. Stop and unload all existing flotilla units.
1. Start new flotilla units.
1. If ELB is defined:
	1. Request registration to ELB.
	1. Wait for ELB registration (i.e. health check).
1. Release deployment lock for service.



## Limitations

Don't use this in production. This implementation is output from 3x8h "hackathon"s.
