# Streams Infrastructure

## Requirements

* Python 2
* Ansible 2.1.1.0 or later
* boto
* AWS credentials
* AWS permissions to create and update stacks

## Getting Started

Ensure you have all the dependencies by running `make deps` in
this directory.  If you make any changes to the code, run a quick
`make test` and Ansible will perform a syntax/sanity check of
the playbook.  While it's not a full test (because it would require
creating a new stack) it is simple.

## Managing the Stack

### CLI

```
ansible-playbook -i inventory/localhost \
                 -e 'stack_env=development aws_region=us-west-1' \
                 create-stack.yml
```

### Jenkins

A job exists to manage the stack:

https://ci.unbounce.com/view/platform/job/event-receiver-streams-manage-stack/

Since environments are commonly located in a specific region, more Jenkins jobs were created that simply wrap the generic job above and provide it with preset environment/region values.

Integration: https://ci.unbounce.com/view/platform/job/event-receiver-streams-manage-integration/

Production: https://ci.unbounce.com/view/platform/job/event-receiver-streams-manage-production/

