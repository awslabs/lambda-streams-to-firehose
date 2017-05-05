.DEFAULT_GOAL := all
.PHONY: all help

ANSIBLE_BIN := ansible-playbook
JQ_BIN := jq

all: help

help: ## Displays this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

deps: ## Ensures required dependencies are met
ifndef AWS_ACCESS_KEY_ID
	$(error "AWS credentials are required")
endif
ifndef AWS_SECRET_ACCESS_KEY
	$(error "AWS credentials are required")
endif
	@[[ $(shell python --version 2>&1 | cut -f2 -d' ' | cut -c1) -eq 2 ]] || (echo "Python 2 is required."; exit 1)
	@pip install -q -r ./requirements.txt
	@hash $(ANSIBLE_BIN) > /dev/null 2>&1 || \
		(echo "Install Ansible to continue."; exit 1)
	@hash $(JQ_BIN) > /dev/null 2>&1 || \
		(echo "Install JQ to continue."; exit 1)

test: deps ## Performs a syntax check of the ansible playbook
	ansible-playbook --check -i inventory/localhost -e 'stack_env=development aws_region=us-west-2' create-stack.yml
	jq . ./files/stack-templates/*.json > /dev/null
	jq . ./files/stack-policies/*.json > /dev/null

