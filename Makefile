SHELL:=/bin/bash

# https://crontab.guru/
# run every 5 minutes
CRONINTERVAL:=*/5 * * * *
cron: CRONCMD:=$(CRONINTERVAL) . $(shell echo $$HOME)/.bash_profile; cd $(shell pwd); python qstats.py >> qstats.log 2>&1
cron:
	@echo "$(CRONCMD)"
