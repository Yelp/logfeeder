.DELETE_ON_ERROR:

whoami    := $(shell whoami)
cur_dir   := $(shell pwd)

all:
	echo >&2 "Must specify target."

test:
	tox

venv:
	tox -evenv

clean:
	rm -rf build/ dist/ .tox/ venv-*/ *.egg-info/
	rm -f .coverage
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

clean_logs:
	rm -rf log_files/*

.PHONY: all test install-hooks clean
