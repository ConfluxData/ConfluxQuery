DOCS_VENV := .venv-docs
DOCS_PYTHON := $(DOCS_VENV)/bin/python

.PHONY: docs-install docs-serve docs-build docs-deploy docs-check

docs-install:
	@test -x $(DOCS_PYTHON) || python3 -m venv $(DOCS_VENV)
	$(DOCS_PYTHON) -m pip install -r requirements-docs.txt

docs-serve: docs-install
	$(DOCS_PYTHON) -m mkdocs serve

docs-build: docs-install
	$(DOCS_PYTHON) -m mkdocs build --strict

docs-check: docs-install
	PATH="$(CURDIR)/$(DOCS_VENV)/bin:$$PATH" bash scripts/check-docs.sh

docs-deploy: docs-install
	$(DOCS_PYTHON) -m mkdocs gh-deploy --strict --force
