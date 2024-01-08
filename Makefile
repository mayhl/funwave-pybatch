
PYTHON=python3
PIP=pip
PKG_NAME='directorybatching'
VENV=.venv
POETRY=~/.local/bin/poetry

install_poetry: 
	curl -sSL https://install.python-poetry.org | ${PYTHON} -

venv:
	test -d ${VENV} || ${PYTHON} -m venv ${VENV}
	@echo "Use 'source ${VENV}/bin/activate' to activate virtual environment"

install_testpypi:
	${PYTHON} -m ${PIP} install --index-url https://test.pypi.org/simple/ ${PKG_NAME}

uninstall:
	${PIP} uninstall --yes ${PKG_NAME}

install: uninstall
	${PIP} install .

publish:
	${POETRY} --build --skip-existing publish
	conda-build meta.yaml
	
dry_publish:
	${POETRY} --dry-run --build ---skip-existing publish
	conda-build --check meta.yaml

develop: uninstall
	${PIP} install -e .

.PHONY: docs
docs:
	$(MAKE) -C docs html
	
