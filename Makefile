# HadoopScope — Makefile
# Comandi di sviluppo standard.
#
# Setup iniziale:
#   make setup        crea il venv e installa le dipendenze dev
#
# Uso quotidiano:
#   make test         esegue tutti i test
#   make dry-run      dry-run con il config di test
#   make caps         mostra capability map
#   make docs         build della documentazione MkDocs
#   make docker-test  integration test con Docker

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------

PYTHON      := python3
VENV_DIR    := .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_PIP    := $(VENV_DIR)/bin/pip

# Usa il venv se esiste, altrimenti Python di sistema
ifeq ($(wildcard $(VENV_DIR)/bin/python),)
  PY := $(PYTHON)
else
  PY := $(VENV_PYTHON)
endif

CONFIG      := config/test.yaml
ENV         := test-hdp

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

.PHONY: help
help:
	@echo "HadoopScope — comandi disponibili:"
	@echo ""
	@echo "  make setup          Crea venv + installa dipendenze dev (PyYAML, pytest)"
	@echo "  make setup-docs     Installa anche MkDocs per la documentazione"
	@echo ""
	@echo "  make test           Esegue tutti i test (unit + config + integration)"
	@echo "  make test-unit      Solo test_base.py e test_config.py"
	@echo "  make test-nopyaml   Test senza PyYAML (verifica zero-deps)"
	@echo ""
	@echo "  make dry-run        Dry-run con config/test.yaml"
	@echo "  make caps           Mostra capability map"
	@echo ""
	@echo "  make docs           Build MkDocs (richiede: make setup-docs)"
	@echo "  make docs-serve     Serve docs in locale su http://localhost:8000"
	@echo ""
	@echo "  make docker-test    Integration test con Docker compose"
	@echo "  make docker-build   Build immagine Docker"
	@echo ""
	@echo "  make lint           Linting con ruff (se installato)"
	@echo "  make clean          Rimuove cache Python e artefatti"
	@echo ""
	@echo "Variabili override:"
	@echo "  make test CONFIG=config/myprod.yaml ENV=prod-hdp"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

.PHONY: setup
setup: $(VENV_DIR)
	$(VENV_PIP) install --quiet --upgrade pip
	$(VENV_PIP) install --quiet pyyaml pytest
	@echo ""
	@echo "Venv pronto in $(VENV_DIR)/"
	@echo "Attiva con: source $(VENV_DIR)/bin/activate"
	@echo "Poi esegui: make test"

$(VENV_DIR):
	$(PYTHON) -m venv $(VENV_DIR)

.PHONY: setup-docs
setup-docs: $(VENV_DIR)
	$(VENV_PIP) install --quiet --upgrade pip
	$(VENV_PIP) install --quiet pyyaml pytest mkdocs mkdocs-material
	@echo "Docs env pronto. Usa: make docs-serve"

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

.PHONY: test
test:
	$(PY) tests/run_all.py

.PHONY: test-unit
test-unit:
	$(PY) tests/test_base.py
	$(PY) tests/test_config.py

.PHONY: test-checks
test-checks:
	$(PY) tests/test_checks.py

.PHONY: test-nopyaml
test-nopyaml:
	@echo "=== Test senza PyYAML (zero-deps) ==="
	@$(PY) -c "import sys; sys.modules['yaml'] = None" tests/run_all.py 2>/dev/null || \
	  PYTHONPATH=. $(PY) -c "\
import sys; \
sys.modules['yaml'] = type(sys)('yaml_disabled'); \
exec(open('tests/test_base.py').read()); \
exec(open('tests/test_config.py').read())"
	@echo ""
	@echo "=== Dry-run senza PyYAML ==="
	$(PY) -c "\
import sys; \
sys.modules['yaml'] = None; \
import hadoopscope; \
" 2>&1 || $(PY) hadoopscope.py --config $(CONFIG) --env $(ENV) --dry-run --output text 2>&1

# ---------------------------------------------------------------------------
# Esecuzione
# ---------------------------------------------------------------------------

.PHONY: dry-run
dry-run:
	$(PY) hadoopscope.py \
		--config $(CONFIG) \
		--env $(ENV) \
		--checks all \
		--dry-run \
		--output text

.PHONY: dry-run-json
dry-run-json:
	$(PY) hadoopscope.py \
		--config $(CONFIG) \
		--env $(ENV) \
		--dry-run \
		--output json

.PHONY: caps
caps:
	$(PY) hadoopscope.py --show-capabilities

.PHONY: run
run:
	$(PY) hadoopscope.py \
		--config $(CONFIG) \
		--env $(ENV) \
		--output text

# ---------------------------------------------------------------------------
# Documentazione
# ---------------------------------------------------------------------------

.PHONY: docs
docs:
	$(VENV_DIR)/bin/mkdocs build --site-dir _site
	@echo "Docs generate in _site/"

.PHONY: docs-serve
docs-serve:
	$(VENV_DIR)/bin/mkdocs serve

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

.PHONY: docker-test
docker-test:
	docker compose up --build --abort-on-container-exit
	docker compose down --remove-orphans

.PHONY: docker-build
docker-build:
	docker build -t hadoopscope:dev .
	docker build -t hadoopscope-mock:dev -f Dockerfile.mock .

.PHONY: docker-run
docker-run:
	docker run --rm hadoopscope:dev --help

# ---------------------------------------------------------------------------
# Qualità
# ---------------------------------------------------------------------------

.PHONY: lint
lint:
	@if $(PY) -m ruff --version >/dev/null 2>&1; then \
		$(PY) -m ruff check . --exclude .venv; \
	elif command -v ruff >/dev/null 2>&1; then \
		ruff check . --exclude .venv; \
	else \
		echo "ruff non installato. Usa: pip install ruff"; \
	fi

# ---------------------------------------------------------------------------
# Pulizia
# ---------------------------------------------------------------------------

.PHONY: clean
clean:
	find . -name "__pycache__" -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -not -path "./.venv/*" -delete 2>/dev/null || true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf _site/
	@echo "Pulito."

.PHONY: clean-venv
clean-venv:
	rm -rf $(VENV_DIR)
	@echo "Venv rimosso. Usa 'make setup' per ricrearlo."
