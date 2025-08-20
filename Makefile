# Makefile for CASPaxos Python Implementation

# Python interpreter from virtual environment
PYTHON := /app/env/bin/python

# Set PYTHONPATH to include the app directory
export PYTHONPATH := /app

# Default target
.PHONY: help
help:
	@echo "CASPaxos Python Implementation"
	@echo "=============================="
	@echo ""
	@echo "Available targets:"
	@echo "  make test           Run all tests"
	@echo "  make test-verbose   Run tests with verbose output"
	@echo "  make test-coverage  Run tests with coverage report"
	@echo "  make test-ballot    Run ballot tests only"
	@echo "  make test-acceptor  Run acceptor tests only"
	@echo "  make test-proposer  Run proposer tests only"
	@echo "  make test-caspaxos  Run main CASPaxos tests only"
	@echo "  make example        Run the example script"
	@echo "  make demo           Run a quick demo (alias for example)"
	@echo "  make clean          Remove Python cache files"
	@echo "  make install-deps   Install test dependencies"
	@echo "  make all            Run tests and example"
	@echo ""
	@echo "Examples:"
	@echo "  make test          # Run all tests"
	@echo "  make example       # Run usage examples"
	@echo "  make all           # Run everything"

# Run all tests
.PHONY: test
test:
	@echo "Running CASPaxos tests..."
	@$(PYTHON) -m pytest caspaxos/__tests__/ -q

# Run tests with verbose output
.PHONY: test-verbose
test-verbose:
	@echo "Running CASPaxos tests (verbose)..."
	@$(PYTHON) -m pytest caspaxos/__tests__/ -v

# Run tests with coverage
.PHONY: test-coverage
test-coverage:
	@echo "Running CASPaxos tests with coverage..."
	@$(PYTHON) -m pytest caspaxos/__tests__/ \
		--cov=caspaxos \
		--cov-report=term-missing \
		--cov-report=html:caspaxos_coverage \
		--cov-branch

# Run specific test modules
.PHONY: test-ballot
test-ballot:
	@echo "Running ballot tests..."
	@$(PYTHON) -m pytest caspaxos/__tests__/test_ballot.py -v

.PHONY: test-acceptor
test-acceptor:
	@echo "Running acceptor tests..."
	@$(PYTHON) -m pytest caspaxos/__tests__/test_acceptor.py -v

.PHONY: test-proposer
test-proposer:
	@echo "Running proposer tests..."
	@$(PYTHON) -m pytest caspaxos/__tests__/test_proposer.py -v

.PHONY: test-caspaxos
test-caspaxos:
	@echo "Running main CASPaxos tests..."
	@$(PYTHON) -m pytest caspaxos/__tests__/test_caspaxos.py -v

# Run the example script
.PHONY: example
example:
	@echo "Running CASPaxos examples..."
	@echo "================================"
	@cd /app && $(PYTHON) -m caspaxos.caspaxos.example

# Alias for example
.PHONY: demo
demo: example

# Run tests and example
.PHONY: all
all: test example
	@echo ""
	@echo "================================"
	@echo "All tests and examples completed successfully!"

# Clean Python cache files
.PHONY: clean
clean:
	@echo "Cleaning Python cache files..."
	@find caspaxos -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find caspaxos -type f -name "*.pyc" -delete 2>/dev/null || true
	@find caspaxos -type f -name "*.pyo" -delete 2>/dev/null || true
	@find caspaxos -type f -name ".coverage" -delete 2>/dev/null || true
	@rm -rf caspaxos_coverage 2>/dev/null || true
	@rm -rf .pytest_cache 2>/dev/null || true
	@echo "Clean complete."

# Install dependencies
.PHONY: install-deps
install-deps:
	@echo "Installing test dependencies..."
	@$(PYTHON) -m pip install pytest pytest-asyncio pytest-cov
	@echo "Dependencies installed."

# Watch tests (requires pytest-watch)
.PHONY: watch
watch:
	@echo "Watching for file changes..."
	@$(PYTHON) -m pytest_watch caspaxos/__tests__/ --clear

# Run a specific test by pattern
.PHONY: test-match
test-match:
	@if [ -z "$(PATTERN)" ]; then \
		echo "Usage: make test-match PATTERN=<test_name_pattern>"; \
		echo "Example: make test-match PATTERN=test_ballot"; \
		exit 1; \
	fi
	@echo "Running tests matching pattern: $(PATTERN)"
	@$(PYTHON) -m pytest caspaxos/__tests__/ -k "$(PATTERN)" -v

# Quick test run (no coverage, minimal output)
.PHONY: quick
quick:
	@$(PYTHON) -m pytest caspaxos/__tests__/ -q --tb=short

# Check code without running (dry run)
.PHONY: check
check:
	@echo "Checking Python syntax..."
	@$(PYTHON) -m py_compile caspaxos/caspaxos.py
	@$(PYTHON) -m py_compile caspaxos/core/*.py
	@$(PYTHON) -m py_compile caspaxos/storage/*.py
	@echo "Syntax check passed."

.PHONY: test-parallel
test-parallel:
	@echo "Running tests in parallel..."
	@$(PYTHON) -m pytest caspaxos/__tests__/ -n auto

# Print the Python path for debugging
.PHONY: debug-path
debug-path:
	@echo "Python: $(PYTHON)"
	@echo "PYTHONPATH: $(PYTHONPATH)"
	@$(PYTHON) --version
	@$(PYTHON) -c "import sys; print('Python path:', sys.path[:3])"