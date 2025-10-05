# Contributing to Gofka Python Client

Thank you for your interest in contributing to the Gofka Python client! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.7 or higher
- pip
- virtualenv (recommended)

### Setup Development Environment

1. Clone the repository:
```bash
git clone https://github.com/user/gofka.git
cd gofka/clients/python
```

2. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the package in development mode:
```bash
pip install -e ".[dev]"
```

4. Verify installation:
```bash
python -c "import gofka; print(gofka.__version__)"
```

## Running Tests

### Unit Tests

Run all tests:
```bash
pytest
```

Run with coverage:
```bash
pytest --cov=gofka --cov-report=html
```

Run specific test file:
```bash
pytest tests/test_producer.py
```

### Integration Tests

Integration tests require a running Gofka broker on `localhost:9092`.

Start the broker:
```bash
cd ../..
go run cmd/broker/main.go
```

Run integration tests:
```bash
pytest -m integration
```

## Code Style

We follow PEP 8 style guidelines and use automated tools to enforce them.

### Format Code

```bash
# Format with black
black gofka/ tests/

# Or use ruff
ruff format gofka/ tests/
```

### Lint Code

```bash
# Lint with pylint
pylint gofka/

# Or use ruff
ruff check gofka/ tests/
```

### Type Checking

```bash
mypy gofka/
```

## Project Structure

```
clients/python/
├── gofka/                  # Main package
│   ├── __init__.py        # Package exports
│   ├── protocol.py        # Low-level protocol
│   ├── producer.py        # Producer client
│   ├── consumer.py        # Consumer client
│   ├── admin.py           # Admin client
│   └── exceptions.py      # Exception classes
├── tests/                 # Test suite
│   ├── __init__.py
│   ├── conftest.py        # Pytest fixtures
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_admin.py
├── examples/              # Example scripts
│   ├── simple_producer.py
│   ├── simple_consumer.py
│   └── admin_demo.py
├── setup.py              # Setup configuration
├── pyproject.toml        # Project metadata and tools
├── MANIFEST.in           # Package manifest
├── README.md             # Documentation
└── LICENSE               # License file
```

## Making Changes

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Write clean, readable code
- Follow existing code style
- Add docstrings to all public functions and classes
- Update README.md if adding new features

### 3. Add Tests

- Write unit tests for new functionality
- Ensure all tests pass
- Aim for high test coverage

### 4. Update Documentation

- Update README.md with new features
- Add docstrings to new functions/classes
- Update examples if needed

### 5. Commit Your Changes

```bash
git add .
git commit -m "feat: add new feature description"
```

Follow conventional commit messages:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Adding tests
- `refactor:` Code refactoring
- `style:` Formatting changes
- `chore:` Maintenance tasks

### 6. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure all tests pass
- Update documentation as needed
- Keep changes focused and atomic

## Reporting Bugs

When reporting bugs, please include:

1. Python version
2. Gofka version
3. Operating system
4. Steps to reproduce
5. Expected behavior
6. Actual behavior
7. Error messages or stack traces

## Feature Requests

We welcome feature requests! Please:

1. Check if the feature already exists
2. Provide a clear use case
3. Describe the expected behavior
4. Consider backward compatibility

## Code Review Process

1. Maintainers review all pull requests
2. Automated tests must pass
3. Code style checks must pass
4. At least one approval required
5. Maintainers merge approved PRs

## Getting Help

- GitHub Issues: https://github.com/user/gofka/issues
- Documentation: https://github.com/user/gofka/tree/main/docs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
