#!/bin/bash
# Quick testing commands for Global Commodity Data Pipeline

set -e

echo "==========================================="
echo "Global Commodity Pipeline - Test Suite"
echo "==========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${YELLOW}pytest not found. Installing testing dependencies...${NC}"
    pip install -r requirements-dev.txt
fi

echo ""
echo -e "${BLUE}Available commands:${NC}"
echo ""
echo -e "${GREEN}1. Run all tests${NC}"
echo "   ./run_tests.sh all"
echo ""
echo -e "${GREEN}2. Run tests with coverage${NC}"
echo "   ./run_tests.sh coverage"
echo ""
echo -e "${GREEN}3. Run specific test file${NC}"
echo "   ./run_tests.sh models   # test_models.py"
echo "   ./run_tests.sh utils    # test_utils.py"
echo "   ./run_tests.sh dags     # test_dags.py"
echo ""
echo -e "${GREEN}4. Run tests in parallel${NC}"
echo "   ./run_tests.sh parallel"
echo ""
echo -e "${GREEN}5. Run with verbose output${NC}"
echo "   ./run_tests.sh verbose"
echo ""
echo -e "${GREEN}6. Run with HTML report${NC}"
echo "   ./run_tests.sh html"
echo ""

# Get command argument
CMD=${1:-help}

case $CMD in
    all)
        echo -e "${BLUE}Running all tests...${NC}"
        pytest tests/ -v
        ;;
    
    coverage)
        echo -e "${BLUE}Running tests with coverage report...${NC}"
        pytest tests/ --cov=dags --cov-report=html --cov-report=term-missing
        echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
        ;;
    
    models)
        echo -e "${BLUE}Running model tests...${NC}"
        pytest tests/test_models.py -v
        ;;
    
    utils)
        echo -e "${BLUE}Running utils tests...${NC}"
        pytest tests/test_utils.py -v
        ;;
    
    dags)
        echo -e "${BLUE}Running DAG tests...${NC}"
        pytest tests/test_dags.py -v
        ;;
    
    parallel)
        echo -e "${BLUE}Running tests in parallel...${NC}"
        pytest tests/ -n auto -v
        ;;
    
    verbose)
        echo -e "${BLUE}Running with verbose output...${NC}"
        pytest tests/ -vv --tb=long
        ;;
    
    html)
        echo -e "${BLUE}Running with HTML report...${NC}"
        pytest tests/ --html=report.html --self-contained-html
        echo -e "${GREEN}HTML report generated: report.html${NC}"
        ;;
    
    quick)
        echo -e "${BLUE}Running quick tests (no coverage)...${NC}"
        pytest tests/ -x --tb=short
        ;;
    
    *)
        echo "Usage: ./run_tests.sh [command]"
        echo ""
        echo "Commands:"
        echo "  all         - Run all tests"
        echo "  coverage    - Run with coverage report"
        echo "  models      - Run model tests only"
        echo "  utils       - Run utils tests only"
        echo "  dags        - Run DAG tests only"
        echo "  parallel    - Run tests in parallel"
        echo "  verbose     - Run with verbose output"
        echo "  html        - Generate HTML report"
        echo "  quick       - Quick test (stop on first failure)"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}✅ Tests completed!${NC}"
