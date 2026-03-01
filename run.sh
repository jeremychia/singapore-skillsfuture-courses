#!/bin/bash

# ============================================
# SkillsFuture Data Pipeline Runner
# ============================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default settings
RUN_EXTRACTORS=false
RUN_MODELLING=false

# --------------------------------------------
# Helper Functions
# --------------------------------------------
print_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_step() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

show_help() {
    echo "Usage: ./run.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --extractors    Run extraction scripts (courses & course_details)"
    echo "  -m, --modelling     Run modelling scripts"
    echo "  -a, --all           Run everything (extractors + modelling)"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./run.sh -e         # Run only extractors"
    echo "  ./run.sh -m         # Run only modelling"
    echo "  ./run.sh -a         # Run full pipeline"
    echo "  ./run.sh -e -m      # Same as --all"
}

# --------------------------------------------
# Parse Arguments
# --------------------------------------------
if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--extractors)
            RUN_EXTRACTORS=true
            shift
            ;;
        -m|--modelling)
            RUN_MODELLING=true
            shift
            ;;
        -a|--all)
            RUN_EXTRACTORS=true
            RUN_MODELLING=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# --------------------------------------------
# STAGE 1: Extractors
# --------------------------------------------
if [ "$RUN_EXTRACTORS" = true ]; then
    print_header "STAGE 1: Running Extractors"
    
    print_step "Extracting courses..."
    python extractor_courses/main.py
    print_success "Courses extraction complete"
    
    print_step "Extracting course details..."
    python extractor_details/main.py
    print_success "Course details extraction complete"
fi

# --------------------------------------------
# STAGE 2: Modelling
# --------------------------------------------
if [ "$RUN_MODELLING" = true ]; then
    print_header "STAGE 2: Running Modelling"
    
    print_step "Building courses model..."
    python modelling/courses.py
    print_success "Courses model complete"
    
    print_step "Building course_runs model..."
    python modelling/course_runs.py
    print_success "Course runs model complete"
    
    print_step "Building training_providers model..."
    python modelling/training_providers.py
    print_success "Training providers model complete"
    
    print_step "Building training_locations model..."
    python modelling/training_locations.py
    print_success "Training locations model complete"
    
    print_step "Building trainers model..."
    python modelling/trainers.py
    print_success "Trainers model complete"
fi

# --------------------------------------------
# Summary
# --------------------------------------------
print_header "Pipeline Complete"
echo -e "${GREEN}All selected stages completed successfully!${NC}"
