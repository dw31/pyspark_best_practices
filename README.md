# PySpark Best Practices: Functional Programming for Apache Spark

A collection of educational Jupyter notebooks demonstrating functional programming principles for PySpark development. Works with both **Databricks** and **local Python environments** using Delta Lake or Apache Iceberg table formats.

## Overview

This repository provides hands-on learning materials for data engineers and developers who want to master functional programming patterns in PySpark. Each notebook combines theoretical concepts with practical, runnable examples that demonstrate how to build maintainable, testable, and performant data pipelines.

## Platform Support

These notebooks support two deployment modes:

### 🌐 Databricks Environment (Recommended)
- **Databricks Runtime**: 12.2 LTS or later
- Pre-configured PySpark and Delta Lake
- Photon acceleration and optimized Spark configurations
- Built-in `spark` session and `dbutils` utilities
- Integrated data visualization and collaboration

### 💻 Local Development Environment
- **Python**: >= 3.8
- **Java**: 11 or 17 (required for Spark)
- **PySpark**: >= 3.4.0
- **Table Formats**: Delta Lake (>= 3.0.0) or Apache Iceberg (>= 0.5.0)
- All dependencies managed via `requirements.txt`

## Dependencies and Requirements

### Core Dependencies
- **PySpark**: >= 3.4.0 (Spark's Python API)
- **Delta Lake**: >= 3.0.0 (ACID lakehouse storage)
- **Apache Iceberg**: >= 0.5.0 (Alternative lakehouse format)
- **Python**: >= 3.8
- **PyArrow**: >= 12.0.0 (Arrow integration for performance)

### Testing Framework
- **pytest**: >= 7.4.0 (Testing framework)
- **pytest-cov**: >= 4.1.0 (Coverage reporting)
- **pytest-xdist**: >= 3.3.0 (Parallel test execution)
- **pytest-mock**: >= 3.11.0 (Mocking utilities)
- **chispa**: >= 0.9.0 (PySpark DataFrame testing)

### Optional Tools
- **Great Expectations**: >= 0.17.0 (Advanced data validation)
- **JupyterLab**: >= 4.0.0 (Local notebook environment)
- **Black/Ruff**: Code formatting and linting

## Repository Structure

### Progress Tracker

**Completed Notebooks**: 19 of 19 ✅ **COMPLETE**
- ✅ Section 1: Spark Fundamentals (1/1)
- ✅ Section 2: Functional Programming (3/3)
- ✅ Section 3: Test-First Development (3/3)
- ✅ Section 4: Data Quality (2/2)
- ✅ Section 5: Performance Optimization (4/4)
- ✅ Section 6: Declarative Pipelines (6/6)

**Appendices**: 2 supplemental notebooks
- Appendix 1.1: Modular Design and Project Structure
- Appendix 1.2: Dependency Management and Package Distribution

---

### Section 1: Spark Fundamentals
**1.1_Understanding_Spark_Lazy_Evaluation_and_Immutability.ipynb**
- Core concepts of lazy evaluation and DataFrame immutability
- How Spark's architecture supports functional programming
- Practical demonstrations of DAG construction and optimization
- Functional pipeline composition patterns
- **Key Learning**: Understanding why Spark naturally aligns with functional principles

### Section 2: Functional Programming Paradigms
**2.1_Embracing_Pure_Functions_and_Minimizing_Side_Effects.ipynb**
- Definition and implementation of pure functions in PySpark
- Separating transformation logic from side effects (actions)
- Creating testable, reusable transformation functions
- Configuration management through dependency injection
- **Key Learning**: Building predictable, composable data transformations

**2.2_Leveraging_PySpark_Built_in_Functions_and_Higher_Order_Functions.ipynb**
- Performance comparison: Built-in functions vs UDFs vs Pandas UDFs
- Higher-order functions for complex array operations
- Functional composition with built-in functions
- Building reusable function libraries
- **Key Learning**: Maximizing performance while maintaining functional purity

**2.3_Effective_Chaining_and_Composition_of_Transformations.ipynb**
- Advanced method chaining patterns for readable pipelines
- Schema contracts with structured `select` statements as data contracts
- Breaking down complex transformations into modular, named functions
- Higher-order function composition and pipeline builder patterns
- Chaining limit guidelines (max 5 operations, use `.transform()` for longer chains)
- Managing complex business logic with clarity and maintainability
- Performance considerations: all approaches have similar performance due to lazy evaluation
- **Key Learning**: Balancing functional composition power with code readability and maintainability

### Section 3: Test-First Development
**3.1_Unit_Testing_PySpark_Code_with_Pytest_in_Databricks.ipynb**
- Structuring PySpark code for testability
- Pytest fixtures for SparkSession and synthetic data
- Mocking Databricks utilities (`dbutils`) for isolated testing
- DataFrame assertion utilities and schema validation
- **Key Learning**: Test-driven development for distributed data processing

**3.2_Validating_DataFrames_and_Schemas.ipynb**
- Functional validation patterns with immutable result types
- Composable validation rules and pipeline construction
- Integration with Great Expectations framework
- Error handling and validation reporting
- **Key Learning**: Building robust data quality validation systems

**3.3_Integrating_Tests_with_Databricks_Repos_and_CICD.ipynb**
- CI/CD pipeline configuration for PySpark projects
- GitHub Actions and Azure DevOps integration examples
- Databricks job configuration for automated testing
- Project structure for collaborative development
- **Key Learning**: Professional-grade development workflows for data pipelines

### Section 4: Data Quality and Validation
**4.1_Implementing_Schema_Enforcement_and_Constraints_with_Delta_Lake.ipynb**
- Delta Lake schema enforcement mechanisms
- CHECK constraints for business rule validation
- Safe schema evolution patterns
- Building declarative data quality utilities
- **Key Learning**: Declarative data quality at the storage layer

**4.2_Declarative_Data_Quality_with_Delta_Live_Tables_DLT_Expectations.ipynb**
- Delta Live Tables expectations framework (WARN, DROP, FAIL strategies)
- Declarative vs imperative data quality patterns
- Functional pipeline simulation with expectation evaluation
- Quality metrics, monitoring, and automated reporting
- Multi-layer quality strategy (Bronze → Silver → Gold)
- Real DLT pipeline code examples for Databricks
- Testing DLT expectations before deployment
- **Key Learning**: Platform-native declarative data quality automation

### Section 5: Performance Optimization
**5.1_Strategic_Data_Handling_Caching_Broadcast_Joins_and_Efficient_Formats.ipynb**
- Caching strategies: Spark Cache vs Delta Cache
- Intelligent broadcast join optimization
- File format performance analysis (Parquet, Delta, JSON, CSV)
- Memory management for functional pipelines
- **Key Learning**: Performance optimization without sacrificing functional principles

**5.2_Logging_and_Error_Reporting_in_Functional_PySpark_Pipelines.ipynb**
- Structured logging with PySpark's PySparkLogger (Spark 4+)
- Standard Python logging module for older runtimes
- JSON logging for scalable log analysis and monitoring
- Centralized log storage with Unity Catalog volumes
- Programmatic error handling with PySparkException
- Error condition classification and debugging strategies
- **Key Learning**: Production-grade observability for functional pipelines

**5.3_Minimizing_Data_Shuffling_and_Addressing_Data_Skew.ipynb**
- Understanding wide vs narrow transformations and shuffle behavior
- Partitioning strategies: `repartition()` vs `coalesce()` optimization
- Data skew detection through distribution analysis and Spark UI
- Skew remediation: AQE, filtering, salting, and parallelism tuning
- Join optimization strategies (broadcast, bucketed, partitioned)
- Functional utilities for shuffle metrics and monitoring
- Performance comparison of shuffle optimization techniques
- **Key Learning**: Distributed system optimization for functional code

**5.4_Advanced_Cluster_Configuration_and_Tuning.ipynb**
- Critical Spark configuration parameters and precedence hierarchy
- Adaptive Query Execution (AQE) for automatic runtime optimization
- Photon acceleration for 2-10x performance improvements
- Cluster sizing strategies by workload type
- Spark memory model and allocation tuning
- Configuration management best practices and anti-patterns
- Cost optimization through proper resource allocation
- **Key Learning**: Platform-level optimizations that enhance functional programming

### Section 6: Declarative Pipelines with pyspark.pipelines
**6.1_Introduction_to_pyspark_pipelines_and_Lakeflow_Architecture.ipynb**
- Evolution from Delta Live Tables (`dlt`) to Lakeflow Declarative Pipelines (`pyspark.pipelines`)
- Apache Spark 4.1+ open-source declarative pipelines vs Databricks Lakeflow extensions
- Declarative vs imperative pipeline paradigms with functional alignment
- Core concepts: tables, views, flows, and automatic dependency resolution
- Lakeflow platform ecosystem: Connect (ingestion), Declarative Pipelines (transformation), Jobs (orchestration)
- Migration guide from legacy `import dlt` to modern `from pyspark import pipelines as dp`
- **Key Learning**: Building declarative, functional data pipelines with automatic orchestration

**6.2_Defining_Tables_Views_and_Sinks.ipynb**
- Defining batch tables with `@dp.table` and configuration options
- Creating materialized views (`@dp.materialized_view`) for pre-computed aggregations
- Using temporary views (`@dp.temporary_view`) for intermediate transformations
- Table properties: partitioning, clustering, Delta Lake optimization settings
- Custom sinks for external system integration (JDBC, cloud storage, APIs)
- Dependency management and complex multi-layer architectures
- Functional composition patterns for testable, reusable table definitions
- **Key Learning**: Declarative table definitions with pure functions and optimal configuration

**6.3_Streaming_Tables_and_Incremental_Processing.ipynb**
- Real-time data processing with `@dp.streaming_table`
- Streaming vs batch table patterns and use cases
- Checkpointing and exactly-once processing guarantees
- Incremental processing strategies and watermarking
- Handling late-arriving data and out-of-order events
- Stream-to-stream and stream-to-batch joins
- Performance optimization for streaming workloads
- **Key Learning**: Building robust real-time pipelines with declarative streaming tables

**6.4_Data_Quality_with_Expectations_in_Lakeflow.ipynb**
- Migrating from `@dlt.expect` to `@dp.expect` for data quality
- Three expectation strategies: WARN (`@dp.expect`), DROP (`@dp.expect_or_drop`), FAIL (`@dp.expect_or_fail`)
- Layered quality strategy: Bronze (monitor), Silver (cleanse), Gold (enforce)
- Composable expectation patterns and reusable quality rules
- Quality metrics collection and monitoring
- Testing expectations before deployment
- **Key Learning**: Declarative data quality with functional expectation patterns

**6.5_Flows_and_Advanced_CDC_Patterns.ipynb**
- Append flows with `dp.append_flow()` for incremental data loading
- Automatic change data capture with `dp.create_auto_cdc_flow()`
- CDC from snapshot tables with `dp.create_auto_cdc_from_snapshot_flow()`
- Handling inserts, updates, and deletes declaratively
- Type 1 and Type 2 slowly changing dimensions
- Merge strategies and conflict resolution
- **Key Learning**: Advanced data integration patterns with declarative CDC flows

**6.6_Best_Practices_and_Anti_Patterns_for_Lakeflow.ipynb**
- Prohibited operations in pipeline definitions (no `.collect()`, `.write()`, `.count()`)
- Table vs view vs temporary view selection criteria
- Performance anti-patterns and optimization strategies
- Testing strategies for declarative pipelines
- Migration checklist from imperative to declarative patterns
- Troubleshooting common pipeline issues
- **Key Learning**: Production-ready declarative pipelines following functional best practices

### Appendix 1: Advanced Topics

**Appendix_1.1_Modular_Design_and_Project_Structure.ipynb**
- Organizing functional PySpark code into modular, reusable components
- Abstraction layers: Extract-Transform-Load (ETL) pattern with pure transformations
- Idempotent transformation design for fault tolerance and safe retries
- Project structure best practices for production-grade PySpark applications
- Configuration management with immutable dataclasses
- Separation of concerns: data extraction, transformation, and loading layers
- **Key Learning**: Scaling functional code through modular design and clean architecture

**Appendix_1.2_Dependency_Management_and_Package_Distribution.ipynb**
- Dependency management strategies in Databricks (cluster-scoped, notebook-scoped, Unity Catalog)
- Building distributable Python wheel packages for PySpark libraries
- Requirements file management and version pinning for reproducibility
- Security and governance: vulnerability scanning, license compliance, access controls
- Complete package deployment workflow from development to production
- CI/CD automation for package building and deployment
- **Key Learning**: Professional dependency management and package distribution for production PySpark

## Getting Started

> 📖 **Quick Start Guide**: See [SETUP_INSTRUCTIONS.md](SETUP_INSTRUCTIONS.md) for platform-specific setup

### Option 1: Databricks Environment (Recommended)

**No setup required!** Notebooks work out-of-the-box.

1. **Import notebooks**: Upload the `notebooks/` folder to your Databricks workspace
2. **Create a cluster**: Use Databricks Runtime 12.2 LTS or later
3. **Run notebooks**: Start with Section 1, `spark` is pre-configured
4. **Install additional libraries** (optional for testing sections):
   ```python
   %pip install pytest great-expectations chispa
   ```

**Important**: Keep the `# %run 00_Environment_Setup.ipynb` line commented out in Databricks.

### Option 2: Local Development Environment

#### Prerequisites
- **Python 3.8+**: Verify with `python --version`
- **Java 11 or 17**: Required for Spark (verify with `java -version`)
  - macOS: `brew install openjdk@17`
  - Ubuntu/Debian: `sudo apt install openjdk-17-jdk`
  - Windows: Download from [Adoptium](https://adoptium.net/)

#### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/dw31/pyspark_best_practices.git
   cd pyspark_best_practices
   ```

2. **Create virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Java environment** (if not already configured):
   ```bash
   # macOS/Linux
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS
   # or
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Linux

   # Windows (PowerShell)
   $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
   ```

5. **Launch Jupyter**:
   ```bash
   jupyter lab
   # or
   jupyter notebook
   ```

6. **Open notebooks** in the `notebooks/` directory

7. **Enable local setup**: In each notebook's first code cell, **uncomment** this line:
   ```python
   %run 00_Environment_Setup.ipynb
   ```

#### Local Development Notes

- **Setup Notebook**: `00_Environment_Setup.ipynb` initializes Spark and loads utilities for local development
- **Platform Detection**: Uncomment `%run` line for local, keep commented for Databricks
- **Table Formats**: Delta Lake disabled by default for compatibility; Spark DataFrames work normally
- **Performance**: Local mode uses all available CPU cores (`local[*]`)
- **Storage**: Data written to `./data/` directory (gitignored)
- **Testing**: Run tests with `pytest tests/` from the project root

## Usage Patterns

### For Learning
- **Sequential Learning**: Follow notebooks in order (1.1 → 6.2)
- **Topic-Focused**: Jump to specific sections based on your needs
- **Hands-On Practice**: Complete exercises at the end of each notebook

### For Reference
- **Pattern Library**: Use code examples as templates for your projects
- **Best Practices Guide**: Reference anti-patterns and recommendations
- **Testing Patterns**: Adapt testing utilities for your codebase

### For Implementation
- **Extract Utilities**: Copy functional utilities to your projects
- **Adapt Examples**: Modify examples for your specific use cases
- **Build on Foundations**: Use notebooks as starting points for advanced implementations

## Key Learning Outcomes

After completing this material, you will be able to:

1. **Apply Functional Principles**: Write pure, composable PySpark transformations leveraging immutability and lazy evaluation
2. **Test Effectively**: Implement comprehensive testing strategies for distributed code with pytest and data validation
3. **Ensure Data Quality**: Build declarative validation systems with Delta Lake constraints and DLT expectations
4. **Optimize Performance**: Balance functional purity with distributed system performance through caching, partitioning, and cluster tuning
5. **Deploy Professionally**: Integrate functional PySpark code with CI/CD workflows and modern data engineering practices

## Contributing

This is an educational repository focused on demonstrating functional programming best practices in PySpark. When contributing:

- **Maintain Functional Purity**: All transformation examples should be pure functions
- **Include Practical Examples**: Use realistic data scenarios
- **Provide Context**: Explain why patterns are beneficial, not just how they work
- **Test Thoroughly**: Include working examples with expected outputs

## Table Formats: Delta Lake vs Apache Iceberg

Both Delta Lake and Apache Iceberg are open-source lakehouse table formats that provide ACID transactions, schema evolution, and time travel capabilities. This repository includes examples for both:

### Delta Lake (Primary Examples)
- **Best for**: Databricks environments, streaming workloads, Unity Catalog integration
- **Strengths**: Tight Databricks integration, Delta Live Tables, optimized for Photon
- **Use cases**: Production data pipelines, real-time analytics, ML feature stores

### Apache Iceberg (Alternative Examples)
- **Best for**: Multi-engine compatibility (Spark, Trino, Flink), open ecosystem
- **Strengths**: Engine-agnostic, strong community, flexible partition evolution
- **Use cases**: Multi-cloud deployments, engine-agnostic architectures

**Note**: Notebooks demonstrate concepts using Delta Lake by default, with Iceberg alternatives clearly marked in comments for easy substitution.

## Best Practices Philosophy

This repository emphasizes:
- **Evidence over Assumptions**: All recommendations backed by measurable benefits
- **Code over Documentation**: Working examples over theoretical explanations
- **Efficiency over Verbosity**: Practical patterns that improve development velocity
- **Functional over Imperative**: Leveraging Spark's natural functional alignment
- **Declarative over Imperative**: Using platform capabilities for quality and performance
- **Platform Flexibility**: Code patterns that work across Databricks and local environments

## Support

For questions about the content:
1. **Review the comprehensive best practices document**: `pyspark_best_practices.md`
2. **Check notebook summaries**: Each notebook includes key takeaways and next steps
3. **Practice with exercises**: Every notebook includes hands-on exercises
4. **Experiment with examples**: All code is designed to be modified and extended

---

*This repository represents a comprehensive educational resource for mastering functional programming in PySpark, specifically tailored for Databricks environments and modern data engineering practices.*
