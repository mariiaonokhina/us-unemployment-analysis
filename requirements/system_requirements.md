# System / Tool Requirements

To run the full project locally, you need:

## 1. Homebrew (macOS)

Install:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## 2. Command Line Tools

Install:

```bash
brew install curl
brew install unzip
```

## 3. Java (required for PySpark)

Install:

```bash
brew install openjdk@17
```

Set Java environment variable:

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

# 4. Apache Spark

Install:

```bash
brew install apache-spark
```

# 5. Python 3.10+ (recommended)

Install: 

```bash
brew install python
```

# Optional: For Faster Processing

Install:

```bash
brew install hadoop
brew install dask
```
