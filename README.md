# 🚀 DAG Pipeline with Streaming & AI Processing

A sophisticated, event-driven data pipeline system built with FastAPI, featuring real-time streaming with Kafka, AI-powered processing using OpenAI, distributed task processing with Redis Queue, and a modern React/Next.js frontend for pipeline orchestration and monitoring.

### 🎯 **DAG Pipeline Orchestration**

- **Dependency Resolution**: Automatic DAG dependency resolution using SQLAlchemy models
- **Block Scheduling**: Smart task scheduling based on dependency completion by Orchestrator.py
- **Data Flow Management**: Automatic data passing between blocks based on data types

### 🤖 **AI-Powered Processing Blocks**

- **CSV Reader**: Intelligent CSV parsing with column detection
- **Sentiment Analysis**: OpenAI GPT-4 powered sentiment classification
- **Toxicity Detection**: AI-powered content moderation
- **File Writer**: Configurable output generation with metadata

### 📡 **Real-time Event Streaming**

- **Kafka Integration**: Event streaming for Real time log displaying
- **WebSocket Broadcasting**: Real-time updates to frontend
- **Redis Pub/Sub**: Block completion event handling with Orchestrator
- **Event Persistence**: Event storage and replay capabilities

### ⚡ **Distributed Task Processing**

- **Redis Queue (RQ)**: Scalable background task processing
- **Multi-Worker Support**: Configurable worker instances
- **Task Monitoring**: Real-time task status tracking
- **Error Handling**: Comprehensive error handling and retry logic

## 🛠️ Tech Stack

### Backend

- **FastAPI** - High-performance async API framework
- **SQLAlchemy** - Advanced ORM with DAG support
- **Redis Queue (RQ)** - Simple job queuing system
- **Apache Kafka** - Distributed event streaming
- **OpenAI** - GPT-4 AI language models
- **SQLite** - Lightweight database (easily switchable)

### Frontend

- **Next.js 14** - React framework with App Router
- **TypeScript** - Type-safe JavaScript
- **Tailwind CSS** - Utility-first CSS framework
- **WebSocket** - Real-time communication

### Infrastructure

- **Docker & Docker Compose** - Containerized deployment
- **Redis** - In-memory data store & pub/sub
- **Zookeeper** - Kafka coordination service

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Poetry (Python package manager)
- Node.js 18+

### 1. Environment Setup

#### Clone Github Repo

```bash
git clone <repo-url>
cd DAG-Pipeline
```

#### Backend Environment Setup

```bash
cp .env.example .env
```

#### Frontend Environment Setup

```bash
cd ui
cp .env.example .env
cd ..
```

Use Frontend env as it is in env.example.
Just update the name

#### Configure Required Variables

**Backend (.env file):**

```bash
OPENAI_API_KEY=your_openai_api_key_here
```

**Frontend (ui/.env file):**

```bash
# Add any frontend-specific environment variables if needed
# Most frontend variables can use default values
```

### 2. Start Services

```shell script
docker-compose up -d
```

### 3. Access Applications

- **Backend API**: http://localhost:8333
- **Frontend UI**: http://localhost:3002
- **API Docs**: http://localhost:8333/docs

## 📊 Pipeline Types

### Sample Pipeline Structure

The key correction shows that:

1. **Sentiment Analysis** and **Toxicity Detection** run **in parallel** after CSV Reader completes
2. **File Writer - Sentiment** starts **immediately** when Sentiment Analysis finishes
3. **File Writer - Toxicity** starts **immediately** when Toxicity Detection finishes
4. The pipeline doesn't wait for both AI blocks to complete before starting file writing
5. Each AI block completion independently triggers its respective file writer

This creates a much more efficient pipeline where the total execution time is significantly reduced due to parallel processing of the AI analysis blocks.

### Block Dependencies

- **CSV Reader**: No dependencies (starts first)
- **Sentiment Analysis**: Depends on CSV Reader
- **Toxicity Detection**: Depends on CSV Reader
- **File Writer - Sentiment**: Depends on Sentiment Analysis completion
- **File Writer - Toxicity**: Depends on Toxicity Detection completion

### Execution Flow

1. **CSV Reader** executes first and extracts text data
2. **Sentiment Analysis** and **Toxicity Detection** start **in parallel** (both depend on CSV Reader)
3. **File Writer - Sentiment** starts **immediately** when Sentiment Analysis completes
4. **File Writer - Toxicity** starts **immediately** when Toxicity Detection completes

This creates a **parallel execution pattern** where both AI analysis blocks run simultaneously after CSV reading, and their respective file writers trigger independently based on completion.

## 🔄 Data Flow Architecture

### 1. **Input Processing**

- CSV files uploaded via frontend
- Automatic column detection and text extraction
- Data validation and error handling

### 2. **AI Processing**

- **Sentiment Analysis**: Batch processing with OpenAI GPT-4
- **Toxicity Detection**: Content moderation with AI
- Rate limiting and error handling for API calls

### 3. **Data Flow Management**

- Automatic data passing between blocks
- Type-safe data transfer using data_type field
- Config updates for dependent blocks

### 4. **Output Generation**

- Structured CSV outputs with metadata
- File size and record count tracking
- Download URLs for processed files

## 📡 Event System

### Event Types

- **Pipeline Events**: `pipeline_started`, `pipeline_completed`, `pipeline_failed`
- **Block Events**: `block_started`, `block_completed`, `block_failed`
- **Data Flow Events**: `data_ready` for block coordination

### Event Flow

1. **Worker Completion** → Redis Pub/Sub
2. **Orchestrator Processing** → Kafka Events
3. **WebSocket Broadcasting** → Real-time UI updates
