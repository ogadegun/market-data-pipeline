# Cloud Market Data Pipeline

A containerized stock market data pipeline deployed on AWS infrastructure, demonstrating cloud-native architecture and serverless container orchestration.

## Project Overview

Built a production-ready data pipeline that fetches real-time intraday stock market data and stores it in AWS RDS PostgreSQL. Successfully deployed using multiple methods: local Docker, AWS EC2, and AWS ECS Fargate (serverless).

**Current Status:** 86,095 rows of 1-minute interval market data across 50 major stocks

## Features

- **Containerized Application**: Docker for portability and consistency across environments
- **Real-Time Data Ingestion**: Fetches 1-minute intraday data from Financial Modeling Prep API
- **Cloud Database**: AWS RDS PostgreSQL with automated backups and high availability
- **Serverless Orchestration**: AWS ECS Fargate deployment (no server management required)
- **Production Logging**: CloudWatch Logs integration for real-time monitoring
- **Data Integrity**: Unique constraints prevent duplicate entries
- **Error Handling**: Comprehensive retry logic and timeout handling for API reliability

## 📊 Key Metrics

- **86,095 rows** of market data stored in production database
- **50 stock tickers** tracked (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, and more)
- **5 days** of historical data (November 3-7, 2025)
- **~344 data points** per ticker per day (1-minute intervals during market hours)
- **100% data integrity** with unique constraints preventing duplicates

## 🛠️ Technology Stack

**Application Layer:**
- Python 3.10
- psycopg2 (PostgreSQL adapter)
- requests (API integration)

**Infrastructure:**
- **Containerization**: Docker
- **Container Registry**: AWS ECR, Docker Hub
- **Database**: PostgreSQL 15 (AWS RDS)
- **Compute**: AWS ECS Fargate (serverless), AWS EC2
- **Monitoring**: AWS CloudWatch Logs
- **Security**: AWS IAM, VPC, Security Groups
- **Networking**: AWS VPC with public subnets

**Phase 1: Local Development**
```
Docker Container (Local Laptop) → AWS RDS
```
- Initial testing and validation
- Verified application logic and database schema

**Phase 2: EC2 Deployment**
```
Docker Container (AWS EC2) → AWS RDS
```
- ✅ Successfully inserted 71,679 rows
- Validated cloud-to-cloud architecture
- Manual container management on virtual machine

**Phase 3: ECS Fargate (Current Production)**
```
AWS ECS Fargate → AWS RDS
```
- ✅ Successfully inserted 46,436 rows
- Serverless container orchestration
- Automatic scaling and infrastructure management
- Production-grade deployment

**Data Insertion Performance:**
- EC2 Deployment: 71,679 rows inserted
- ECS Fargate Deployment: 46,436 rows inserted
- Database Query Response: < 100ms for standard queries
- API Rate Limiting: Handles FMP API limits gracefully with retry logic

