# Cloud Market Data Pipeline

A containerized data pipeline for fetching and storing stock market data, deployed on AWS infrastructure.

## Features

- Docker containerization for portability
- Fetches 1-minute intraday data for 50 major stocks
- PostgreSQL database storage
- Error handling and logging
- Designed for AWS ECS deployment

## Local Development

### Prerequisites
- Docker Desktop
- Python 3.10+
- PostgreSQL database (or use Docker)

### Setup

1. Clone this repository
2. Copy `.env.example` to `.env` and fill in your credentials
3. Build the Docker image:
   ```bash
   docker build -t market-data-pipeline .
   ```

4. Run locally:
   ```bash
   docker run --env-file .env market-data-pipeline
   ```

## AWS Deployment

### Phase 1: EC2 Deployment
- Deploy container to EC2 instance
- Manual scaling

### Phase 2: ECS Deployment  
- Push image to Amazon ECR
- Deploy to ECS cluster
- Auto-scaling capabilities

### Phase 3: Monitoring
- CloudWatch logs integration
- CloudWatch metrics and alarms
- Automated failure notifications

### Phase 4: Infrastructure as Code
- Terraform configuration
- Reproducible infrastructure
- Multi-environment support


## Technology Stack

- **Language**: Python 3.10
- **Database**: PostgreSQL
- **Containerization**: Docker
- **Cloud Platform**: AWS (EC2, ECS, ECR, RDS, CloudWatch)
- **IaC**: Terraform (planned)
