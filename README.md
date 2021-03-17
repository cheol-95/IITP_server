# IITP_server
**Sensor data verification platform using Celery**

**개발환경**
- Ubuntu 18.04.4 LTS
- Python v3.7.4
- Celery v4.4.5
- mongoDB v3.6.3
- Redis v4.0.9
- RabbitMQ
- Bson

**Server**
  1. Celery, MongoDB, Redis, RabbitMQ 등 서비스를 운영하기 위한 환경 구축
  2. Celery_task - Client의 요청에 맞는 기능 수행 정의
  3. MongoDB - aggregate 구성
  4. Redis - 요약 정보 데이터 구성
