# IITP_server
Sensor data verification platform using Celery

-Celery, MongoDB, Redis, RabbitMQ, Pandas, Numpy
-Json, Bson, Bytearray

Server
  1. Celery, MongoDB, Redis, RabbitMQ 등 서비스를 운영하기 위한 환경 구축  -> 쉘 스크립트를 통해 한번에 설치하는 방안 업데이트 예정
  2. Celery_task - Client의 요청에 맞는 기능 수행 정의
  3. MongoDB - mongoDB ->aggregate ->Pipeline 구성
  4. redis, mongodb - ConnectionPool
