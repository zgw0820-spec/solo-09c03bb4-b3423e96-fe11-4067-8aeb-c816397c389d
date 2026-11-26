FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 同一镜像按 compose 的 command 启动两个角色：
#   迁移服务:   app.main:app      (端口 8000)
#   K8s API 替身: app.stub.app:app (端口 8080)
EXPOSE 8000 8080
