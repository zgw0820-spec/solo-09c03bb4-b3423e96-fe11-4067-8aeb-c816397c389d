"""运行配置（环境变量）。"""
from __future__ import annotations

import os

# 迁移期间存储版本固定为 v1：所有请求只走 v1 API 路径
KUBERNETES_API_URL = os.environ.get("KUBERNETES_API_URL", "http://127.0.0.1:8080").rstrip("/")
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "500"))
MAX_CONFLICT_RETRIES = int(os.environ.get("MAX_CONFLICT_RETRIES", "5"))
MAX_SERVER_RETRIES = int(os.environ.get("MAX_SERVER_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.environ.get("RETRY_BACKOFF_SECONDS", "0.2"))
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))

CRD_GROUP = "ops.example.com"
CRD_PLURAL = "nodemaintenancepolicies"
CRD_NAME = "nodemaintenancepolicies.ops.example.com"
STORAGE_VERSION = "v1"
LEGACY_VERSION = "v1beta1"
