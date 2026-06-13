import time
import random
import logging
import threading
import requests
from dataclasses import dataclass
from typing import Any

# Import cấu hình tập trung từ hệ thống
from config.settings import settings

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 2.0      
    max_delay: float = 60.0      
    jitter: float = 1.0          

class TokenBucketLimiter:
    """Thuật toán Token Bucket bảo vệ Thread-safe, giải phóng Lock trước khi sleep."""
    def __init__(self, rate_per_min: int = 28):
        self._rate = rate_per_min / 60.0  
        self._max_tokens = float(rate_per_min)
        self._tokens = float(rate_per_min)
        self._last_check = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            wait_time = 0
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_check
                self._tokens = min(self._max_tokens, self._tokens + elapsed * self._rate)
                self._last_check = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return  

                wait_time = (1.0 - self._tokens) / self._rate

            # SỬA LỖI BUG KHÓA LUỒNG: Sleep ngoài khối Lock
            time.sleep(wait_time)

def _calc_backoff(attempt: int, cfg: RetryConfig, retry_after: int | None = None) -> float:
    """Tính toán toán học thời gian chờ Exponential Backoff + Jitter nhiễu."""
    if retry_after:
        return min(float(retry_after), cfg.max_delay)
    
    # SỬA LỖI BUG UNBOUNDLOCALERROR: Đưa biến ra phạm vi ngoài khối IF
    exponential = cfg.base_delay * (2 ** attempt)
    jitter = random.uniform(0, cfg.jitter)
    return min(exponential + jitter, cfg.max_delay)

class CoinGeckoClient:
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    RETRYABLE_STATUS = {429, 500, 502, 503, 504}
    NON_RETRYABLE_STATUS = {400, 401, 403, 404, 422}

    def __init__(self, cfg: RetryConfig | None = None):
        # LÝ DO ĐỔI: Tự động lấy Key từ settings, không bắt người dùng điền tay thủ công nữa
        self.api_key = settings.COINGECKO_API_KEY
        if not self.api_key:
            raise ValueError("[CRITICAL] Không tìm thấy COINGECKO_API_KEY trong cấu hình hệ thống!")
        
        self.cfg = cfg or RetryConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "DE-Portfolio-Pipeline/1.0",
            "x-cg-demo-api-key": self.api_key  
        })

    def get(self, endpoint: str, params: dict | None = None) -> Any:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        attempt = 0

        while attempt <= self.cfg.max_retries:
            try:
                resp = self.session.get(url, params=params, timeout=10)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = _calc_backoff(attempt, self.cfg)
                self._log_event("WARNING", "api_retry", attempt, f"Hạ tầng mạng lỗi: {str(e)}", next_wait=wait)
                time.sleep(wait)
                attempt += 1
                continue

            if resp.status_code == 200:
                self._log_event("INFO", "api_success", attempt, "Lấy dữ liệu thành công", status=200, endpoint=endpoint)
                return resp.json()

            if resp.status_code in self.NON_RETRYABLE_STATUS:
                self._log_event("ERROR", "api_non_retryable_error", attempt, resp.text[:200], status=resp.status_code)
                resp.raise_for_status()

            if resp.status_code in self.RETRYABLE_STATUS:
                if attempt >= self.cfg.max_retries:
                    break

                retry_after = None
                if "Retry-After" in resp.headers:
                    try:
                        retry_after = int(resp.headers["Retry-After"])
                    except ValueError:
                        pass

                wait = _calc_backoff(attempt, self.cfg, retry_after)
                self._log_event("WARNING", "api_retry", attempt, f"Gặp lỗi HTTP {resp.status_code}", next_wait=wait)
                time.sleep(wait)
                attempt += 1
                continue

            self._log_event("ERROR", "api_unknown_status", attempt, "HTTP Status lạ", status=resp.status_code)
            resp.raise_for_status()

        self._log_event("CRITICAL", "api_retry_exhausted", attempt, "Cạn kiệt lượt retry, dừng hệ thống")
        raise RuntimeError(f"API call failed after {self.cfg.max_retries} retries: {endpoint}")

    def _log_event(self, level: str, event: str, attempt: int, msg: str, **kwargs) -> None:
        log_payload = {
            "level": level,
            "component": "crawler_engine",
            "event": event,
            "attempt": attempt,
            "message": msg,
            **kwargs
        }
        if level == "INFO": logger.info(log_payload)
        elif level == "WARNING": logger.warning(log_payload)
        elif level == "ERROR": logger.error(log_payload)
        else: logger.critical(log_payload)