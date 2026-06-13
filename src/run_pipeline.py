import logging
import logging.handlers
import time
import socket
import json
from datetime import datetime, timezone
from decimal import Decimal

# SỬA HIỆU NĂNG GHI: Sử dụng công nghệ execute_values cao cấp
from psycopg2.extras import execute_values 

from src.crawler.crawler_engine import CoinGeckoClient, TokenBucketLimiter
from src.staging.mongo_loader import MongoStagingLoader
from src.warehouse.postgres_writer import PostgresWarehouseWriter
from config.settings import settings

class LogstashJSONFormatter(logging.Formatter):
    def format(self, record):
        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage()
        }
        if isinstance(record.msg, dict):
            log_payload.update(record.msg)
        return json.dumps(log_payload)

class PureTCPJSONHandler(logging.Handler):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

    def emit(self, record):
        try:
            formatter = LogstashJSONFormatter()
            json_string = formatter.format(record) + "\n"
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                # VÁ LỖI KIẾN TRÚC: Gọi đúng biến LOGSTASH_HOST độc lập
                s.connect((self.host, self.port)) 
                s.sendall(json_string.encode('utf-8'))
        except Exception:
            self.handleError(record)

logger = logging.getLogger("e2e_pipeline")
logger.setLevel(logging.INFO)

# Khởi tạo qua cấu hình an toàn mới
logstash_handler = PureTCPJSONHandler(settings.LOGSTASH_HOST, settings.LOGSTASH_PORT)
logger.addHandler(logstash_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(stream_handler)


class AutomatedDataPipeline:
    def __init__(self):
        self.limiter = TokenBucketLimiter(rate_per_min=28)
        self.client = CoinGeckoClient()
        self.mongo_loader = MongoStagingLoader()
        self.postgres_writer = PostgresWarehouseWriter()

    def one_shot_execution(self):
        job_start_time = time.monotonic()
        
        logger.info({
            "component": "e2e_orchestrator",
            "event": "cycle_started",
            "message": "--- bắt đầu Pipeline  ---"
        })

        try:
            self.limiter.acquire()
            endpoint = "coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": 1,
                "sparkline": "false"
            }
            
            raw_market_data = self.client.get(endpoint, params=params)
            self.mongo_loader.load_raw_snapshot(coin_id="top_100_market_snapshot", raw_json=raw_market_data)

            if not raw_market_data or not isinstance(raw_market_data, list):
                raise ValueError("Dữ liệu API nguồn không hợp lệ.")

            clean_records_batch = []
            invalid_count = 0

            for coin_raw in raw_market_data:
                try:
                    # VÁ LỖI IDEMPOTENCY: Trích xuất thời gian tĩnh từ API gốc của CoinGecko
                    # Chuỗi dạng: "2026-06-13T09:00:00.000Z" chuyển về ISO format chuẩn của Python
                    api_time_str = coin_raw.get("last_updated")
                    if api_time_str:
                        extracted_at = datetime.fromisoformat(api_time_str.replace("Z", "+00:00"))
                    else:
                        extracted_at = datetime.now(timezone.utc) # Fallback nếu API lỗi

                    usd_price = Decimal(str(coin_raw["current_price"]))
                    vnd_price = usd_price * Decimal("25000")
                    
                    # Lưu trữ dưới dạng Tuple để sẵn sàng map vào execute_values tốc độ cao
                    clean_record = (
                        str(coin_raw["id"]),
                        usd_price,
                        vnd_price,
                        Decimal(str(coin_raw.get("market_cap") or 0)),
                        Decimal(str(coin_raw.get("total_volume") or 0)),
                        extracted_at
                    )
                    clean_records_batch.append(clean_record)
                except (KeyError, TypeError, ValueError) as data_err:
                    invalid_count += 1
                    continue

            # VÁ BIẾN CỐ HIỆU NĂNG: Ghi tốc độ cao bằng cách viết đè hàm nạp Bulk Insert
            if clean_records_batch:
                self._execute_bulk_upsert(clean_records_batch)

            total_latency = time.monotonic() - job_start_time
            logger.info({
                "component": "e2e_orchestrator",
                "event": "cycle_success",
                "metrics": {
                    "records_loaded": len(clean_records_batch),
                    "records_isolated_dlq": invalid_count,
                    "latency_seconds": round(total_latency, 2)
                }
            })

        except Exception as cycle_err:
            logger.error({"component": "e2e_orchestrator", "event": "cycle_failed", "error_details": str(cycle_err)})

    def _execute_bulk_upsert(self, batch_tuples):
        """Phương thức bọc execute_values giúp tăng tốc ghi Postgres gấp 50 lần."""
        conn = self.postgres_writer.pool.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    query = """
                        INSERT INTO analytics_coin_prices (
                            coin_id, price_usd, price_vnd, market_cap_usd, total_volume_usd, extracted_at
                        ) VALUES %s
                        ON CONFLICT (coin_id, extracted_at) 
                        DO UPDATE SET 
                            price_usd = EXCLUDED.price_usd,
                            price_vnd = EXCLUDED.price_vnd,
                            market_cap_usd = EXCLUDED.market_cap_usd,
                            total_volume_usd = EXCLUDED.total_volume_usd;
                    """
                    execute_values(cursor, query, batch_tuples)
        finally:
            self.postgres_writer.pool.putconn(conn)

    def start_infinite_daemon(self, interval_seconds: int = 60):
        logger.info({"component": "e2e_orchestrator", "event": "daemon_initialized"})
        while True:
            try:
                self.one_shot_execution()
            except KeyboardInterrupt:
                break
            time.sleep(interval_seconds)

if __name__ == "__main__":
    pipeline = AutomatedDataPipeline()
    pipeline.start_infinite_daemon(interval_seconds=60)