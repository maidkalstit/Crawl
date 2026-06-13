import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from typing import List, Dict, Any

# Import cấu hình tập trung
from config.settings import settings

logger = logging.getLogger(__name__)

class PostgresWarehouseWriter:
    def __init__(self):
        """Khởi tạo Threaded Connection Pool tới PostgreSQL Warehouse."""
        try:
            # LÝ DO CHỌN: ThreadedConnectionPool cho phép duy trì và tái sử dụng kết nối an toàn giữa các luồng
            self.pool = ThreadedConnectionPool(
                minconn=5,
                maxconn=20,
                dsn=settings.postgres_uri
            )
            logger.info({"level": "INFO", "component": "postgres_writer", "event": "pool_initialized"})
        except Exception as e:
            logger.critical({"level": "CRITICAL", "component": "postgres_writer", "event": "pool_failed", "error": str(e)})
            raise RuntimeError(f"Không thể khởi tạo kết nối Postgres Pool: {e}")

    def upsert_clean_records(self, records: List[Dict[str, Any]]):
        """
        Ghi dữ liệu hàng loạt theo cơ chế UPSERT (INSERT ON CONFLICT DO UPDATE).
        Phương thức này đảm bảo tính Idempotency (Chạy lại nhiều lần không sợ trùng lặp dữ liệu).
        """
        if not records:
            return

        # Lấy 1 kết nối ra từ Pool
        conn = self.pool.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    # Câu lệnh SQL tối ưu: Nếu trùng lặp (coin_id, extracted_at) thì tự động cập nhật lại giá mới
                    query = """
                        INSERT INTO analytics_coin_prices (
                            coin_id, price_usd, price_vnd, market_cap_usd, total_volume_usd, extracted_at
                        ) VALUES (
                            %(coin_id)s, %(price_usd)s, %(price_vnd)s, %(market_cap_usd)s, %(total_volume_usd)s, %(extracted_at)s
                        )
                        ON CONFLICT (coin_id, extracted_at) 
                        DO UPDATE SET 
                            price_usd = EXCLUDED.price_usd,
                            price_vnd = EXCLUDED.price_vnd,
                            market_cap_usd = EXCLUDED.market_cap_usd,
                            total_volume_usd = EXCLUDED.total_volume_usd;
                    """
                    # Thực thi ghi theo mẻ (Batch Execution) để giảm thiểu TCP Round-trip
                    cursor.executemany(query, records)
            
            logger.info({
                "level": "INFO",
                "component": "postgres_writer",
                "event": "postgres_write_success",
                "batch_size": len(records)
            })
            
        except Exception as e:
            conn.rollback() # Khôi phục lại trạng thái trước khi lỗi để bảo vệ DB
            logger.error({
                "level": "ERROR",
                "component": "postgres_writer",
                "event": "postgres_write_failed",
                "error": str(e)
            })
            raise e
        finally:
            # Bắt buộc phải trả lại kết nối về cho Pool để các luồng khác tái sử dụng
            self.pool.putconn(conn)

if __name__ == "__main__":
    # Điền thư viện bắt buộc: pip install psycopg2-binary
    print("── [TESTING POSTGRES WRITER] ──")
    from datetime import datetime, timezone
    
    try:
        writer = PostgresWarehouseWriter()
        mock_clean_data = [{
            "coin_id": "bitcoin",
            "price_usd": 65200.50,
            "price_vnd": 1655000000.0,
            "market_cap_usd": 1280000000000.0,
            "total_volume_usd": 35000000000.0,
            "extracted_at": datetime.now(timezone.utc)
        }]
        writer.upsert_clean_records(mock_clean_data)
        print("Kiểm thử nạp Postgres thành công!")
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")