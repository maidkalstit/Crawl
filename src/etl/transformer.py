import logging
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Tuple
# CHỈNH SỬA BẮT BUỘC: Sử dụng Decimal để bảo vệ tính chính xác của số liệu tài chính
from decimal import Decimal 

from pymongo import MongoClient
# Sửa lại đường dẫn import tương đối chuẩn theo cấu trúc src/
from config.settings import settings

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        """Khởi tạo kết nối tới MongoDB để đọc dữ liệu Staging và ghi vào DLQ."""
        self.mongo_client = MongoClient(settings.mongo_uri)
        self.mongo_db = self.mongo_client[settings.MONGO_DB_NAME]
        
        # Collection nguồn (Staging)
        self.src_collection = self.mongo_db["raw_coin_prices"]
        
        # Collection cô lập (Dead Letter Queue - DLQ)
        self.dlq_collection = self.mongo_db["dlq_malformed_prices"]

    def extract_raw_data(self, batch_size: int = 100) -> Generator[Dict[str, Any], None, None]:
        """
        Sử dụng Python Generator (yield) để đọc dữ liệu từ MongoDB theo từng mẻ nhỏ,
        giúp tối ưu hóa bộ nhớ RAM ở mức hằng số, chống lỗi OOM tuyệt đối.
        """
        cursor = self.src_collection.find({}).batch_size(batch_size)
        for document in cursor:
            yield document

    def transform_and_validate(self, doc: Dict[str, Any]) -> Tuple[bool, Dict[str, Any] | None, str | None]:
        """
        MÔ-ĐUN KIỂM ĐỊNH TỐI CAO:
        Xử lý bóc tách cấu trúc lai (Variant JSONB), kiểm tra chất lượng dữ liệu tài chính.
        """
        try:
            raw_id = str(doc["_id"])
            coin_id = doc["coin_id"]
            fetched_at = doc["fetched_at"]
            raw_payload = doc["raw_data"]

            # KỊCH BẢN 1: Dữ liệu từ tài liệu Snapshot đơn lẻ (/coins/{id})
            if "market_data" in raw_payload:
                market_data = raw_payload["market_data"]
                current_price = market_data.get("current_price", {})
                
                raw_usd = current_price.get("usd")
                raw_vnd = current_price.get("vnd")
                
                if raw_usd is None or raw_vnd is None:
                    return False, None, "Price metrics (usd/vnd) are null in market_data"

                usd_price = Decimal(str(raw_usd))
                vnd_price = Decimal(str(raw_vnd))
                market_cap = Decimal(str(market_data.get("market_cap", {}).get("usd", 0)))
                total_volume = Decimal(str(market_data.get("total_volume", {}).get("usd", 0)))

            # KỊCH BẢN 2: Dữ liệu từ mảng Snapshot diện rộng (/coins/markets) của con Daemon tự động
            elif isinstance(raw_payload, list):
                # Vì file độc lập này quét toàn bộ bảng, ta cần xử lý thớ dữ liệu dạng mảng nếu có
                return False, None, "This module processes single-document stream. Bulk arrays should be unrolled."

            else:
                # Kịch bản API trả về lỗi hoặc thiếu các Key nền tảng
                if "current_price" not in raw_payload:
                    return False, None, "Missing core financial metrics in payload root"
                
                raw_usd = raw_payload.get("current_price")
                if raw_usd is None:
                    return False, None, "Price token is null"
                    
                usd_price = Decimal(str(raw_usd))
                vnd_price = usd_price * Decimal("25000") # Tự động quy đổi tỷ giá an toàn
                market_cap = Decimal(str(raw_payload.get("market_cap" ) or 0))
                total_volume = Decimal(str(raw_payload.get("total_volume") or 0))

            # Đóng gói bản ghi tinh luyện siêu sạch
            clean_data = {
                "staging_id": raw_id,
                "coin_id": str(coin_id),
                "price_usd": usd_price, # Kiểu Decimal giữ nguyên giá trị gốc
                "price_vnd": vnd_price, # Kiểu Decimal chính xác tuyệt đối
                "market_cap_usd": market_cap,
                "total_volume_usd": total_volume,
                "extracted_at": fetched_at
            }
            return True, clean_data, None

        except KeyError as ke:
            return False, None, f"Missing required system key: {str(ke)}"
        except (TypeError, ValueError) as te:
            return False, None, f"Data type mismatch during casting: {str(te)}"

    def isolate_to_dlq(self, original_doc: Dict[str, Any], error_reason: str):
        """Bảo vệ hiện trường lỗi: Đóng gói và cách ly bản ghi nhiễm độc vào MongoDB DLQ."""
        dlq_envelope = {
            "failed_at": datetime.now(timezone.utc),
            "original_staging_id": original_doc.get("_id"),
            "coin_id": original_doc.get("coin_id", "unknown"),
            "error_reason": error_reason,
            "corrupted_payload": original_doc.get("raw_data") 
        }
        self.dlq_collection.insert_one(dlq_envelope)
        
        logger.warning({
            "level": "WARNING",
            "component": "etl_transformer",
            "event": "data_isolated_to_dlq",
            "coin_id": original_doc.get("coin_id", "unknown"),
            "reason": error_reason
        })

    def run_etl_job(self):
        """Kích hoạt tiến trình quét và tinh luyện dữ liệu toàn cục."""
        logger.info({"level": "INFO", "component": "etl_transformer", "event": "etl_job_started"})
        
        processed_count = 0
        valid_count = 0
        invalid_count = 0

        for raw_doc in self.extract_raw_data(batch_size=100):
            processed_count += 1
            
            is_valid, clean_record, error_reason = self.transform_and_validate(raw_doc)
            
            if is_valid and clean_record:
                valid_count += 1
                logger.info({
                    "level": "INFO", 
                    "component": "etl_transformer", 
                    "event": "record_transform_success", 
                    "coin_id": clean_record["coin_id"],
                    "price_usd": str(clean_record["price_usd"]) # Ép chuỗi khi log để tránh lỗi format JSON
                })
            else:
                invalid_count += 1
                self.isolate_to_dlq(raw_doc, error_reason)

        logger.info({
            "level": "INFO",
            "component": "etl_transformer",
            "event": "etl_job_finished",
            "total_processed": processed_count,
            "valid_records": valid_count,
            "isolated_records_dlq": invalid_count
        })

if __name__ == "__main__":
    transformer = DataTransformer()
    transformer.run_etl_job()