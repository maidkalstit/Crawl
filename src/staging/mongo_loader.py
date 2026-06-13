import logging
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError

# Import cấu hình tập trung
from config.settings import settings

logger = logging.getLogger(__name__)

class MongoStagingLoader:
    def __init__(self):
        """
        Khởi tạo và cấu hình Connection Pool tới MongoDB Staging.
        """
        try:
            # LÝ DO CHỈNH SỬA: Cấu hình các tham số Pooling và Write Concern tối ưu cho SRE
            self.client = MongoClient(
                settings.mongo_uri,
                maxPoolSize=50,            # Giới hạn tối đa 50 kết nối tái sử dụng
                minPoolSize=10,            # Luôn giữ duy trì tối thiểu 10 kết nối ngầm
                serverSelectionTimeoutMS=5000, # Giới hạn 5 giây để tìm thấy DB, quá hạn thì báo sập
                retryWrites=True           # Tự động ghi lại 1 lần nếu gặp lỗi mạng tạm thời
            )
            self.db = self.client[settings.MONGO_DB_NAME]
            
            # Chọn collection lưu trữ dữ liệu giá coin thô
            self.collection = self.db["raw_coin_prices"]
            
            # Kích hoạt kiểm tra kết nối ngay khi khởi tạo
            self.client.admin.command('ping')
            
            # TỰ ĐỘNG KHỞI TẠO INDEX (CHỈ THỰC HIỆN 1 LẦN BAN ĐẦU)
            self._ensure_indexes()
            
        except (ConnectionFailure, ConfigurationError) as e:
            logger.critical({"event": "mongo_connection_failed", "error": str(e)})
            raise RuntimeError(f"Không thể thiết lập kết nối tới MongoDB Staging: {e}")

    def _ensure_indexes(self):
        """Tạo chỉ mục (Index) ngầm để tối ưu hóa tốc độ truy vấn cho tầng ETL phía sau."""
        # Compound Index: Kết hợp mã coin và thời gian cào để ETL quét theo cụm cực nhanh
        self.collection.create_index([("coin_id", 1), ("fetched_at", -1)])
        logger.info({"event": "mongo_index_verified", "message": "Đã tối ưu hóa Index thành công."})

    def load_raw_snapshot(self, coin_id: str, raw_json: dict) -> str:
        """
        Nạp nguyên bản cục JSON thô vào MongoDB kèm theo các Metadata định danh hệ thống.
        """
        if not raw_json:
            logger.warning({"event": "mongo_load_empty", "coin_id": coin_id, "message": "Dữ liệu JSON rỗng, từ chối nạp."})
            return ""

        # PHƯƠNG THỨC HOẠT ĐỘNG: Bọc dữ liệu thô (Variant) bằng một Document bọc ngoài (Envelope Pattern)
        document = {
            "coin_id": coin_id,
            "fetched_at": datetime.now(timezone.utc), # Trục thời gian chuẩn quốc tế UTC
            "pipeline_version": "1.0.0",
            "raw_data": raw_json                      # ĐÂY CHÍNH LÀ CỘT VARIANT TỰ NHIÊN
        }

        # Thực thi ghi xuống với cấu hình Write Concern an toàn cao
        # w=1: Chờ 1 node Master ghi xong thành công vào RAM mới phản hồi cho Python
        coll_with_write_concern = self.collection.with_options(
            write_concern=self.collection.write_concern.__class__(w=1, j=True) # j=True: Bắt buộc ghi vào nhật ký Journal trên ổ đĩa
        )
        
        result = coll_with_write_concern.insert_one(document)
        
        logger.info({
            "event": "mongo_load_success",
            "coin_id": coin_id,
            "inserted_id": str(result.inserted_id)
        })
        
        return str(result.inserted_id)

# ── ĐOẠN CODE KIỂM THỬ TÍCH HỢP (INTEGRATION TEST) ──────────────────────
if __name__ == "__main__":
    # Đảm bảo anh đã cài thư viện: pip install pymongo
    print("── [TESTING MONGO LOADER] ──")
    try:
        loader = MongoStagingLoader()
        
        # Giả lập một cục dữ liệu biến động cấu trúc cào được từ API CoinGecko
        mock_api_data = {
            "name": "Bitcoin",
            "symbol": "btc",
            "market_data": {
                "current_price": {"usd": 65000, "vnd": 1650000000},
                "high_24h": {"usd": 66000}
            }
        }
        
        inserted_id = loader.load_raw_snapshot(coin_id="bitcoin", raw_json=mock_api_data)
        print(f"Ghi dữ liệu thành công! Document ID tại MongoDB: {inserted_id}")
        
    except Exception as e:
        print(f"Lỗi kiểm thử hệ thống: {e}")