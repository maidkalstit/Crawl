import logging
import time
from typing import List

# Import các mảnh ghép từ các phân tầng trước
from src.crawler.crawler_engine import CoinGeckoClient, TokenBucketLimiter
from src.staging.mongo_loader import MongoStagingLoader

# Thiết lập log để bám sát dấu vết vận hành của Pipeline
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class CoinGeckoDataPipeline:
    def __init__(self):
        """Khởi tạo và liên kết các thành phần hạ tầng."""
        # TẦNG 1: Khởi tạo bộ kiểm soát tốc độ chủ động và HTTP Client kháng lỗi
        self.limiter = TokenBucketLimiter(rate_per_min=28) # Giữ buffer an toàn 28 req/min
        self.client = CoinGeckoClient()
        
        # TẦNG 2: Khởi tạo bộ nạp dữ liệu thô vào vùng đệm MongoDB
        self.mongo_loader = MongoStagingLoader()

    def execute_ingestion_flow(self, target_coins: List[str]):
        """
        Phương thức điều phối chính: Vận hành vòng lặp cào và nạp dữ liệu.
        """
        pipeline_start_time = time.monotonic()
        logger.info({
            "level": "INFO",
            "component": "pipeline_orchestrator",
            "event": "pipeline_started",
            "message": f"Bắt đầu chu kỳ nạp dữ liệu cho {len(target_coins)} đồng tiền mã hóa."
        })

        success_count = 0
        
        for coin_id in target_coins:
            try:
                # 1. KIỂM SOÁT TỐC ĐỘ: Bắt bắt luồng phải xếp hàng chờ Token, tránh dập sập Rate Limit
                self.limiter.acquire()

                # 2. THU THẬP: Gọi endpoint chi tiết của từng đồng xu để lấy cục JSON cực kỳ giàu thông tin (Rich Payload)
                # Sử dụng endpoint: /coins/{id} thay vì /simple/price để khai thác tối đa sức mạnh của cột VARIANT
                endpoint = f"/coins/{coin_id}"
                params = {
                    "localization": "false",
                    "tickers": "false",
                    "market_data": "true",
                    "community_data": "false",
                    "developer_data": "false",
                    "sparkline": "false"
                }
                
                raw_data = self.client.get(endpoint, params=params)

                # 3. NẠP STAGING: Đẩy ngay lập tức cục JSON thô vào MongoDB
                mongo_id = self.mongo_loader.load_raw_snapshot(coin_id=coin_id, raw_json=raw_data)
                
                if mongo_id:
                    success_count += 1
                    
            except Exception as e:
                # CÔ LẬP LỖI (Fault Isolation): Lỗi của coin này (ví dụ: gõ sai id coin) 
                # không được phép làm sập toàn bộ chu kỳ cào của các coin khác phía sau.
                logger.error({
                    "level": "ERROR",
                    "component": "pipeline_orchestrator",
                    "event": "coin_ingestion_failed",
                    "coin_id": coin_id,
                    "message": f"Thất bại khi xử lý đồng xu {coin_id}. Chi tiết: {str(e)}"
                })
                continue  # Bỏ qua coin lỗi, tiếp tục vòng lặp chu kỳ

        # Kết thúc chu kỳ, đo lường tổng độ trễ để phục vụ SRE Monitor sau này
        total_latency = time.monotonic() - pipeline_start_time
        logger.info({
            "level": "INFO",
            "component": "pipeline_orchestrator",
            "event": "pipeline_finished",
            "success_rate": f"{(success_count / len(target_coins)) * 100:.2f}%",
            "total_latency_seconds": round(total_latency, 2),
            "message": "Hoàn thành chu kỳ nạp dữ liệu thô vào Staging."
        })

# ── ĐOẠN CODE KÍCH HOẠT VẬN HÀNH TOÀN LUỒNG (MAIN ENTRYPOINT) ────────────────
if __name__ == "__main__":
    # Danh sách các đồng xu chiến lược cần theo dõi dữ liệu
    WATCH_LIST = ["bitcoin", "ethereum", "solana", "cardano", "ripple"]
    
    # Khởi chạy pipeline điều phối
    orchestrator = CoinGeckoDataPipeline()
    orchestrator.execute_ingestion_flow(target_coins=WATCH_LIST)