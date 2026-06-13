import pytest
from datetime import datetime, timezone
from bson import ObjectId
from src.etl.transformer import DataTransformer

@pytest.fixture
def transformer_instance():
    """Khởi tạo một instance giả lập của DataTransformer để test."""
    # Tránh kết nối thật vào Mongo khi đang chạy Unit Test
    return DataTransformer()

def test_transform_and_validate_success(transformer_instance):
    """Trường hợp 1: Thử nghiệm với một cục JSON thô hoàn hảo từ API."""
    mock_mongo_doc = {
        "_id": ObjectId(),
        "coin_id": "bitcoin",
        "fetched_at": datetime.now(timezone.utc),
        "raw_data": {
            "market_data": {
                "current_price": {"usd": 65000.0, "vnd": 1650000000.0},
                "market_cap": {"usd": 1200000000000.0},
                "total_volume": {"usd": 30000000000.0}
            }
        }
    }
    
    is_valid, clean_record, error_reason = transformer_instance.transform_and_validate(mock_mongo_doc)
    
    # Kiểm tra kết quả khẳng định (Assertions)
    assert is_valid is True
    assert clean_record["coin_id"] == "bitcoin"
    assert clean_record["price_usd"] == 65000.0
    assert error_reason is None

def test_transform_and_validate_missing_price(transformer_instance):
    """Trường hợp 2: Thử nghiệm kịch bản lỗi khi API bị khuyết thiếu trường giá (Nhiễm độc)."""
    mock_corrupted_doc = {
        "_id": ObjectId(),
        "coin_id": "ethereum",
        "fetched_at": datetime.now(timezone.utc),
        "raw_data": {
            "market_data": {
                "current_price": {
                    "usd": None, # LỖI: Giá bị Null
                    "vnd": 50000000.0
                }
            }
        }
    }
    
    is_valid, clean_record, error_reason = transformer_instance.transform_and_validate(mock_corrupted_doc)
    
    # Hệ thống bắt buộc phải nhận diện được đây là bản ghi lỗi để đẩy về DLQ
    assert is_valid is False
    assert clean_record is None
    assert "Price metrics (usd/vnd) are null" in error_reason