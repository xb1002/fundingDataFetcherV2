# core/base_adapter.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from .dto import OHLCVRequestParams, FundingRequestParams, CandleType, FundingRecordType
import requests
import time
import logging
import json

logging.basicConfig(level=logging.INFO)

class ExchangeAdapter(ABC):
    def __init__(self):
        self.session = requests.Session()

    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None, 
                    headers: Optional[Dict[str, str]] = None,
                    max_retries: int = 3, timeout: float = 5.0) -> Dict:
        """
        发送HTTP请求（带重连机制）
        
        Args:
            url: 请求URL
            params: 请求参数
            headers: 请求头
            
        Returns:
            Optional[Dict]: 响应数据，失败时返回None
        """
        logger = logging.getLogger(self.__class__.__name__)
        if params is None:
            params = {}
        if headers is None:
            headers = {}
            
        for attempt in range(max_retries + 1):
            try:
                logger.debug(f"发送请求 (尝试 {attempt + 1}/{max_retries + 1}): {url}")
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=headers, 
                    timeout=timeout
                )
                response.raise_for_status()
                
                data = response.json()
                logger.debug(f"请求成功: {url}")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    raise Exception("请求多次失败，已放弃。")
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败: {e}")
                raise Exception("响应不是有效的JSON格式。")
        
        raise Exception("请求失败，未能获取响应。")

    @abstractmethod
    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        ...

    @abstractmethod
    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        ...

    @abstractmethod
    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        ...

    @abstractmethod
    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        ...
