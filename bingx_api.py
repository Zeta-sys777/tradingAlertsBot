# bingx_api.py

import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from config import BINGX_API_KEY, BINGX_API_SECRET, BINGX_BASE_URL


class BingXAPI:
    """
    Клиент для работы с BingX Swap (фьючерсы) через REST API.
    """

    def __init__(self):
        if not BINGX_API_KEY or not BINGX_API_SECRET:
            raise ValueError("BingX API ключи не заданы. Проверь .env и config.")
        self.api_key = BINGX_API_KEY
        self.api_secret = BINGX_API_SECRET.encode()
        self.base_url = BINGX_BASE_URL.rstrip("/")

    def _sign(self, params: dict) -> str:
        """
        Подписать запрос по правилам BingX: HMAC SHA256 от query string.
        """
        query = urlencode(params)
        return hmac.new(
            self.api_secret,
            query.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _headers(self) -> dict:
        return {
            "X-BX-APIKEY": self.api_key,
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> dict:
        """
        Базовый GET-запрос к BingX с подписью.
        """
        if params is None:
            params = {}

        params["timestamp"] = int(time.time() * 1000)
        signature = self._sign(params)
        params["signature"] = signature

        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_futures_balance(self) -> dict:
        """
        Получить баланс фьючерсного (swap) аккаунта.
        GET /openApi/swap/v2/user/balance
        """
        path = "/openApi/swap/v2/user/balance"
        data = self._get(path, params={})
        return data

    def get_futures_positions(self) -> dict:
        """
        Получить открытые позиции по фьючерсам (swap).
        GET /openApi/swap/v2/user/positions
        """
        path = "/openApi/swap/v2/user/positions"
        data = self._get(path, params={})
        return data

    def get_positions_dict(self) -> dict:
        """
        Возвращает позиции в виде словаря: {(symbol, side): position_data}.
        Удобно для сравнения состояний.
        """
        positions_raw = self.get_futures_positions()
        if positions_raw.get("code") != 0 or "data" not in positions_raw:
            return {}

        positions = positions_raw.get("data", [])
        result = {}

        for pos in positions:
            symbol = pos.get("symbol")
            side = pos.get("positionSide")
            if symbol and side:
                result[(symbol, side)] = pos

        return result

    def get_trade_orders(self, symbol: str, side: str | None = None) -> dict:
        """
        Получить ордера по символу (и опционально по стороне).
        Эндпоинт: GET /openApi/swap/v2/trade/allOrders
        """
        path = "/openApi/swap/v2/trade/allOrders"
        params: dict = {
            "symbol": symbol,
            "limit": 100,
        }
        data = self._get(path, params=params)

        if side is not None and data.get("code") == 0 and isinstance(data.get("data"), list):
            filtered = []
            for order in data["data"]:
                order_side = order.get("positionSide") or order.get("side")
                if order_side is None:
                    filtered.append(order)
                else:
                    if str(order_side).upper().startswith(side.upper()[0]):
                        filtered.append(order)
            data["data"] = filtered

        return data

    def get_open_orders(self, symbol: str | None = None) -> dict:
        """
        Получить текущие открытые ордера (включая TP/SL).
        Часто эндпоинт: GET /openApi/swap/v2/trade/openOrders
        (проверь в своей доке, путь можно поправить при необходимости).
        """
        path = "/openApi/swap/v2/trade/openOrders"
        params: dict = {}
        if symbol:
            params["symbol"] = symbol
        params["limit"] = 100
        data = self._get(path, params=params)
        return data

    def get_tp_sl_for_position(self, symbol: str, side: str) -> dict:
        """
        Попытаться найти TP/SL по позиции через открытые ордера.

        Возвращает:
        {
            "tp": float | None,
            "sl": float | None,
        }
        """
        result = {"tp": None, "sl": None}

        try:
            data = self.get_open_orders(symbol)
        except Exception as e:
            print(f"⚠️ get_tp_sl_for_position: error fetching open orders: {e}")
            return result

        if data.get("code") != 0 or not isinstance(data.get("data"), list):
            return result

        for order in data["data"]:
            o_symbol = order.get("symbol")
            if o_symbol != symbol:
                continue

            order_side = (order.get("positionSide") or order.get("side") or "").upper()
            if side.upper() not in order_side and order_side != "":
                continue

            otype = (order.get("type") or "").upper()
            trigger_price = order.get("stopPrice") or order.get("price") or order.get("triggerPrice")
            if trigger_price is None:
                continue

            try:
                trigger_price_f = float(trigger_price)
            except Exception:
                continue

            # Примитивная эвристика:
            # - тип TAKE_PROFIT/TAKE_PROFIT_MARKET → TP
            # - тип STOP/STOP_MARKET → SL
            if "TAKE_PROFIT" in otype:
                result["tp"] = trigger_price_f
            elif "STOP" in otype:
                result["sl"] = trigger_price_f

        return result


# ===== Глобальный экземпляр и упрощённые обёртки =====

_bingx_client: BingXAPI | None = None


def _get_client() -> BingXAPI:
    global _bingx_client
    if _bingx_client is None:
        _bingx_client = BingXAPI()
    return _bingx_client


def get_futures_balance() -> dict:
    client = _get_client()
    return client.get_futures_balance()


def get_futures_positions() -> dict:
    client = _get_client()
    return client.get_futures_positions()


def get_positions_dict() -> dict:
    client = _get_client()
    return client.get_positions_dict()


def get_trade_orders(symbol: str, side: str | None = None) -> list:
    client = _get_client()
    data = client.get_trade_orders(symbol, side)
    if data.get("code") == 0 and isinstance(data.get("data"), list):
        return data["data"]
    return []


def get_tp_sl_for_position(symbol: str, side: str) -> dict:
    """
    Упрощённая обёртка для получения TP/SL по позиции.
    """
    client = _get_client()
    return client.get_tp_sl_for_position(symbol, side)
