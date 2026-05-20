import zmq
import msgpack
import logging
import time
from typing import Dict, Any, Optional

# Configure industrial-grade logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("QuantZMQClient")

class MarketDataClient:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5555", timeout_ms: int = 2000):
        """
        Initialize the ZMQ REQ client with a timeout anti-deadlock mechanism.
        
        :param endpoint: ZMQ binding address (For Linux production, 'ipc:///tmp/quant.ipc' is recommended)
        :param timeout_ms: Send/Receive timeout to prevent REQ-REP deadlock
        """
        self.endpoint = endpoint
        self.timeout_ms = timeout_ms
        self.context = zmq.Context()
        self.socket: Optional[zmq.Socket] = None
        self._connect()

    def _connect(self):
        """Establish or reconstruct the ZMQ Socket connection."""
        if self.socket:
            # Drop pending messages immediately upon close to avoid hanging
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            
        self.socket = self.context.socket(zmq.REQ)
        
        # Enforce timeouts: the core of the bulletproof design 
        # to prevent the main trading thread from hanging indefinitely.
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        self.socket.connect(self.endpoint)
        logger.debug(f"Connected to ZMQ endpoint: {self.endpoint}")

    def send_market_slice(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Serialize market data, send it to the Julia engine, and wait for trading signals.
        """
        try:
            # 1. MessagePack binary serialization (ultra-low latency)
            payload = msgpack.packb(data, use_bin_type=True)
            
            # 2. Send request
            self.socket.send(payload)
            
            # 3. Block and wait for response (Safeguarded by RCVTIMEO)
            reply = self.socket.recv()
            
            # 4. Deserialize response
            signal = msgpack.unpackb(reply, raw=False)
            return signal

        except zmq.error.Again:
            logger.error(f"ZMQ Timeout: Julia engine failed to respond within {self.timeout_ms}ms. Rebuilding socket.")
            # Reset the state machine to prevent REQ/REP state desynchronization
            self._connect()  
            return None
        except Exception as e:
            logger.error(f"Unexpected IPC communication error: {str(e)}", exc_info=True)
            return None

    def close(self):
        """Gracefully clean up resources."""
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
        self.context.term()
        logger.info("ZMQ Client terminated.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Local testing entry point
if __name__ == "__main__":
    # Mock minimalist Orderbook and K-line slice fetched from CCXT
    mock_market_slice = {
        "timestamp": int(time.time() * 1000),
        "ticker": "BTC/USDT",
        "last_price": 65432.10,
        "volume_24h": 1500.5,
        "orderbook": {
            "bids": [[65430.0, 1.2], [65425.0, 0.5]],
            "asks": [[65435.0, 2.1], [65440.0, 0.8]]
        }
    }

    logger.info("Starting Python Market Data Routing Node...")
    with MarketDataClient() as client:
        for i in range(3):
            logger.info(f"===> Sending market slice #{i+1}")
            mock_market_slice["timestamp"] = int(time.time() * 1000)
            
            start_time = time.perf_counter()
            signal = client.send_market_slice(mock_market_slice)
            latency = (time.perf_counter() - start_time) * 1000  # Calculate Round-Trip Time (RTT) in ms
            
            if signal:
                logger.info(f"<=== Received trading signal: {signal} (RTT: {latency:.2f} ms)")
            time.sleep(1)