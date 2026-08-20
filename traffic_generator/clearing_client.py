import os
import time
import httpx

from .metrics import (
    CLEARING_HOUSE_ERRORS_TOTAL,
    CLEARING_HOUSE_REQUEST_DURATION_SECONDS,
    CLEARING_HOUSE_REQUESTS_TOTAL,
    CLEARING_HOUSE_TIMEOUTS_TOTAL
)

CLEARING_HOUSE_URL = os.getenv(
    "CLEARING_HOUSE_URL",
    "http://127.0.0.1:8001"
)

class ClearingHouseClient:
    def __init__(self):
        self.base_url = (CLEARING_HOUSE_URL.rstrip("/"))
        self.timeout = httpx.Timeout(5.0)

    async def _request(
        self,
        *,
        method: str,
        path: str,
        operation: str,
        correlation_id: str,
        json_data: dict | None = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        start_time = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    json=json_data,
                    headers={
                        "X-Correlation-ID":correlation_id
                    },
                )
            duration = (time.perf_counter()- start_time)
            CLEARING_HOUSE_REQUEST_DURATION_SECONDS.labels(
                operation=operation
            ).observe(duration)
            CLEARING_HOUSE_REQUESTS_TOTAL.labels(
                operation=operation,
                status_code=str(response.status_code),
            ).inc()
            if response.is_error:
                CLEARING_HOUSE_ERRORS_TOTAL.labels(
                    operation=operation,
                    error_type="http_error",
                ).inc()
                print()
                print("CLEARING HOUSE ERROR")
                print(f"URL: {url}")
                print(
                    f"Status: "
                    f"{response.status_code}"
                )
                print(
                    f"Response: "
                    f"{response.text}"
                )
                print()
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException:
            duration = (time.perf_counter()- start_time)
            CLEARING_HOUSE_REQUEST_DURATION_SECONDS.labels(
                operation=operation
            ).observe(duration)
            CLEARING_HOUSE_TIMEOUTS_TOTAL.labels(
                operation=operation
            ).inc()
            CLEARING_HOUSE_REQUESTS_TOTAL.labels(
                operation=operation,
                status_code="timeout",
            ).inc()
            CLEARING_HOUSE_ERRORS_TOTAL.labels(
                operation=operation,
                error_type="timeout",
            ).inc()
            raise

        except httpx.RequestError:
            duration = (time.perf_counter()- start_time)
            CLEARING_HOUSE_REQUEST_DURATION_SECONDS.labels(
                operation=operation
            ).observe(duration)
            CLEARING_HOUSE_REQUESTS_TOTAL.labels(
                operation=operation,
                status_code="network_error",
            ).inc()
            CLEARING_HOUSE_ERRORS_TOTAL.labels(
                operation=operation,
                error_type="network_error",
            ).inc()
            raise

    async def submit_payment(
            self,
            *,
            payment_id: str,
            sender: str,
            receiver: str,
            amount: str,
            currency: str,
            correlation_id: str
    ) -> dict:
        return await self._request(
            method="POST",
            path="/payments",
            operation="submit_payment",
            correlation_id=correlation_id,
            json_data={
                "client_payment_id":payment_id,
                "sender":sender,
                "receiver":receiver,
                "amount":amount,
                "currency":currency,
            }
        )

    async def get_payment_status(
            self,
            *,
            clearing_id: str,
            correlation_id: str
    ) -> dict:
        return await self._request(
            method="GET",
            path=(
                f"/payments/"
                f"{clearing_id}"
            ),
            operation="get_status",
            correlation_id=correlation_id,
        )
    
    async def cancel_payment(
            self,
            *,
            clearing_id: str,
            correlation_id: str
    ) -> dict:
        return await self._request(
            method="POST",
            path=(
                f"/payments/"
                f"{clearing_id}/cancel"
            ),
            operation="cancel_payment",
            correlation_id=correlation_id,
        )
        
