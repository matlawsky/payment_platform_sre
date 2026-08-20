import os
import httpx

CLEARING_HOUSE_URL = os.getenv(
    "CLEARING_HOUSE_URL",
    "http://127.0.0.1:8001"
)

class ClearingHouseClient:
    def __init__(self):
        self.base_url = (CLEARING_HOUSE_URL.rstrip("/"))
        self.timeout = httpx.Timeout(5.0)

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
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/payments",
                json={
                    "client_payment_id": str(payment_id),
                    "sender": str(sender),
                    "receiver": str(receiver),
                    "amount": str(amount),
                    "currency": str(currency),
                },
                headers={
                    "X-Correlation-ID": correlation_id
                }
            )
            response.raise_for_status()
            return response.json()

    async def get_payment_status(
            self,
            *,
            clearing_id: str,
            correlation_id: str
    ) -> dict:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
            (
                f"{self.base_url}/payments/"
                f"{clearing_id}"
            ),
            headers = {
                "X-Correlation-ID":correlation_id
            },
        )
        response.raise_for_status()
        return response.json()
    
    async def cancel_payment(
            self,
            *,
            clearing_id: str,
            correlation_id: str
    ) -> dict:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
            (
                f"{self.base_url}/payments/"
                f"{clearing_id}/cancel"
            ),
            headers={
                "X-Correlation-ID": correlation_id
            }
            )
            response.raise_for_status()
            return response.json()
        
