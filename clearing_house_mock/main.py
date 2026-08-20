import random
import time
import uuid
from decimal import Decimal
from typing import Literal
from fastapi import (
    FastAPI,
    HTTPException,
)
from pydantic import BaseModel, Field
app = FastAPI(
    title="Clearing House Mock",
    version="1.0.0",
)

# schemas
class ClearingPaymentRequest(BaseModel):
    client_payment_id: str
    sender: str
    receiver: str
    amount: Decimal = Field(gt=0)
    currency: Literal[
        "PLN",
        "EUR",
        "USD",
        "GBP",
    ]

class ClearingPaymentResponse(BaseModel):
    clearing_id: str
    client_payment_id: str
    status: str

# mock storage
payments = {}

# create payments
@app.post("/payments",response_model=ClearingPaymentResponse,status_code=202)
async def create_payment(payment:ClearingPaymentRequest):
    clearing_id = ("CLR-"+ uuid.uuid4().hex[:12].upper())
    now = time.time()
    processing_time = (random.uniform(1.0,5.0))
    # chosing the final state
    # at the moment while we get tthe payment
    # but client does not know it yet
    terminal_status = (
        random.choices(
            population=["COMPLETED","REJECTED"],
            weights=[95,5],
            k=1,
        )[0]
    )
    payments[clearing_id] = {
        "clearing_id":clearing_id,
        "client_payment_id":payment.client_payment_id,
        "sender":payment.sender,
        "receiver":payment.receiver,
        "amount":str(payment.amount),
        "currency":payment.currency,
        "received_at":now,
        "ready_at":now + processing_time,
        "terminal_status":terminal_status,
        "cancelled":False,
    }
    return {
        "clearing_id":clearing_id,
        "client_payment_id":payment.client_payment_id,
        "status":"PENDING",
    }

# get status
@app.get("/payments/{clearing_id}",response_model=ClearingPaymentResponse)
async def get_payment(clearing_id: str):
    payment = payments.get(clearing_id)
    if payment is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Clearing payment "
                "not found"
            ),
        )
    if payment["cancelled"]:
        status = "CANCELLED"

    elif time.time() >= (payment["ready_at"]):
        status = (payment["terminal_status"])
    else:
        status = "PENDING"
    return {
        "clearing_id":clearing_id,
        "client_payment_id":payment["client_payment_id"],
        "status":status,
    }

# cancel payment
@app.post("/payments/{clearing_id}/cancel",response_model=ClearingPaymentResponse)
async def cancel_payment(clearing_id: str):
    payment = payments.get(clearing_id)
    if payment is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Clearing payment "
                "not found"
            ),
        )
    # if we ended processing there is no way to cancel
    if (
        time.time()
        >= payment["ready_at"]
        and not payment["cancelled"]
    ):
        current_status = (payment["terminal_status"])
        raise HTTPException(
            status_code=409,
            detail=(
                "Payment already reached "
                f"{current_status}"
            ),
        )
    payment["cancelled"] = True
    return {
        "clearing_id":clearing_id,
        "client_payment_id":payment["client_payment_id"],
        "status":"CANCELLED",
    }

# health
@app.get("/health")
async def health():
    return {
        "status":"UP",
        "service":"clearing-house",
    }