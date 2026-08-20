from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, model_validator
from traffic_generator.models import PaymentStatus


class PaymentCreate(BaseModel):
    sender: str = Field(
        min_length=1,
        max_length=100
    )

    receiver: str = Field(
        min_length=1,
        max_length=100
    )

    amount: Decimal = Field(
        gt=0,
        decimal_places=2
    )

    currency: Literal[
        "PLN",
        "EUR",
        "USD",
        "GBP"
    ]

    @model_validator(mode="after")
    def validate_sender_and_receiver(self):
        if self.sender == self.receiver:
            raise ValueError(
                "Sender and receiver cannot be the same."
            )
        return self


class PaymentResponse(BaseModel):
    payment_id: str
    clearing_id: str | None
    sender: str
    receiver: str
    amount: Decimal
    currency: str
    status: PaymentStatus
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}


class StatusHistoryResponse(BaseModel):
    status: PaymentStatus
    timestamp: datetime
    model_config = {"from_attributes": True}