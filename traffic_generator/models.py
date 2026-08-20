from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import (
    DateTime,
    Enum as SqlEnum,
    ForeignKey,
    Numeric,
    String,
    func,
)

from sqlalchemy.orm import Mapped, mapped_column

from traffic_generator.database import Base

class PaymentStatus(str, Enum):
    INITIATED = "INITIATED"
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"

class Payment(Base):
    __tablename__ = "payments"

    payment_id: Mapped[str] = mapped_column(
        String(50),
        primary_key = True
    )

    clearing_id: Mapped[str | None] = mapped_column(
        String(50),
        nullable = True,
        index = True
    )

    sender: Mapped[str] = mapped_column(
        String(100),
        nullable = False
    )

    receiver: Mapped[str] = mapped_column(
        String(100),
        nullable = False
    )

    amount: Mapped[Decimal] = mapped_column(
        Numeric(18,2),
        nullable = False
    )

    currency: Mapped[str] = mapped_column(
        String(3),
        nullable = False
    )

    status: Mapped[PaymentStatus] = mapped_column(
        SqlEnum(
            PaymentStatus,
            native_enum = False
        ),
        nullable = False,
        default = PaymentStatus.INITIATED
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default = func.now(),
        nullable = False
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable = False
    )

class PaymentStatusHistory(Base):
    __tablename__ = "payment_status_history"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    payment_id: Mapped[str] = mapped_column(
        ForeignKey("payments.payment_id"),
        nullable=False,
        index=True
    )

    status: Mapped[PaymentStatus] = mapped_column(
        SqlEnum(
            PaymentStatus,
            native_enum=False
        ),
        nullable=False
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
