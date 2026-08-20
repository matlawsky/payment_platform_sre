import asyncio
import json
import logging
import time
import uuid
import httpx
from contextlib import asynccontextmanager
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request
)
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from traffic_generator.clearing_client import ClearingHouseClient
from traffic_generator.database import (
    AsyncSessionLocal,
    Base,
    engine,
    get_db
)
from traffic_generator.models import (
    Payment,
    PaymentStatus,
    PaymentStatusHistory
)
from traffic_generator.schemas import (
    PaymentCreate,
    PaymentResponse,
    StatusHistoryResponse
)

# LOGGING
class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "timestamp": self.formatTime(
                record,
                "%Y-%m-%dT%H:%M:%S"
            ),
            "level": record.levelname,
            "service": "payment-api",
            "message": record.getMessage()
        }

        fields = [
            "correlation_id",
            "payment_id",
            "clearing_id",
            "method",
            "endpoint",
            "status_code",
            "latency_ms",
            "payment_status",
            "error_type"
        ]
        for field in fields:
            value = getattr(
                record, field, None
            )
            if value is not None:
                data[field] = value
        return json.dumps(data)

logger = logging.getLogger(
    "traffic_generator"
)

logger.setLevel(
    logging.INFO
)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.handlers.clear()
logger.addHandler(handler)

# DATABASE

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all
        )
    logger.info("Payment API for traffic generator started")

    yield

    await engine.dispose()

app = FastAPI(
    title="Traffic Generator Payment API",
    version="1.0.0",
    lifespan=lifespan
)

clearing_client = (ClearingHouseClient())

# CORRELATION ID + REQUEST LOGGING
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    correlation_id = (
        request.headers.get("X-Correlation-ID")
    or str(uuid.uuid4())
    )

    request.state.correlation_id = (correlation_id)

    start = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception as error:
        latency_ms = (
            time.perf_counter() - start
        ) * 1000

        logger.exception(
            "Unhandled application error",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "endpoint": request.url.path,
                "status_code": 500,
                "latency_ms": round(latency_ms, 2),
                "error_type": type(error).__name__
            }
        )
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "correlation_id": correlation_id,
            },
            headers={
                "X-Correlation-ID":correlation_id
            },
        )

    latency_ms = (
        time.perf_counter() - start
    ) * 1000

    logger.info(
        "HTTP request completed",
        extra={
            "correlation_id": correlation_id,
            "method": request.method,
            "endpoint": request.url.path,
            "status_code": response.status_code,
            "latency_ms": round(latency_ms, 2)
        }
    )

    response.headers["X-Correlation-ID"] = correlation_id

    return response

async def update_payment_status(
    db: AsyncSession,
    payment: Payment,
    new_status: PaymentStatus
):
    if payment.status == new_status:
        return

    payment.status = new_status
    history = PaymentStatusHistory(
        payment_id=payment.payment_id,
        status= new_status
    )
    db.add(history)
    await db.commit()
    await db.refresh(payment)

# async clearing house processing

async def process_payment(
    payment_id: str,
    correlation_id: str
):
    """
    Process is workinf after returning to client http 202
    Flow:
    INITIATED -> sent to Clearing House -> PENDING 
    -> poll Clearing House - > COMPLETED/REJECTED/CANCELLED
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Payment).where(
                Payment.payment_id == payment_id
            )
        )
        payment = (
            result
            .scalars()
            .first()
        )

        if payment is None:
            return

        try:
            # send payment to clearinghouse
            clearing_response = (
                await clearing_client.submit_payment(
                    payment_id=payment.payment_id,
                    sender=payment.sender,
                    receiver=payment.receiver,
                    amount=payment.amount,
                    currency=payment.currency,
                    correlation_id=correlation_id
                )
            )
            payment.clearing_id = (
                clearing_response["clearing_id"]
            )
            await update_payment_status(
                db, payment, PaymentStatus.PENDING
            )
            logger.info(
                "Payment accepted at the clearing house.",
                extra={
                    "correlation_id": correlation_id,
                    "payment_id": payment.payment_id,
                    "clearing_id": payment.clearing_id,
                    "payment_status": PaymentStatus.PENDING.value
                },
            )

            # polling

            max_attempts = 30
            poll_interval = 0.5

            for attempt in range(max_attempts):
                await asyncio.sleep(poll_interval)
                # payment could have been cancelled
                await db.refresh(payment)
                if payment.status in {
                    PaymentStatus.COMPLETED,
                    PaymentStatus.REJECTED,
                    PaymentStatus.CANCELLED
                }:
                    return

                response = (
                    await clearing_client.get_payment_status(
                        clearing_id=payment.clearing_id,
                        correlation_id=correlation_id
                    )
                )

                clearing_status = (
                    PaymentStatus(response['status'])
                )

                if clearing_status == (PaymentStatus.PENDING):
                    continue

                await update_payment_status(db, payment, clearing_status)

                logger.info("Payment reached terminal status",
                extra={
                    "correlation_id":correlation_id,
                    "payment_id": payment.payment_id,
                    "clearing_id": payment.clearing_id,
                    "payment_status": clearing_status.value
                })
                return

            logger.warning(
                "Payment still pending after polling timeout",
                extra={
                    "correlation_id": correlation_id,
                    "payment_id": payment.payment_id,
                    "clearing_id": payment.clearing_id,
                    "payment_status": PaymentStatus.PENDING.value
                }
            )
        except (
            httpx.RequestError,
            httpx.HTTPStatusError,
        ) as error:
            logger.exception(
                "Clearing house communication error",
                extra={
                    "correlation_id": correlation_id,
                    "payment_id": payment.payment_id,
                    "clearing_id": payment.clearing_id,
                    "error_type": type(error).__name__
                }
            )
            # here automatically payment IS NOT set as rejected
            # lack of response from clearign house
            # does not mean rejection of payment
            # functionality of retry/reconciliation might be added later

# create payment
@app.post(
    "/payments",
    response_model=PaymentResponse,
    status_code=202,
)
async def create_payment(
    payment_data: PaymentCreate,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    payment_id = (
        "PAY-"+uuid.uuid4().hex[:12].upper()
    )
    payment = Payment(
        payment_id=str(payment_id),
        sender=payment_data.sender,
        receiver=payment_data.receiver,
        amount=payment_data.amount,
        currency=payment_data.currency,
        status=PaymentStatus.INITIATED
    )
    db.add(payment)
    db.add(PaymentStatusHistory(
        payment_id=payment_id,
        status=PaymentStatus.INITIATED
    ))

    await db.commit()
    await db.refresh(payment)

    logger.info(
        "Payment initiated",
        extra={
            "correlation_id": request.state.correlation_id,
            "payment_id": payment_id,
            "payment_status": PaymentStatus.INITIATED.value
        }
    )

    asyncio.create_task(process_payment(
        payment_id, request.state.correlation_id
    ))
    return payment

# get payment
@app.get("/payments/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id:str,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(Payment).where(Payment.payment_id==payment_id)
    )
    payment = (
        result
        .scalars()
        .first()
    )

    if payment is None:
        raise HTTPException(
            status_code=404,
            detail="Payment not found"
        )
    return payment

# payment history
@app.get("/payments/{payment_id}/history", response_model=list[StatusHistoryResponse])
async def get_payment_history(
    payment_id: str,
    db: AsyncSession = Depends(get_db)
):
    result= await db.execute(
        select(PaymentStatusHistory)
        .where(PaymentStatusHistory.payment_id==payment_id)
        .order_by(PaymentStatusHistory.timestamp)

    )
    return (
        result
        .scalars()
        .first()
    )

#cancel payment
@app.post("/payments/{payment_id}/cancel",response_model=PaymentResponse)
async def cancel_payment(
    payment_id: str,
    request: Request,
    db: AsyncSession = Depends(
        get_db
    ),
):
    result = await db.execute(
        select(Payment).where(Payment.payment_id== payment_id)
    )
    payment = (
        result
        .scalars()
        .first()
    )
    if payment is None:
        raise HTTPException(
            status_code=404,
            detail="Payment not found",
        )
    terminal_statuses = {
        PaymentStatus.COMPLETED,
        PaymentStatus.REJECTED,
        PaymentStatus.CANCELLED,
    }
    if payment.status in terminal_statuses:
        raise HTTPException(
            status_code=409,
            detail=(
                "Payment already reached "
                "terminal status."
            ),
        )
    # Payment was not yet sent to Clearinghouse
    if payment.clearing_id is None:
        await update_payment_status(
            db,
            payment,
            PaymentStatus.CANCELLED,
        )
        return payment

    # Payment is already on Clearing House side
    response = (
        await clearing_client.cancel_payment(
            clearing_id=payment.clearing_id,
            correlation_id=request.state.correlation_id,
        )
    )
    new_status = PaymentStatus(response["status"])
    await update_payment_status(db,payment,new_status)
    return payment

# Health
@app.get("/health")
async def health():
    return {
        "status": "UP",
        "service": "payment_api",
    }