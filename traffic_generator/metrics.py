from prometheus_client import Counter, Gauge, Histogram

# HTTP / API
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests handled by Payment API",
    ["method","endpoint","status_code"]
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Payment API HTTP request duration",
    ["method","endpoint"],
    buckets=( 0.01, 0.025, 0.05,0.1,0.25, 0.5,1,2,5,10),
)

HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Current number of HTTP requests being processed",
)

# PAYMENT BUSINESS METRICS
PAYMENTS_INITIATED_TOTAL = Counter(
    "payments_initiated_total",
    "Total number of initiated payments",
)
PAYMENTS_COMPLETED_TOTAL = Counter(
    "payments_completed_total",
    "Total number of payments completed",
)
PAYMENTS_REJECTED_TOTAL = Counter(
    "payments_rejected_total",
    "Total number of payments rejected",
)
PAYMENTS_CANCELLED_TOTAL = Counter(
    "payments_cancelled_total",
    "Total number of payments cancelled",
)
PAYMENTS_PENDING = Gauge(
    "payments_pending",
    "Current number of payments in PENDING state",
)
PAYMENTS_CURRENT = Gauge(
    "payments_current",
    "Current number of payments by status",
    ["status"],
)

PAYMENT_PROCESSING_DURATION_SECONDS = Histogram(
    "payment_processing_duration_seconds",
    (
        "Time between INITIATED and terminal payment status "
        "COMPLETED / REJECTED / CANCELLED"
    ),
    ["terminal_status"],
    buckets=(0.5,1, 2,3,5,10,15,30,60,
    ),
)

# CLEARING HOUSE
CLEARING_HOUSE_REQUESTS_TOTAL = Counter(
    "clearing_house_requests_total",
    "Requests sent from Payment API to Clearing House",
    ["operation", "status_code"],
)

CLEARING_HOUSE_REQUEST_DURATION_SECONDS = Histogram(
    "clearing_house_request_duration_seconds",
    "Clearing House request duration observed by Payment API",
    ["operation"],
    buckets=(0.05, 0.1,0.25,0.5,1,2,3,5, 10,30),
)


CLEARING_HOUSE_TIMEOUTS_TOTAL = Counter(
    "clearing_house_timeouts_total",
    "Total Clearing House timeouts",
    ["operation"],
)

CLEARING_HOUSE_ERRORS_TOTAL = Counter(
    "clearing_house_errors_total",
    "Total Clearing House communication errors",
    ["operation","error_type"],
)

# DATABASE
DB_QUERY_DURATION_SECONDS = Histogram(
    "db_query_duration_seconds",
    "Database operation duration",
    ["operation"],
    buckets=(0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2, 5),
)

DB_ERRORS_TOTAL = Counter( "db_errors_total","Database errors",["operation"])