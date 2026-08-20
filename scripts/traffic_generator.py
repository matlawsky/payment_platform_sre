import argparse
import asyncio
import random
import statistics
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
import httpx

# CONSTANTS
TERMINAL_STATUSES = {
    "COMPLETED",
    "REJECTED",
    "CANCELLED",
}

VALID_PAYMENT_STATUSES = {
    "INITIATED",
    "PENDING",
    "COMPLETED",
    "REJECTED",
    "CANCELLED",
}

# STATISTICS
@dataclass
class TrafficStats:
    total_requests: int = 0
    successful_requests: int = 0
    client_errors: int = 0
    server_errors: int = 0
    connection_errors: int = 0
    post_requests: int = 0
    get_requests: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    status_codes: Counter = field(default_factory=Counter)

    def record_request(
        self,
        *,
        method: str,
        status_code: int | None,
        latency_ms: float,
    ) -> None:
        self.total_requests += 1
        self.latencies_ms.append(latency_ms)
        if method == "POST":
            self.post_requests += 1
        elif method == "GET":
            self.get_requests += 1
        if status_code is None:
            self.connection_errors += 1
            return

        self.status_codes[status_code] += 1
        if 200 <= status_code < 400:
            self.successful_requests += 1
        elif 400 <= status_code < 500:
            self.client_errors += 1
        elif status_code >= 500:
            self.server_errors += 1

stats = TrafficStats()

# PAYMENT STATE
# IDs of all payments created by the generator
created_payment_ids: list[str] = []
# Recent known state of every payment
payment_statuses: dict[str, str] = {}

# PAYMENT GENERATION
def generate_payment() -> dict:
    """
    Tworzy losową płatność zgodną ze schematem
    Payment API.
    """
    sender = (f"CUST-{random.randint(1000, 9999)}")
    receiver = sender
    while receiver == sender:
        receiver = (f"CUST-{random.randint(1000, 9999)}")
    amount = round(random.uniform(10,20_000),2)
    currency = random.choice(["PLN","EUR","USD","GBP"])

    return {
        "sender": sender,
        "receiver": receiver,
        "amount": amount,
        "currency": currency,
    }

# TRAFFIC PROFILES
def get_current_rps(
    *,
    profile: str,
    base_rps: int,
    elapsed: float,
    duration: int,
) -> int:
    """
    Defines recent RPS depending on the profile used
    """
    if profile == "normal":
        return base_rps

    # SPIKE
    if profile == "spike":
        progress = (elapsed / duration)
        #scenario: 30% of the time normal traffic
        # next 20% of the time we get RPS x5
        #return to normal for the rest
        # Przez 30% czasu normalny ruch.
        if (0.30 <= progress <= 0.50):
            return base_rps * 5
        return base_rps

    # STRESS
    if profile == "stress":
        progress = min(elapsed / duration,1.0)
        # from 1x to 5x base RPS.
        multiplier = (1+ 4 * progress)
        return max(1,int(base_rps* multiplier))

    raise ValueError(
        f"Unknown traffic profile: {profile}"
    )

# HTTP ERROR DEBUGGING
def print_http_error(
    *,
    method: str,
    url: str,
    status_code: int,
    response_body: str,
) -> None:
    print()
    print("=" * 70)
    print("HTTP REQUEST FAILED")
    print("=" * 70)
    print(f"Method:      {method}")
    print(f"URL:         {url}")
    print(f"Status code: {status_code}")
    print(f"Response:    {response_body}")
    print("=" * 70)
    print()


# CREATE PAYMENT
async def create_payment(*,client: httpx.AsyncClient,base_url: str) -> None:
    payload = generate_payment()
    correlation_id = str(uuid.uuid4())
    headers = {"X-Correlation-ID":correlation_id}
    url = (f"{base_url}/payments")
    start_time = (time.perf_counter())
    try:
        response = await client.post(url,json=payload,headers=headers)
        latency_ms = (time.perf_counter()- start_time) * 1000

        stats.record_request(
            method="POST",
            status_code=response.status_code,
            latency_ms=latency_ms,
        )

        # NON-2XX RESPONSE
        if not response.is_success:
            print_http_error(
                method="POST",
                url=url,
                status_code=response.status_code,
                response_body=response.text,
            )
            return

        # READ JSON
        try:
            data = response.json()
        except ValueError:
            print(
                "Payment API returned "
                "invalid JSON:"
            )
            print(response.text)
            return

        payment_id = data.get("payment_id")
        payment_status = data.get("status")
        if payment_id:
            created_payment_ids.append(payment_id)
            if (payment_status in VALID_PAYMENT_STATUSES):
                payment_statuses[payment_id] = payment_status

    except httpx.RequestError as error:
        latency_ms = (time.perf_counter()- start_time) * 1000
        stats.record_request(method="POST",status_code=None,latency_ms=latency_ms)
        print(
            f"[CONNECTION ERROR] "
            f"POST {url}: "
            f"{error}"
        )

# GET PAYMENT
async def get_payment(*,client: httpx.AsyncClient,base_url: str) -> None:
    # If there is nothing we create first transaction
    if not created_payment_ids:
        await create_payment(client=client,base_url=base_url)
        return
    payment_id = random.choice(created_payment_ids)
    correlation_id = str(uuid.uuid4())
    headers = {"X-Correlation-ID":correlation_id}
    url = (
        f"{base_url}/payments/"
        f"{payment_id}"
    )
    start_time = (time.perf_counter())
    try:
        response = await client.get(url,headers=headers)
        latency_ms = (time.perf_counter()- start_time) * 1000
        stats.record_request(method="GET",status_code=response.status_code,latency_ms=latency_ms)
        if not response.is_success:
            print_http_error(method="GET",url=url,status_code=response.status_code,response_body=response.text)
            return
        try:
            data = response.json()
        except ValueError:
            print(
                f"Invalid JSON received "
                f"for {payment_id}"
            )
            return
        payment_status = data.get("status")

        if (payment_status in VALID_PAYMENT_STATUSES):
            old_status = (payment_statuses.get(payment_id))
            payment_statuses[payment_id] = payment_status

            # We show only the status change
            # not to spam on terminal
            if (old_status!= payment_status):
                print(
                    f"[STATUS CHANGE] "
                    f"{payment_id}: "
                    f"{old_status} "
                    f"-> "
                    f"{payment_status}"
                )

    except httpx.RequestError as error:
        latency_ms = (time.perf_counter()- start_time) * 1000
        stats.record_request(method="GET",status_code=None,latency_ms=latency_ms)
        print(
            f"[CONNECTION ERROR] "
            f"GET {url}: "
            f"{error}"
        )

# EXECUTE REQUEST
async def execute_request(
    *,
    client: httpx.AsyncClient,
    base_url: str,
    read_ratio: float,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    read_ratio = 0.20 means:
    80% POST /payments
    20% GET /payments/{payment_id}
    """
    async with semaphore:
        if (random.random()< read_ratio):
            await get_payment(client=client,base_url=base_url)
        else:
            await create_payment(client=client,base_url=base_url)

# SCHEDULE REQUEST
async def delayed_request(
    *,
    delay: float,
    client: httpx.AsyncClient,
    base_url: str,
    read_ratio: float,
    semaphore: asyncio.Semaphore,
) -> None:
    if delay > 0:
        await asyncio.sleep(delay)
    await execute_request(
        client=client,
        base_url=base_url,
        read_ratio=read_ratio,
        semaphore=semaphore,
    )

# PAYMENT STATUS COUNTS
def get_payment_status_counts() -> Counter:
    return Counter(payment_statuses.values())

# LIVE STATUS
def print_live_status(*,elapsed: int,current_rps: int) -> None:
    payment_counts = (get_payment_status_counts())
    print(
        f"[{elapsed:04d}s] "
        f"RPS={current_rps:<4} "
        f"HTTP={stats.total_requests:<7} "
        f"2xx={stats.successful_requests:<7} "
        f"4xx={stats.client_errors:<5} "
        f"5xx={stats.server_errors:<5} "
        f"| "
        f"INIT={payment_counts['INITIATED']:<5} "
        f"PEND={payment_counts['PENDING']:<5} "
        f"COMP={payment_counts['COMPLETED']:<5} "
        f"REJ={payment_counts['REJECTED']:<4}"
    )

# GENERATE TRAFFIC
async def generate_traffic(
    *,
    base_url: str,
    base_rps: int,
    duration: int,
    profile: str,
    read_ratio: float,
    max_concurrency: int,
    timeout_seconds: float,
) -> None:
    print()
    print("=" * 70)
    print(
        "PAYMENT PLATFORM "
        "TRAFFIC GENERATOR"
    )
    print("=" * 70)
    print(
        f"Target:           "
        f"{base_url}"
    )
    print(
        f"Base RPS:         "
        f"{base_rps}"
    )
    print(
        f"Duration:         "
        f"{duration}s"
    )
    print(
        f"Profile:          "
        f"{profile}"
    )
    print(
        f"Read ratio:       "
        f"{read_ratio:.0%}"
    )
    print(
        f"Create ratio:     "
        f"{1 - read_ratio:.0%}"
    )
    print(
        f"Max concurrency:  "
        f"{max_concurrency}"
    )

    print(
        f"HTTP timeout:     "
        f"{timeout_seconds}s"
    )
    print("=" * 70)
    print()

    limits = httpx.Limits(
        max_connections=max_concurrency,
        max_keepalive_connections=max(10, max_concurrency // 2)
    )
    timeout = httpx.Timeout(timeout_seconds)
    semaphore = asyncio.Semaphore(max_concurrency)
    pending_tasks: set[asyncio.Task] = set()
    async with httpx.AsyncClient(limits=limits,timeout=timeout) as client:
        test_start = (time.perf_counter())
        second = 0
        while True:
            elapsed = (time.perf_counter()- test_start)
            if elapsed >= duration:
                break
            current_rps = (
                get_current_rps(
                    profile=profile,
                    base_rps=base_rps,
                    elapsed=elapsed,
                    duration=duration,
                )
            )
            print_live_status(elapsed=second,current_rps=current_rps)
            # Requests are distibuteted equaly
            # in single second mark
            interval = (1.0 / current_rps)
            batch_start = (time.perf_counter())
            for request_number in range(current_rps):
                desired_time = (batch_start+(request_number*interval))
                delay = max(0,desired_time- time.perf_counter())
                task = asyncio.create_task(
                    delayed_request(
                        delay=delay,
                        client=client,
                        base_url=base_url,
                        read_ratio=read_ratio,
                        semaphore=semaphore,
                    )
                )
                pending_tasks.add(task)

                # automatic deletion
                # of tasks that ended from the set
                task.add_done_callback(pending_tasks.discard)
            second += 1
            # we keep cycle over 1 second
            next_second = (test_start+ second)
            sleep_time = (next_second- time.perf_counter())
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        # we wait for all requests to end
        if pending_tasks:
            print()
            print(
                "Waiting for remaining "
                f"{len(pending_tasks)} "
                "requests..."
            )
            await asyncio.gather(*pending_tasks,return_exceptions=True)

# FINAL PAYMENT STATUS REFRESH
async def refresh_payment_statuses(
    *,
    base_url: str,
    max_payments: int,
    wait_seconds: float,
    timeout_seconds: float,
) -> None:
    """
    After the traffic generation finishes 
    we will wait for the clearing house to finish 
    and then  we refresh the status this way
    we can get update on the statuses
    """

    if not created_payment_ids:
        return
    if wait_seconds > 0:
        print()
        print(
            f"Waiting {wait_seconds:.1f}s "
            "for Clearing House processing..."
        )
        await asyncio.sleep(wait_seconds)
    ids_to_check = (created_payment_ids[-max_payments:])
    print(
        f"Refreshing status of "
        f"{len(ids_to_check)} "
        "payments..."
    )
    limits = httpx.Limits(max_connections=50,max_keepalive_connections=25)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        limits=limits,
    ) as client:
        semaphore = (asyncio.Semaphore(50))
        async def refresh(payment_id: str) -> None:
            async with semaphore:
                url = (
                    f"{base_url}/payments/"
                    f"{payment_id}"
                )
                try:
                    response = (await client.get(url))
                    if not response.is_success:
                        return
                    data = (response.json())
                    status = (data.get("status"))
                    if (status in VALID_PAYMENT_STATUSES):
                        payment_statuses[payment_id] = status
                except (httpx.RequestError,ValueError):
                    pass

        await asyncio.gather(
            *[refresh(payment_id) for payment_id in ids_to_check]
        )

# PERCENTILE
def percentile(values: list[float],percentile_value: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(percentile_value* (len(sorted_values)- 1))
    return sorted_values[index]

# SUMMARY
def print_summary() -> None:
    payment_counts = (get_payment_status_counts())
    print()
    print("=" * 70)
    print("TRAFFIC GENERATION SUMMARY")
    print("=" * 70)
    print()
    print("HTTP REQUESTS")
    print("-" * 70)
    print(
        f"Total requests:       "
        f"{stats.total_requests}"
    )
    print(
        f"POST requests:        "
        f"{stats.post_requests}"
    )
    print(
        f"GET requests:         "
        f"{stats.get_requests}"
    )
    print(
        f"Successful HTTP:      "
        f"{stats.successful_requests}"
    )
    print(
        f"Client errors 4xx:    "
        f"{stats.client_errors}"
    )
    print(
        f"Server errors 5xx:    "
        f"{stats.server_errors}"
    )
    print(
        f"Connection errors:    "
        f"{stats.connection_errors}"
    )
    if stats.total_requests > 0:
        success_rate = (stats.successful_requests/ stats.total_requests*100)
        print(
            f"HTTP success rate:    "
            f"{success_rate:.2f}%"
        )
    print()
    print("HTTP STATUS CODES")
    print("-" * 70)
    for status_code, count in sorted(stats.status_codes.items()):
        print(
            f"HTTP {status_code}:"
            f"{count:>12}"
        )
    print()
    print("HTTP LATENCY")
    print( "-" * 70)
    if stats.latencies_ms:
        print(
            f"Average:              "
            f"{statistics.mean(stats.latencies_ms):.2f} ms"
        )
        print(
            f"p50:                  "
            f"{percentile(stats.latencies_ms, 0.50):.2f} ms"
        )
        print(
            f"p95:                  "
            f"{percentile(stats.latencies_ms, 0.95):.2f} ms"
        )
        print(
            f"p99:                  "
            f"{percentile(stats.latencies_ms, 0.99):.2f} ms"
        )
        print(
            f"Maximum:              "
            f"{max(stats.latencies_ms):.2f} ms"
        )

    else:
        print("No latency data.")
    print()
    print("PAYMENT BUSINESS STATUS")
    print("-" * 70)
    print(
        f"Payments created:     "
        f"{len(created_payment_ids)}"
    )
    print(
        f"INITIATED:            "
        f"{payment_counts['INITIATED']}"
    )
    print(
        f"PENDING:              "
        f"{payment_counts['PENDING']}"
    )
    print(
        f"COMPLETED:            "
        f"{payment_counts['COMPLETED']}"
    )
    print(
        f"REJECTED:             "
        f"{payment_counts['REJECTED']}"
    )
    print(
        f"CANCELLED:            "
        f"{payment_counts['CANCELLED']}"
    )
    terminal_count = (
        payment_counts["COMPLETED"]
        + payment_counts["REJECTED"]
        + payment_counts["CANCELLED"]
    )
    if terminal_count > 0:
        completed_rate = (payment_counts["COMPLETED"]/ terminal_count* 100)
        print()
        print(
            "Completed / terminal: "
            f"{completed_rate:.2f}%"
        )
    print()
    print("=" * 70)

# ARGUMENTS
def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Traffic generator for "
            "Payment Platform SRE Lab"
        )
    )
    parser.add_argument(
        "--base-url",
        default=("http://127.0.0.1:8000"),
        help=(
            "Payment API URL. "
            "Default: http://127.0.0.1:8000"
        ),
    )

    parser.add_argument(
        "--rps",
        type=int,
        default=10,
        help=(
            "Base requests per second. "
            "Default: 10"
        ),
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help=(
            "Traffic generation duration "
            "in seconds. Default: 60"
        ),
    )

    parser.add_argument(
        "--profile",
        choices=["normal","spike","stress"],
        default="normal",
        help=(
            "Traffic profile. "
            "Default: normal"
        ),
    )

    parser.add_argument(
        "--read-ratio",
        type=float,
        default=0.20,
        help=(
            "Fraction of GET requests. "
            "0.20 = 20%% GET / 80%% POST. "
            "Default: 0.20"
        ),
    )

    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=200,
        help=(
            "Maximum simultaneous HTTP "
            "requests. Default: 200"
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help=(
            "HTTP request timeout in "
            "seconds. Default: 10"
        ),
    )

    parser.add_argument(
        "--final-wait",
        type=float,
        default=6.0,
        help=(
            "Seconds to wait after traffic "
            "generation before final payment "
            "status refresh. Default: 6"
        ),
    )

    parser.add_argument(
        "--final-check-limit",
        type=int,
        default=500,
        help=(
            "Maximum number of recent "
            "payments checked at the end. "
            "Default: 500"
        ),
    )

    args = parser.parse_args()

    if args.rps <= 0:
        parser.error("--rps must be greater than 0")

    if args.duration <= 0:
        parser.error("--duration must be greater than 0")

    if not (0.0 <= args.read_ratio <= 1.0):
        parser.error(
            "--read-ratio must be "
            "between 0 and 1"
        )

    if args.max_concurrency <= 0:
        parser.error(
            "--max-concurrency must "
            "be greater than 0"
        )

    return args

# MAIN
async def main():
    args = parse_args()
    base_url = (args.base_url.rstrip("/"))
    await generate_traffic(
        base_url=base_url,
        base_rps=args.rps,
        duration=args.duration,
        profile=args.profile,
        read_ratio=args.read_ratio,
        max_concurrency=args.max_concurrency,
        timeout_seconds=args.timeout,
    )

    await refresh_payment_statuses(
        base_url=base_url,
        max_payments=args.final_check_limit,
        wait_seconds=args.final_wait,
        timeout_seconds=args.timeout,
    )
    print_summary()

if __name__ == "__main__":
    try:
        asyncio.run( main())
    except KeyboardInterrupt:
        print()
        print(
            "Traffic generator stopped "
            "by user."
        )
        print_summary()