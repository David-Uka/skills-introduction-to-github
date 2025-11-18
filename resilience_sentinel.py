import time
import threading
from typing import Callable, Any

class SentinelOpenException(Exception):
    """Raised when the circuit breaker is in the OPEN state, indicating the protected service is short-circuited."""
    pass

class SentinelBreaker:
    """
    Implements the thread-safe Circuit Breaker Pattern for fault tolerance in 
    microservice architectures. It acts as a sentinel, protecting the client 
    from continuously calling a failing service.

    States:
    1. CLOSED: Normal operation. Fails accumulate until threshold is met.
    2. OPEN: Short-circuits calls to the service. Prevents cascading failures.
    3. HALF_OPEN: Allows a few test calls to check if the service has recovered.
    """
    STATE_CLOSED = 'CLOSED'
    STATE_OPEN = 'OPEN'
    STATE_HALF_OPEN = 'HALF_OPEN'

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, half_open_test_attempts: int = 2):
        """
        :param failure_threshold: Max failures before the circuit opens.
        :param recovery_timeout: Time (seconds) the circuit stays open before moving to HALF_OPEN.
        :param half_open_test_attempts: Number of test calls allowed in HALF_OPEN state.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_test_attempts = half_open_test_attempts

        self._state = self.STATE_CLOSED
        self._failures = 0
        self._last_failure_time = 0.0
        self._half_open_successes = 0
        self._lock = threading.Lock() # Essential for thread-safe state management

    def _set_state(self, new_state: str):
        """Internal helper to change the state."""
        with self._lock:
            self._state = new_state

    def _check_and_transition(self):
        """Checks current state and handles time-based transitions from OPEN."""
        current_time = time.monotonic()

        if self._state == self.STATE_OPEN:
            # Check if timeout has expired, if so, transition to HALF_OPEN
            if current_time - self._last_failure_time > self.recovery_timeout:
                print("Sentinel Transition: OPEN -> HALF_OPEN (Recovery timeout reached)")
                self._set_state(self.STATE_HALF_OPEN)
                self._half_open_successes = 0 # Reset test counters

    def call(self, func: Callable[..., Any], *args, **kwargs):
        """
        The wrapper function that executes the protected service call.
        """
        # Always check for transition first, especially from OPEN to HALF_OPEN
        self._check_and_transition()

        if self._state == self.STATE_OPEN:
            # Raise the custom exception for short-circuiting
            raise SentinelOpenException(
                f"Sentinel is OPEN. Service is unavailable. Time until HALF_OPEN: {int(self.recovery_timeout - (time.monotonic() - self._last_failure_time))}s"
            )

        try:
            # Execute the protected function
            result = func(*args, **kwargs)

            # Success handling
            if self._state == self.STATE_HALF_OPEN:
                # In HALF_OPEN, we track consecutive successes
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_test_attempts:
                    print("Sentinel Transition: HALF_OPEN -> CLOSED (Test calls successful)")
                    self._set_state(self.STATE_CLOSED)
                    self._failures = 0 # Full reset on success
            
            # Successful call in CLOSED state resets the counter
            elif self._state == self.STATE_CLOSED:
                if self._failures > 0:
                    print("Sentinel Partial Reset: Service recovered from minor failures.")
                self._failures = 0

            return result

        except Exception as e:
            # Failure handling
            print(f"Service call failed: {type(e).__name__}")
            
            if self._state == self.STATE_HALF_OPEN:
                # Any failure in HALF_OPEN immediately returns to OPEN
                print("Sentinel Transition: HALF_OPEN -> OPEN (Test call failed)")
                self._last_failure_time = time.monotonic()
                self._set_state(self.STATE_OPEN)
                # Keep the failure count, no full reset

            elif self._state == self.STATE_CLOSED:
                # Accumulate failures in CLOSED state
                self._failures += 1
                if self._failures >= self.failure_threshold:
                    # Threshold reached, open the circuit!
                    print("Sentinel Transition: CLOSED -> OPEN (Failure threshold reached)")
                    self._last_failure_time = time.monotonic()
                    self._set_state(self.STATE_OPEN)
            
            # Re-raise the original exception so the calling code can handle it
            raise

# --- Example Usage (Demonstration) ---

def risky_service_call(request_id):
    """A simulated external service that fails sometimes."""
    # Note: We need to reference the breaker instance 'sb' here, which is initialized below.
    # We use a trick for the example to check the state, which is okay for this demo.
    current_state = SentinelBreaker.STATE_CLOSED
    try:
        current_state = sb._state
    except NameError:
        pass # Ignore if sb is not defined yet

    # Simulate failure on specific request IDs
    if request_id % 3 == 0 and current_state != SentinelBreaker.STATE_HALF_OPEN:
        print(f"  [Service] Request {request_id}: FAILED (Simulated ConnectionError)")
        raise ConnectionError("Service unreachable")
    elif request_id % 7 == 0:
        # Simulate a persistent failure that lasts longer
        print(f"  [Service] Request {request_id}: FAILED (Simulated TimeoutError)")
        raise TimeoutError("Service timed out")
    else:
        print(f"  [Service] Request {request_id}: Success")
        return f"Response for {request_id}"

# 1. Initialize the Circuit Breaker (now SentinelBreaker)
sb = SentinelBreaker(failure_threshold=3, recovery_timeout=5, half_open_test_attempts=2)
print(f"--- Sentinel Breaker Initialized (State: {sb._state}) ---")

# 2. Simulate calls over time
for i in range(1, 15):
    print(f"\nAttempt {i}:")
    try:
        # Protect the service call with the circuit breaker
        response = sb.call(risky_service_call, request_id=i)
        print(f"  [Client] Received: {response}")
    except SentinelOpenException as e:
        # This is the expected short-circuiting behavior
        print(f"  [Client] SHORT-CIRCUITED by Sentinel: {e}")
        time.sleep(1) # Wait a bit before next attempt
    except Exception as e:
        # Handle the actual service failure (e.g., log it)
        print(f"  [Client] Service Exception: {e}")
        time.sleep(0.5) # Wait a bit before next attempt

    # Inject a pause to simulate time passing and demonstrate the recovery timeout
    if i == 5:
        print("\n--- Injecting a 6-second pause to test recovery timeout ---")
        time.sleep(6) # Breaker should move to HALF_OPEN after this pause

print("\n--- Final State Check ---")
sb._check_and_transition() # Ensure transition is checked after the loop finishes
print(f"Final Sentinel State: {sb._state}")
