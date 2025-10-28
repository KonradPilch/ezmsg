import os
import sys
import json
import datetime
import itertools
import argparse
import typing
import random
import time

from contextlib import contextmanager, redirect_stdout, redirect_stderr

from ..messagecodec import MessageEncoder
from .envinfo import TestEnvironmentInfo
from .impl import (
    TestParameters,
    TestLogEntry, 
    perform_test, 
    Communication,
    CONFIGS,
)

import ezmsg.core as ez

DEFAULT_MSG_SIZES = [2 ** exp for exp in range(4, 25, 8)]
DEFAULT_N_CLIENTS = [2 ** exp for exp in range(0, 6, 2)]
DEFAULT_COMMS = [c for c in Communication]

# --- Output Suppression Context Manager (from the previous solution) ---
@contextmanager
def suppress_output(verbose: bool = False):
    """Context manager to redirect stdout and stderr to os.devnull"""
    if verbose: 
        yield
    else:
        # Open the null device for writing
        with open(os.devnull, 'w') as fnull:
            # Redirect both stdout and stderr to the null device
            with redirect_stderr(fnull):
                with redirect_stdout(fnull):
                    yield

CHECK_FOR_QUIT = lambda: False

if sys.platform.startswith('win'):
    import msvcrt
    def _check_for_quit_win() -> bool:
        """
        Checks for the 'q' key press in a non-blocking way.
        Returns True if 'q' is pressed (case-insensitive), False otherwise.
        """
        # Windows: Use msvcrt for non-blocking keyboard hit detection
        if msvcrt.kbhit(): # type: ignore
            # Read the key press (returns bytes)
            key = msvcrt.getch() # type: ignore
            try:
                # Decode and check for 'q'
                return key.decode().lower() == 'q'
            except UnicodeDecodeError:
                # Handle potential non-text key presses gracefully
                return False
        return False

    CHECK_FOR_QUIT = _check_for_quit_win

else:
    import select
    def _check_for_quit() -> bool:
        """
        Checks for the 'q' key press in a non-blocking way.
        Returns True if 'q' is pressed (case-insensitive), False otherwise.
        """
        # Linux/macOS: Use select to check if stdin has data
        # select.select(rlist, wlist, xlist, timeout)
        # timeout=0 makes it non-blocking
        if sys.stdin.isatty():
            i, o, e = select.select([sys.stdin], [], [], 0) # type: ignore
            if i:
                # Read the buffered character
                key = sys.stdin.read(1)
                return key.lower() == 'q'
        return False
    
    CHECK_FOR_QUIT = _check_for_quit

def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def perf_run(    
    duration: float,
    num_buffers: int,
    iters: int,
    msg_sizes: typing.Iterable[int] | None,
    n_clients: typing.Iterable[int] | None,
    comms: typing.Iterable[str] | None,
    configs: typing.Iterable[str] | None,
    grid: bool,
) -> None:
    
    if n_clients is None:
        n_clients = DEFAULT_N_CLIENTS
    if any(c < 0 for c in n_clients):
        ez.logger.error('All tests must have >=0 clients')
        return

    if msg_sizes is None:
        msg_sizes = DEFAULT_MSG_SIZES
    if any(s < 0 for s in msg_sizes):
        ez.logger.error('All msg_sizes must be >=0 bytes')

    if not grid and len(list(n_clients)) != len(list(msg_sizes)):
        ez.logger.warning(
            "Not performing a grid test of all combinations of n_clients and msg_sizes, but " + \
            "len(n_clients) != len(msg_sizes). " + \
            "If you want to perform all combinations of n_clients and msg_sizes, use --grid"
        )

    try:
        communications = DEFAULT_COMMS if comms is None else [Communication(c) for c in comms]
    except ValueError:
        ez.logger.error(f"Invalid test communications requested. Valid communications: {', '.join([c.value for c in Communication])}")
        return
    
    try:
        configurators = list(CONFIGS.values()) if configs is None else [CONFIGS[c] for c in configs]
    except ValueError:
        ez.logger.error(f"Invalid test configuration requested. Valid configurations: {', '.join([c for c in CONFIGS])}")
        return
    
    subitr = itertools.product if grid else zip

    test_list = [
        (msg_size, clients, conf, comm)
        for msg_size, clients in subitr(msg_sizes, n_clients)
        for conf, comm in itertools.product(configurators, communications)
    ] * iters

    random.shuffle(test_list)

    with open(f'perf_{get_datestamp()}.txt', 'w') as out_f:

        out_f.write(json.dumps(TestEnvironmentInfo(), cls = MessageEncoder) + "\n")

        ez.logger.info("Starting perf tests.  Press 'q' + enter to quit tests early.")
        time.sleep(3.0) # Give user an opportunity to read message.

        for test_idx, (msg_size, clients, conf, comm) in enumerate(test_list):

            if CHECK_FOR_QUIT():
                ez.logger.info("Stopping tests early...")
                break

            ez.logger.info(f"TEST {test_idx + 1}/{len(test_list)}: {clients=}, {msg_size=}, conf={conf.__name__}, comm={comm.value}")

            output = TestLogEntry(
                params = TestParameters(
                    msg_size = msg_size,
                    n_clients = clients,
                    config = conf.__name__,
                    comms = comm.value,
                    duration = duration,
                    num_buffers = num_buffers
                ),
                results = perform_test(
                    n_clients = clients,
                    duration = duration, 
                    msg_size = msg_size, 
                    buffers = num_buffers,
                    comms = comm,
                    config = conf,
                )
            )

            out_f.write(json.dumps(output, cls = MessageEncoder) + "\n")

def setup_run_cmdline(subparsers: argparse._SubParsersAction) -> None:

    p_run = subparsers.add_parser("run", help="run performance test")

    p_run.add_argument(
        "--duration",
        type=float,
        default=2.0,
        help="individual test duration in seconds (default = 2.0)",
    )

    p_run.add_argument(
        "--num-buffers",
        type=int,
        default=32,
        help="shared memory buffers (default = 32)",
    )

    p_run.add_argument(
        "--iters", "-i",
        type = int,
        default = 3,
        help = "number of times to run each test (default = 3)"
    )

    p_run.add_argument(
        "--msg-sizes",
        type = int,
        default = None,
        nargs = "*",
        help = f"message sizes in bytes (default = {DEFAULT_MSG_SIZES})"
    )

    p_run.add_argument(
        "--n-clients",
        type = int,
        default = None,
        nargs = "*",
        help = f"number of clients (default = {DEFAULT_N_CLIENTS})"
    )

    p_run.add_argument(
        "--comms",
        type = str,
        default = None,
        nargs = "*",
        help = f"communication strategies to test (default = {[c.value for c in DEFAULT_COMMS]})"
    )

    p_run.add_argument(
        "--configs",
        type = str,
        default = None,
        nargs = "*",
        help = f"configurations to test (default = {[c for c in CONFIGS]})"
    )

    p_run.add_argument(
        "--grid",
        action = "store_true",
        help = "perform all combinations of msg_sizes and n_clients " + \
            "(default: False; msg_sizes and n_clients must match in length)"
    )

    p_run.set_defaults(_handler=lambda ns: perf_run(
        duration = ns.duration, 
        num_buffers = ns.num_buffers,
        iters = ns.iters,
        msg_sizes = ns.msg_sizes,
        n_clients = ns.n_clients,
        comms = ns.comms,
        configs = ns.configs,
        grid = ns.grid,
    ))