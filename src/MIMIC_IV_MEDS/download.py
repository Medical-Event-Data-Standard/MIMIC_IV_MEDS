import contextlib
import hashlib
import logging
import os
import queue
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Connect and per-chunk read timeouts (seconds). `requests` defaults to no
# timeout, so a stalled TCP socket parks a worker in recv() indefinitely —
# PhysioNet's edge silently drops chunked connections, which was hitting us
# as a full pipeline hang (see #42). READ_TIMEOUT_S is the gap between bytes,
# not total wall-clock: a slow-but-alive 25 KB/s stream is fine, only truly
# stalled connections trip it.
CONNECT_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 60.0
DEFAULT_TIMEOUT = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S)

# 1 MiB streaming chunks instead of 8 KiB — 128x fewer write() syscalls on
# large files (chartevents.csv.gz is ~25 GB uncompressed) with no downside.
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def coerce_download_workers(raw: object, *, default: int = 1) -> int:
    """Coerce + validate a raw `download_workers` config value.

    Used by both the CLI (`__main__.py`, where the value comes from Hydra and may be
    `None`) and the library API (`download_data`, where it's a kwarg). Centralized so
    the two layers raise identical error messages.

    `None` → `default` (so `download_workers: null` in YAML behaves like an unset key).
    `bool` is rejected explicitly because `int(True) == 1` would silently take the
    sequential path even though `download_workers: true` in YAML is almost certainly a
    config typo.
    """
    if raw is None:
        return default
    if isinstance(raw, bool):
        raise ValueError(f"download_workers must be a positive int, got {raw!r} (bool)")
    try:
        value = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"download_workers must be a positive int, got {raw!r} ({type(raw).__name__})"
        ) from e
    if value < 1:
        raise ValueError(f"download_workers must be a positive int, got {value}")
    return value


def make_session_with_retries() -> requests.Session:
    """Return a `requests.Session` with a retry adapter mounted for transient server errors.

    PhysioNet returns 429/500/502/503/504 regularly under load; without retries a single transient failure
    unwinds the whole crawl. The adapter handles connect-time failures and error-status retries before the
    body starts streaming. urllib3's first retry happens immediately; subsequent retries sleep
    `backoff_factor * (2 ** (attempt - 1))` seconds, so with our `backoff_factor=2.0` and `total=5` the
    worst-case sequence is 0, 2, 4, 8, 16 s between attempts (capped by urllib3's default `backoff_max`).
    `Retry-After` headers are respected when the server supplies them.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_checksum_cache = {}


def compute_sha256(file_path: Path) -> str:
    """Computes the SHA256 checksum of the specified file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_checksum_mapping(base_url: str, session: requests.Session) -> dict:
    """Downloads and parses the SHA256SUMS.txt from the given base URL.

    The expected checksum file is located at <base_url>/SHA256SUMS.txt. Returns a dictionary mapping relative
    file paths to their expected SHA256 checksum.
    """
    if base_url in _checksum_cache:
        return _checksum_cache[base_url]
    checksum_url = base_url + "SHA256SUMS.txt" if base_url.endswith("/") else base_url + "/SHA256SUMS.txt"
    r = session.get(checksum_url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    mapping = {}
    for line in r.text.strip().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            mapping[parts[1]] = parts[0]
    _checksum_cache[base_url] = mapping
    return mapping


logger = logging.getLogger(__name__)


class MockResponse:  # pragma: no cover
    """A mock requests.Response objects for tests."""

    def __init__(self, status_code: int, contents: str = ""):
        self.status_code = status_code
        self.contents = contents.encode()

    def iter_content(self, chunk_size):
        return [self.contents[i : i + chunk_size] for i in range(0, len(self.contents), chunk_size)]

    @property
    def text(self):
        return self.contents.decode()

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.exceptions.HTTPError(self.status_code)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Mirror `requests.Response.__exit__`: close the response so the mock
        # catches future regressions where production code starts relying on
        # close being part of the context-manager contract.
        self.close()
        return False

    def close(self):
        pass


class MockSession:  # pragma: no cover
    """A mock requests.Session objects for tests."""

    def __init__(
        self,
        return_status: int | dict = 200,
        return_contents: str | dict = "hello world",
        expect_url: str | None = None,
    ):
        self.return_status = return_status
        self.return_contents = return_contents
        self.expect_url = expect_url
        self.headers = {}
        self.auth = None

    def close(self):
        pass

    def get(self, url: str, stream: bool = False, **kwargs):
        if self.expect_url is not None and url != self.expect_url:
            raise ValueError(f"Expected URL {self.expect_url}, got {url}")
        if isinstance(self.return_status, dict):
            if url in self.return_status:
                status = self.return_status[url]
            else:
                status = 404
        else:
            status = self.return_status
        if isinstance(self.return_contents, dict):
            if url in self.return_contents:
                contents = self.return_contents[url]
            else:
                status = 404
        else:
            contents = self.return_contents
        return MockResponse(status_code=status, contents=contents)


def download_file(url: str, output_dir: Path, session: requests.Session):
    """Download a single file.

    Args:
        url: The URL to download.
        output_dir: The directory to download the file to.
        session: The requests session to use for downloading.

    Raises:
        Various requests exceptions if the download fails.

    Examples:
        >>> import tempfile
        >>> url = "http://example.com/foo.csv"
        >>> mock_session = MockSession(expect_url=url, return_contents="1,2,3")
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_file(url, Path(tmpdir), mock_session)
        ...     out_path = Path(tmpdir) / "foo.csv"
        ...     out_path.read_text()
        '1,2,3'
        >>> url = "http://example.com"
        >>> mock_session = MockSession(return_contents="hello world", expect_url=url)
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_file(url, Path(tmpdir), mock_session)
        ...     assert len(list(Path(tmpdir).iterdir())) == 1 # Only one file should be downloaded
        ...     out_path = Path(tmpdir) / "index.html"
        ...     out_path.read_text()
        'hello world'
    """
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path) or "index.html"
    file_path = Path(output_dir) / filename

    if file_path.exists():
        parts = parsed_url.path.split("/")
        if len(parts) >= 4:
            base_path = "/".join(parts[:4]) + "/"
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{base_path}"
            rel_key = "/".join(parts[4:]) if "/".join(parts[4:]) else filename
            try:
                mapping = get_checksum_mapping(base_url, session)
                if rel_key in mapping:
                    expected_checksum = mapping[rel_key]
                    actual_checksum = compute_sha256(file_path)
                    if actual_checksum == expected_checksum:
                        logger.info(f"Skipping download, file already exists and valid checksum: {file_path}")
                        return
                    else:
                        logger.info(
                            f"Checksum mismatch for {file_path}. Expected {expected_checksum} but got "
                            f"{actual_checksum}. Redownloading."
                        )
                else:
                    logger.info(
                        f"No checksum found for {rel_key} in SHA256SUMS.txt. Redownloading file: {file_path}"
                    )
            except Exception as e:
                logger.warning(f"Checksum validation failed for {file_path}: {e}. Proceeding to download.")
        else:
            logger.debug(
                f"Skipping checksum validation for {url}: "
                "URL path too short to derive SHA256SUMS.txt location"
            )

    try:
        # Use the response as a context manager so a non-200 status (or any
        # exception thrown by raise_for_status) reliably returns the streaming
        # connection to the pool instead of leaking it.
        with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as response:
            if response.status_code != 200:
                logger.error(
                    f"Failed to download {url} in streaming download_file get: {response.status_code}"
                )
            response.raise_for_status()
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    file.write(chunk)
    except Exception as e:
        raise ValueError(f"Failed to download {url}") from e

    logger.info(f"Downloaded: {file_path}")


def _enumerate_files(base_url: str, output_dir: Path, session: requests.Session) -> list[tuple[str, Path]]:
    """Walk a (possibly recursive) HTML directory listing and return a flat list of files to download.

    Each result is a `(file_url, output_subdir)` pair: the URL to fetch, and the subdirectory under
    `output_dir` where the file should land. Subdirectories are created eagerly during the walk so
    that subsequent parallel writes don't race on `mkdir`.

    Separating enumeration from transfer lets callers fan out the actual downloads across a worker
    pool — see `crawl_and_download`. The walk itself is sequential and only fetches small HTML
    indexes, so it isn't a meaningful contributor to wall-clock time.
    """
    if not base_url.endswith("/"):
        return [(base_url, output_dir)]

    try:
        # Use the response as a context manager so its connection is reliably released
        # back to the urllib3 pool — including in the raise_for_status() failure path,
        # where without the with-block the response would only release on GC.
        with session.get(base_url, timeout=DEFAULT_TIMEOUT) as response:
            if response.status_code != 200:
                logger.error(f"Failed to download {base_url} in initial get: {response.status_code}")
            response.raise_for_status()
            body = response.text
    # Catch the full RequestException tree (HTTPError, ConnectionError, Timeout, ...)
    # rather than just HTTPError. The retry adapter has already given up by this point,
    # so any of these is a real failure and should surface as a ValueError so callers
    # don't have to catch two unrelated exception families.
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to download data from {base_url}") from e

    items: list[tuple[str, Path]] = []
    soup = BeautifulSoup(body, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(base_url, href)
        if not full_url.startswith(base_url):
            continue

        if full_url.endswith("/"):  # directory
            subdir = Path(output_dir) / full_url.replace(base_url, "").strip("/")
            subdir.mkdir(parents=True, exist_ok=True)
            items.extend(_enumerate_files(full_url, subdir, session))
        else:
            filepath = output_dir / full_url.replace(base_url, "")
            subdir = filepath.parent
            subdir.mkdir(parents=True, exist_ok=True)
            items.append((full_url, subdir))
    return items


def crawl_and_download(
    base_url: str,
    output_dir: Path,
    session: requests.Session,
    *,
    max_workers: int = 1,
    session_factory: Callable[[], requests.Session] | None = None,
):
    """Recursively crawl and download files, optionally in parallel.

    Args:
        base_url: The base URL to crawl.
        output_dir: The directory to download the files to.
        session: The requests session used for HTML enumeration. When `max_workers == 1`, this
            session is also used for the file transfers; otherwise its `auth` and `headers` are
            cloned onto each worker session created via `session_factory`.
        max_workers: Number of parallel file downloads. Defaults to 1 (sequential).
            Values > 1 fan out the file transfers across a thread pool; this is the lever
            that beats PhysioNet's ~50 KB/s per-connection cap, since they do not appear
            to throttle aggregate per-IP throughput. Note that even at `max_workers == 1`
            the implementation now fully enumerates the directory tree before downloading
            any files (rather than the prior interleaved crawl+download). For typical
            PhysioNet-shaped datasets (tens of files, two-deep tree) this is sub-second
            and a few KB of memory; the unified path keeps sequential and parallel modes
            from drifting apart over time.
        session_factory: Required when `max_workers > 1` (raises `ValueError` if missing).
            Called exactly `max_workers` times up-front to mint per-worker `requests.Session`s,
            which are then checked out / returned via a queue across files (so each worker pays
            the TCP/TLS handshake once, not per file). Each worker's session inherits `auth`
            and `headers` from the enumerating `session` so authenticated downloads work
            transparently. Ignored when `max_workers == 1`.

    Raises:
        ValueError: If `max_workers > 1` is requested without a `session_factory`, or if any
            download fails. With `max_workers > 1`, all downloads run to completion before the
            failure is raised, so a single bad URL doesn't leave the other workers half-finished;
            the raised error includes the count of failures plus the first failing URL, with the
            original exception attached as `__cause__`.

    Examples:
        >>> import tempfile
        >>> pages = {
        ...     "http://example.com/": (
        ...         "<a href='http://example.com/foo.csv'>foo</a>"
        ...         "<a href='bar/'>bar</a>"
        ...         "<a href='http://example.com/bur/wor.csv'>bur/wor</a>"
        ...         "<div>hello world</div>"
        ...         "<a href='http://example3.com/not_captured.csv'>baz</a>"
        ...     ),
        ...     "http://example.com/foo.csv": "1,2,3,4,5,6",
        ...     "http://example.com/bar/": (
        ...         "<a href='http://example.com/bar/baz.csv'>baz</a>"
        ...         "<a href='http://example.com/bar/qux.csv'>qux</a>"
        ...     ),
        ...     "http://example.com/bar/baz.csv": "7,8,9",
        ...     "http://example.com/bar/qux.csv": "10,11,12",
        ...     "http://example.com/bur/wor.csv": "13,14,15",
        ... }
        >>> mock_session = MockSession(return_contents=pages)
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     crawl_and_download("http://example.com/", tmpdir, mock_session)
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     want = {"foo.csv", "bar/baz.csv", "bar/qux.csv", "bur/wor.csv"}
        ...     assert got == want, f"want {want}, got {got}"
        ...     assert (tmpdir / "foo.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"
        ...     assert (tmpdir / "bar" / "baz.csv").read_text() == "7,8,9", "bar/baz.csv check"
        ...     assert (tmpdir / "bar" / "qux.csv").read_text() == "10,11,12", "bar/qux.csv check"
        ...     assert (tmpdir / "bur" / "wor.csv").read_text() == "13,14,15", "bur/wor.csv check"
    """
    if max_workers > 1 and session_factory is None:
        # Per docstring contract — silently degrading to sequential when the caller
        # asked for parallelism would mask config bugs (user sets download_workers=8
        # in main.yaml, sees no speedup, blames PhysioNet).
        raise ValueError("session_factory must be provided when max_workers > 1")

    files = _enumerate_files(base_url, output_dir, session)

    if max_workers <= 1:
        for file_url, subdir in files:
            download_file(file_url, subdir, session)
        return

    # Pre-create exactly max_workers sessions, hand them out via a queue so each one
    # is reused across many files instead of being torn down per-file. Each new session
    # would otherwise pay a fresh TCP + TLS handshake (and cold-start the urllib3
    # connection pool); on a 33-file MIMIC-IV download with workers=8, that's 33 handshakes
    # vs the 8 we get here. Auth + headers are cloned from the enumerating session so
    # authenticated endpoints work transparently.
    worker_auth = session.auth
    worker_headers = dict(session.headers)
    pool: queue.Queue[requests.Session] = queue.Queue()
    # Track every session we create so the cleanup in the outer finally can close them
    # all unconditionally. Closing only the queue's contents would miss two cases:
    #   (a) `session_factory()` raises partway through pre-creation, leaving earlier
    #       sessions orphaned (no caller would ever see them);
    #   (b) on the Ctrl+C / SystemExit path, in-flight workers may still be holding
    #       checked-out sessions when the parent's finally runs — those wouldn't be
    #       in the queue at drain time.
    all_sessions: list[requests.Session] = []
    try:
        for _ in range(max_workers):
            s = session_factory()
            all_sessions.append(s)
            s.auth = worker_auth
            s.headers.update(worker_headers)
            pool.put(s)
    except Exception:
        # Best-effort cleanup of the partially-built pool; the original error is what
        # the caller should see, so swallow any close() failures here.
        for s in all_sessions:
            with contextlib.suppress(Exception):
                s.close()
        raise

    def _download_one(item: tuple[str, Path]) -> None:
        file_url, subdir = item
        worker_session = pool.get()
        try:
            download_file(file_url, subdir, worker_session)
        finally:
            pool.put(worker_session)

    errors: list[tuple[str, Exception]] = []
    ex = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="download")
    # Decide what kind of executor shutdown to do based on whether we make it through the
    # as_completed loop normally. The default `with ThreadPoolExecutor(...) as ex:` exit
    # path calls shutdown(wait=True), which would block Ctrl+C until every in-flight
    # download_file finishes — and PhysioNet downloads at ~50 KB/s can run for hours.
    # On interrupt we instead cancel queued futures and skip the wait so the abort
    # surfaces within at most one read-timeout window (READ_TIMEOUT_S=60s; in-flight
    # iter_content reads can't be cancelled mid-chunk, but the TCP read will time out).
    shutdown_kwargs: dict = {"wait": True}
    try:
        future_to_url = {ex.submit(_download_one, item): item[0] for item in files}
        for fut in as_completed(future_to_url):
            file_url = future_to_url[fut]
            try:
                fut.result()
            # Catch Exception (not BaseException) so KeyboardInterrupt / SystemExit
            # are not aggregated as ordinary download failures — they propagate to
            # the outer except and trigger the fast-shutdown path below.
            except Exception as e:
                errors.append((file_url, e))
                logger.error(f"Parallel download failed for {file_url}: {e}")
    except (KeyboardInterrupt, SystemExit):
        shutdown_kwargs = {"wait": False, "cancel_futures": True}
        raise
    finally:
        ex.shutdown(**shutdown_kwargs)
        # Close every session we created, not just those currently in the queue. On the
        # interrupt path, a worker thread mid-`iter_content` may not have returned its
        # session to the queue yet; closing via `all_sessions` covers it (and is also
        # safe to call repeatedly — Session.close is idempotent).
        for s in all_sessions:
            with contextlib.suppress(Exception):
                s.close()

    if errors:
        first_url, first_err = errors[0]
        raise ValueError(f"{len(errors)} download(s) failed (first: {first_url!r})") from first_err


def download_data(
    output_dir: Path,
    dataset_info: DictConfig,
    do_demo: bool = False,
    session_factory: Callable[[], requests.Session] = make_session_with_retries,
    download_workers: int = 1,
):
    """Downloads the data specified in dataset_info.dataset_urls to the output_dir.

    Args:
        output_dir: The directory to download the data to.
        dataset_info: The dataset information containing the URLs to download.
        do_demo: If True, download the demo URLs instead of the main URLs.
        session_factory: A callable that returns a requests.Session object (for testing).
        download_workers: Number of files to download in parallel within a single base URL.
            Defaults to 1 (sequential). PhysioNet caps each connection at ~50 KB/s but does
            not appear to per-IP throttle, so values of 4-8 typically yield a 4-8x throughput
            increase. Higher values give diminishing returns and may trigger 429 responses
            (which the retry adapter handles transparently).

    Raises:
        ValueError: If the command fails

    Examples:
        >>> import tempfile
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": ["http://example.com/demo.csv"],
        ...         "dataset": ["http://example.com/dataset/"],
        ...         "common": ["http://example.com/common.csv"],
        ...     }
        ... })
        >>> demo_session = MockSession(return_contents={
        ...     "http://example.com/demo.csv": "demo", "http://example.com/common.csv": "common"
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=True, session_factory=lambda: demo_session)
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"demo.csv", "common.csv"}, f"want {'demo.csv', 'common.csv'}, got {got}"
        ...     assert (tmpdir / "demo.csv").read_text() == "demo", "demo.csv check"
        ...     assert (tmpdir / "common.csv").read_text() == "common", "common.csv check"
        >>> real_session = MockSession(return_contents={
        ...     "http://example.com/dataset/": "<a href='http://example.com/dataset/foo.csv'>foo</a>",
        ...     "http://example.com/common.csv": "common",
        ...     "http://example.com/dataset/foo.csv": "1,2,3,4,5,6",
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=False, session_factory=lambda: real_session)
        ...     assert real_session.headers == {}, "Headers check"
        ...     assert real_session.auth is None, "Auth check"
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"foo.csv", "common.csv"}, f"want {'foo.csv', 'common.csv'}, got {got}"
        ...     assert len(got) == 2, f"want 2 files, got {[f.relative_to(tmpdir) for f in got]}"
        ...     assert (tmpdir / "foo.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"
        ...     assert (tmpdir / "common.csv").read_text() == "common", "common.csv check"
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "dataset": [{"url": "http://example.com/dataset/", "username": "u", "password": "p"}],
        ...     }
        ... })
        >>> real_session = MockSession(return_contents={
        ...     "http://example.com/dataset/": "<a href='http://example.com/dataset/baz/bar.csv' />",
        ...     "http://example.com/dataset/baz/bar.csv": "1,2,3,4,5,6",
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=False, session_factory=lambda: real_session)
        ...     assert real_session.headers["User-Agent"] == "Wget/1.21.1 (linux-gnu)", "User-Agent check"
        ...     assert real_session.auth == ("u", "p"), "Auth check"
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"baz/bar.csv"}, f"want {'baz/bar.csv'}, got {got}"
        ...     assert (tmpdir / "baz/bar.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"

    If the internal download fails, a ValueError is raised:
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": ["http://example.com/demo.csv"],
        ...         "dataset": ["http://example.com/dataset/"],
        ...         "common": ["http://example.com/common.csv"],
        ...     }
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=True, session_factory=lambda: real_session)
        Traceback (most recent call last):
            ...
        ValueError: Failed to download data from http://example.com/demo.csv: Failed to download http://example.com/demo.csv
    """

    # Validate up front via the same coercer the CLI uses, so the error message shape is
    # identical across entry points and a misconfigured `download_workers` (None, bool,
    # negative, non-numeric) fails loudly here rather than silently degrading inside
    # crawl_and_download or producing a confusing log line.
    download_workers = coerce_download_workers(download_workers)

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    urls = list(urls) + list(dataset_info.urls.get("common", []))

    for url in urls:
        session = session_factory()
        try:
            if isinstance(url, dict | DictConfig):
                username = url.get("username", None)
                password = url.get("password", None)
                logger.info(f"Authenticating for {username}")
                session.auth = (username, password)
                session.headers.update({"User-Agent": "Wget/1.21.1 (linux-gnu)"})

                # `.get("url")` works uniformly for both `dict` and
                # `DictConfig`; attribute access `url.url` would have raised
                # on a plain dict even though the isinstance check admits one.
                url = url.get("url")

            try:
                crawl_and_download(
                    url,
                    output_dir,
                    session,
                    max_workers=download_workers,
                    session_factory=session_factory,
                )
            except ValueError as e:
                # Preserve the aggregated message from crawl_and_download (e.g. "10
                # download(s) failed (first: 'chartevents.csv.gz')") in the surface
                # error rather than burying it in `__cause__`. `__cause__` is still
                # set for traceback walkers that want the full chain.
                raise ValueError(f"Failed to download data from {url}: {e}") from e
        finally:
            # Release the connection pool tied to this per-URL session so
            # long runs don't hold extra sockets/fds open after each URL.
            session.close()
