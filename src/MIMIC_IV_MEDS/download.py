import hashlib
import logging
import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig

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
    r = session.get(checksum_url)
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

    def get(self, url: str, stream: bool = False):
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
    try:
        response = session.get(url, stream=True)
        if response.status_code != 200:
            logger.error(f"Failed to download {url} in streaming download_file get: {response.status_code}")
        response.raise_for_status()
    except Exception as e:
        raise ValueError(f"Failed to download {url}") from e

    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path) or "index.html"
    file_path = Path(output_dir) / filename

    if file_path.exists():
        parts = parsed_url.path.split("/")
        if len(parts) >= 4:
            base_path = "/".join(parts[:4]) + "/"
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{base_path}"
            rel_key = "/".join(parts[4:]) if "/".join(parts[4:]) else filename
        else:
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
            rel_key = filename
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

    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    logger.info(f"Downloaded: {file_path}")


def crawl_and_download(base_url: str, output_dir: Path, session: requests.Session):
    """Recursively crawl and download files.

    Args:
        base_url: The base URL to crawl.
        output_dir: The directory to download the files to.
        session: The requests session to use for downloading.

    Raises:
        Various requests exceptions if downloads fail.

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
    if not base_url.endswith("/"):
        download_file(base_url, output_dir, session)
        return

    try:
        response = session.get(base_url)
        if response.status_code != 200:
            logger.error(f"Failed to download {base_url} in initial get: {response.status_code}")
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(f"Failed to download data from {base_url}") from e

    soup = BeautifulSoup(response.text, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(base_url, href)
        if not full_url.startswith(base_url):
            continue

        if full_url.endswith("/"):  # It's a directory
            subdir = Path(output_dir) / href.strip("/")
            subdir.mkdir(parents=True, exist_ok=True)
            crawl_and_download(full_url, subdir, session)
        else:
            filepath = output_dir / full_url.replace(base_url, "")
            subdir = filepath.parent
            subdir.mkdir(parents=True, exist_ok=True)
            download_file(full_url, subdir, session)


def download_data(
    output_dir: Path,
    dataset_info: DictConfig,
    do_demo: bool = False,
    session_factory: callable = requests.Session,
):
    """Downloads the data specified in dataset_info.dataset_urls to the output_dir.

    Args:
        output_dir: The directory to download the data to.
        dataset_info: The dataset information containing the URLs to download.
        do_demo: If True, download the demo URLs instead of the main URLs.
        session_factory: A callable that returns a requests.Session object (for testing).

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
        ValueError: Failed to download data from http://example.com/demo.csv
    """

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    urls += dataset_info.urls.get("common", [])

    for url in urls:
        session = session_factory()

        if isinstance(url, (dict, DictConfig)):
            username = url.get("username", None)
            password = url.get("password", None)
            logger.info(f"Authenticating for {username}")
            session.auth = (username, password)
            session.headers.update({"User-Agent": "Wget/1.21.1 (linux-gnu)"})

            url = url.url

        try:
            crawl_and_download(url, output_dir, session)
        except ValueError as e:
            raise ValueError(f"Failed to download data from {url}") from e
