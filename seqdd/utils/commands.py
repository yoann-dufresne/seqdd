"""Builders for the external download commands, with consistent retry/resume behavior."""

# Retry a few times on transient network failures (including connection refused).
CURL_RETRY = '--retry 3 --retry-delay 5 --retry-connrefused'


def curl_download(url: str, output: str, silent: bool = True, resume: bool = True) -> str:
    """
    Build a curl command line that downloads ``url`` to ``output`` resiliently.

    Combines ``--retry`` (recover from transient network errors) with ``-C -`` (resume a
    partially downloaded file), which together let an interrupted transfer continue instead of
    restarting from scratch.

    :param url: The URL to download.
    :param output: The output file path.
    :param silent: Whether to pass ``-s`` (no progress meter), as for background validation jobs.
    :param resume: Whether to resume a partially downloaded file (``-C -``).
    :return: The curl command line string.
    """
    parts = ['curl', CURL_RETRY]
    if silent:
        parts.append('-s')
    if resume:
        parts.append('-C -')
    parts.append(f'-o {output}')
    parts.append(f'"{url}"')
    return ' '.join(parts)
