#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import tracemalloc

import pytest
from aioresponses import CallbackResult, aioresponses
from conftest import generate_stream
from source_salesforce.streams import BulkIncrementalSalesforceStream


@pytest.mark.parametrize(
    "n_records, first_size, first_peak",
    (
        (1000, 0.4, 1),
        (10000, 1, 2),
        (100000, 4, 9),
        (200000, 7, 19),
    ),
    ids=[
        "1k recods",
        "10k records",
        "100k records",
        "200k records",
    ],
)
@pytest.mark.asyncio
async def test_memory_download_data(stream_config, stream_api, n_records, first_size, first_peak):
    job_full_url_results: str = "https://fase-account.salesforce.com/services/data/v57.0/jobs/query/7504W00000bkgnpQAA/results"
    stream: BulkIncrementalSalesforceStream = generate_stream("Account", stream_config, stream_api)
    await stream.ensure_session()
    content = b'"Id","IsDeleted"'
    for _ in range(n_records):
        content += b'"0014W000027f6UwQAI","false"\n'

    def callback(url, **kwargs):
        return CallbackResult(body=content)

    import gc

    with aioresponses() as m:
        m.get(job_full_url_results, status=200, callback=callback)
        gc.collect()
        tracemalloc.start()
        tmp_file, response_encoding, _ = await stream.download_data(url=job_full_url_results)
        for x in stream.read_with_chunks(tmp_file, response_encoding):
            pass
        fs, fp = tracemalloc.get_traced_memory()
        first_size_in_mb, first_peak_in_mb = fs / 1024**2, fp / 1024**2

        print(f"{n_records} Records:")
        print(f"first_size_in_mb={first_size_in_mb} wanted < {first_size}")
        print(f"first_peak_in_mb={first_peak_in_mb} wanted < {first_peak}")

        assert first_size_in_mb < first_size
        assert first_peak_in_mb < first_peak

    await stream._session.close()

"""
1000 Records:
first_size_in_mb=1.4702997207641602 wanted < 0.4
first_peak_in_mb=1.7444953918457031 wanted < 1

10000 Records:
first_size_in_mb=2.4218692779541016 wanted < 1
first_peak_in_mb=3.209315299987793 wanted < 2

100000 Records:
first_size_in_mb=5.086523056030273 wanted < 4
first_peak_in_mb=7.993342399597168 wanted < 9


"""
