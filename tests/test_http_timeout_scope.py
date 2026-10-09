import asyncio

import httpx
import pytest

import kosis_analysis.client as transport


def test_local_rate_limit_queue_is_not_the_network_deadline(monkeypatch):
    class Limiter:
        async def acquire(self):
            await asyncio.sleep(0.025)

    class Client:
        async def get(self, url, *, params, timeout):
            assert timeout == 0.01
            return httpx.Response(200, json=[{"DT": "0"}])

    monkeypatch.setattr(transport, "_KOSIS_RATE_LIMITER", Limiter())

    async def run():
        with transport.kosis_http_timeout(0.01):
            return await transport._kosis_call(Client(), "Param/statisticsParameterData.do", {})

    assert asyncio.run(run()) == [{"DT": "0"}]
    assert transport._REQUEST_TIMEOUT.get() == transport.HTTP_TIMEOUT


def test_real_network_timeout_remains_infrastructure_failure(monkeypatch):
    class Limiter:
        async def acquire(self):
            pass

    class Client:
        async def get(self, url, *, params, timeout):
            assert timeout == 20
            raise httpx.ReadTimeout("fixture")

    monkeypatch.setattr(transport, "_KOSIS_RATE_LIMITER", Limiter())

    async def run():
        with transport.kosis_http_timeout(20):
            await transport._kosis_call(Client(), "Param/statisticsParameterData.do", {})

    with pytest.raises(transport.KosisTransportError, match="TIMEOUT"):
        asyncio.run(run())
    assert transport._REQUEST_TIMEOUT.get() == transport.HTTP_TIMEOUT


def test_parallel_requests_do_not_change_each_others_deadlines():
    async def read(seconds):
        with transport.kosis_http_timeout(seconds):
            await asyncio.sleep(0)
            return transport._REQUEST_TIMEOUT.get()

    async def run():
        return await asyncio.gather(read(20), read(30))

    assert asyncio.run(run()) == [20, 30]
