import asyncio
import time
import requests
import re
import os

URL = 'https://zip.cm.edu.kg/all.txt'
PATTERN = r'^(\[[0-9A-Fa-f:.]+\]|(?:\d{1,3}\.){3}\d{1,3}):(\d+)#([A-Za-z]{2,})$'

def fetch_endpoints():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    lines = r.text.splitlines()
    endpoints = []
    seen = set()
    for line in lines:
        s = line.strip()
        if not s:
            continue
        m = re.match(PATTERN, s)
        if not m:
            continue
        host = m.group(1)
        if host.startswith('[') and host.endswith(']'):
            host = host[1:-1]
        port = int(m.group(2))
        region = m.group(3)
        key = (host, port, region)
        if key in seen:
            continue
        seen.add(key)
        endpoints.append({'raw': s, 'host': host, 'port': port, 'region': region})
    return endpoints

async def tcp_time(host, port, timeout):
    start = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    except Exception:
        return None
    try:
        writer.close()
        if hasattr(writer, 'wait_closed'):
            await writer.wait_closed()
    except Exception:
        pass
    return time.perf_counter() - start

async def rank_endpoints(endpoints, timeout=2.0, concurrency=200):
    sem = asyncio.Semaphore(concurrency)
    async def run(ep):
        async with sem:
            t = await tcp_time(ep['host'], ep['port'], timeout)
            return ep, t
    tasks = [asyncio.create_task(run(ep)) for ep in endpoints]
    results = await asyncio.gather(*tasks)
    filtered = [(ep, t) for ep, t in results if t is not None]
    filtered.sort(key=lambda x: x[1])
    return filtered

def main():
    endpoints = fetch_endpoints()
    if not endpoints:
        return
    ranked = asyncio.run(rank_endpoints(endpoints))
    lines = [ep['raw'] for ep, _ in ranked]
    if os.path.exists('ip.txt'):
        os.remove('ip.txt')
    with open('ip.txt', 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')

if __name__ == '__main__':
    main()
