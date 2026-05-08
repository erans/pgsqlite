#!/usr/bin/env python3
"""Quick batch test for 7B model"""
import time
import requests
import asyncio
import aiohttp

API_URL = "http://localhost:8000/v1/chat/completions"

async def send_request(session, idx):
    payload = {
        "model": "qwen2.5-7b-instruct",
        "messages": [{"role": "user", "content": "What is Rust?"}],
        "max_tokens": 50
    }
    start = time.time()
    async with session.post(API_URL, json=payload) as resp:
        result = await resp.json()
        elapsed = time.time() - start
        tokens = result["usage"]["completion_tokens"]
        return elapsed, tokens

async def test_concurrent():
    print("Testing 3 concurrent requests...")
    start = time.time()
    
    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, i) for i in range(3)]
        results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start
    
    print(f"\nResults:")
    for i, (elapsed, tokens) in enumerate(results):
        print(f"  Request {i+1}: {elapsed:.2f}s ({tokens} tokens)")
    
    total_tokens = sum(r[1] for r in results)
    aggregate_throughput = total_tokens / total_time
    
    print(f"\nTotal time: {total_time:.2f}s")
    print(f"Total tokens: {total_tokens}")
    print(f"Aggregate throughput: {aggregate_throughput:.1f} tokens/sec")

if __name__ == "__main__":
    asyncio.run(test_concurrent())
