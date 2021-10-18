import sys

def fetch_urls(chunk_size = 1000):
    start = None
    for start in range(0, 9135000, chunk_size):
        yield f"select * from urls order by id limit {start + 1}, {chunk_size}"

