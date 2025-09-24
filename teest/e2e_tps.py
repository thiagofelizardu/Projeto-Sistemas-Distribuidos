#!/usr/bin/env python3
"""
e2e_tps.py — E2E load tester (API -> Kafka -> Persister -> Postgres)

Requisitos:
    pip install httpx asyncpg matplotlib tqdm

Uso rápido (Windows PowerShell):
    python e2e_tps.py --pg_dsn "postgresql://user:password@localhost:5432/sd_payments_db" --rate 100 --minutes 2

Parâmetros principais:
    --url           (default: http://localhost:8080/payments)
    --pg_dsn        DSN do Postgres (obrigatório)
    --table         (default: payment_entity)   # conforme sua JPA Entity
    --id_column     (default: tx_id)            # naming strategy snake_case
    --ts_column     (default: created_at)       # opcional para coletar TS do DB
    --db_timeout    tempo máximo para confirmar no DB após envio (s)
    --rate          req/s
    --minutes       duração
"""

import argparse, asyncio, time, math, csv, uuid
from collections import defaultdict, deque
import httpx, asyncpg
import matplotlib.pyplot as plt
from tqdm import tqdm

DEFAULT_BODY = {
    "merchantId": "MERCH-001",
    "customerId": "CUST-123456",
    "terminalId": "TERM-0001",
    "amount": 1050,
    "currency": "BRL",
    "method": "CREDIT",
    "entryMode": "CHIP",
    "cardHash": "3f6d9a0c1e2b4d5f6a7b8c9d0e1f2345"
}

def now(): return time.time()

async def ensure_pg(dsn):
    return await asyncpg.create_pool(dsn, min_size=1, max_size=10)

async def poll_db(pool, table, id_column, ids_batch, timeout_s, ts_column=None):
    """
    Retorna dict {txId: (found_bool, persisted_ts)}; persisted_ts vem do DB se ts_column existir.
    Faz polling por até timeout_s.
    """
    sql = f"SELECT {id_column}" + (f", {ts_column}" if ts_column else "") + f" FROM {table} WHERE {id_column} = ANY($1)"
    end = now() + timeout_s
    seen = {}
    while now() < end and len(seen) < len(ids_batch):
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, list(ids_batch))
        for r in rows:
            # asyncpg traz colunas pelo nome original
            tid = str(r[id_column])
            if tid not in seen:
                seen[tid] = (True, r.get(ts_column) if ts_column else None)
        if len(seen) == len(ids_batch):
            break
        await asyncio.sleep(0.1)
    for tid in ids_batch:
        if tid not in seen:
            seen[tid] = (False, None)
    return seen

async def send_one(client, url, body, timeout):
    t0 = now()
    try:
        r = await client.post(url, json=body, timeout=timeout)
        ok = 200 <= r.status_code < 300
        txid = None
        try:
            data = r.json()
            txid = data.get("txId") or data.get("id") or data.get("phastOrderId")
        except Exception:
            pass
        return {"t0": t0, "t1": now(), "status": r.status_code, "ok": ok, "txId": str(txid) if txid else None, "err": None if ok else f"status:{r.status_code}"}
    except Exception as e:
        return {"t0": t0, "t1": now(), "status": None, "ok": False, "txId": None, "err": str(e)}

async def runner(url, rate, minutes, timeout, concurrency, dsn, table, id_column, db_timeout, ts_column):
    duration_s = minutes * 60
    results = []
    async with httpx.AsyncClient() as client, await ensure_pg(dsn) as pool:
        sem = asyncio.Semaphore(concurrency)
        start = now()
        inflight = deque()  # (txId, t0)
        seconds = int(duration_s)

        async def burst():
            tasks = []
            for _ in range(rate):
                body = dict(DEFAULT_BODY)
                # redundância de correlação caso a API não devolva txId (o backend ignora se não usar)
                client_tx = str(uuid.uuid4())
                body.setdefault("clientTxId", client_tx)
                async def one():
                    async with sem:
                        res = await send_one(client, url, body, timeout)
                        tid = res["txId"] or client_tx
                        res["txId"] = tid
                        if tid:
                            inflight.append((tid, res["t0"]))
                        results.append(res)
                tasks.append(asyncio.create_task(one()))
            await asyncio.gather(*tasks)

        with tqdm(total=seconds, desc="Segundos", unit="s") as bar:
            for sec in range(seconds):
                await burst()
                bar.update(1)
                # pace 1s exato
                target = start + sec + 1
                delay = target - now()
                if delay > 0: await asyncio.sleep(delay)

        # Confirmação: poll em lotes até db_timeout
        confirm_deadline = now() + db_timeout
        confirmations = {}  # txId -> (found, seen_at_client, db_ts)
        batch_size = 500
        while now() < confirm_deadline and inflight:
            batch = []
            while inflight and len(batch) < batch_size:
                batch.append(inflight.popleft())
            ids = [tid for (tid, _) in batch]
            seen = await poll_db(pool, table, id_column, ids, timeout_s=2.0, ts_column=ts_column)
            t_seen = now()
            for (tid, t0) in batch:
                ok, db_ts = seen.get(tid, (False, None))
                if ok:
                    confirmations[tid] = (True, t_seen, db_ts)
                else:
                    # refile para tentar novamente
                    inflight.append((tid, t0))
        while inflight:
            tid, _ = inflight.popleft()
            confirmations[tid] = (False, None, None)

    stop = now()
    return results, confirmations, start, stop

def analyze(results, confirmations, start, stop, out_png, out_csv):
    # TPS por segundo (respostas)
    buckets = defaultdict(list)
    for r in results:
        sec = int(math.floor(r["t1"] - start))
        buckets[sec].append(r)
    secs = list(range(int(math.ceil(stop - start)) + 1))
    tps_total = [len(buckets.get(s, [])) for s in secs]
    tps_ok = [sum(1 for r in buckets.get(s, []) if r["ok"]) for s in secs]
    tps_fail = [sum(1 for r in buckets.get(s, []) if not r["ok"]) for s in secs]

    # Latências E2E (cliente): request_start -> momento em que o cliente viu no DB
    e2e_latencies = []
    confirmed = 0
    for r in results:
        tid = r["txId"]
        if not tid: continue
        conf = confirmations.get(tid)
        if not conf: continue
        found, seen_at_client, db_ts = conf
        if found and seen_at_client:
            e2e = seen_at_client - r["t0"]
            e2e_latencies.append(e2e)
            confirmed += 1

    # CSV detalhado
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["txId","status","ok","req_start","resp_end","e2e_s","db_ts"])
        for r in results:
            tid = r["txId"]
            conf = confirmations.get(tid) if tid else None
            found, seen_at_client, db_ts = conf if conf else (False, None, None)
            e2e = (seen_at_client - r["t0"]) if (found and seen_at_client) else ""
            w.writerow([tid, r["status"], r["ok"], r["t0"], r["t1"], e2e, str(db_ts) if db_ts else ""])

    # Gráfico TPS
    plt.figure(figsize=(10,6))
    plt.plot(secs, tps_total, label="TPS resposta (total)")
    plt.plot(secs, tps_ok, label="TPS sucesso", linestyle="--")
    plt.plot(secs, tps_fail, label="TPS falha", linestyle=":")
    plt.xlabel("Segundos"); plt.ylabel("Req/s"); plt.title("TPS (respostas por segundo)")
    plt.legend(); plt.grid(True); plt.tight_layout(); plt.savefig(out_png.replace(".png","_tps.png")); plt.close()

    if e2e_latencies:
        e2e_sorted = sorted(e2e_latencies)
        def quantile(q):
            idx = max(0, min(len(e2e_sorted)-1, int(q * (len(e2e_sorted)-1))))
            return e2e_sorted[idx]
        p50, p95, p99 = quantile(0.50), quantile(0.95), quantile(0.99)

        # CDF de latência E2E
        plt.figure(figsize=(10,6))
        xs = e2e_sorted
        ys = [i/len(xs) for i in range(1, len(xs)+1)]
        plt.plot(xs, ys)
        plt.xlabel("Latência E2E (s)"); plt.ylabel("CDF"); plt.title(f"Latência E2E (p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s)")
        plt.grid(True); plt.tight_layout(); plt.savefig(out_png.replace(".png","_e2e.png")); plt.close()

    print(f"Total req: {len(results)}  OK: {sum(1 for r in results if r['ok'])}  Confirmadas no DB: {confirmed}")
    print(f"Arquivos: {out_csv}, {out_png.replace('.png','_tps.png')}, {out_png.replace('.png','_e2e.png') if e2e_latencies else '(sem e2e)'}")

def parse_args():
    p = argparse.ArgumentParser(description="E2E load: API -> DB (Postgres)")
    p.add_argument("--url", default="http://localhost:8080/payments")
    p.add_argument("--rate", type=int, default=100)
    p.add_argument("--minutes", type=int, default=1)
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--concurrency", type=int, default=400)
    p.add_argument("--pg_dsn", required=True, help="ex: postgresql://user:password@localhost:5432/sd_payments_db")
    p.add_argument("--table", default="payment_entity")
    p.add_argument("--id_column", default="tx_id")
    p.add_argument("--ts_column", default="created_at", help="opcional: coluna TS do DB (ex.: persisted_at)")
    p.add_argument("--db_timeout", type=float, default=60.0, help="tempo máximo para confirmar no DB após envio (s)")
    p.add_argument("--out_png", default="e2e.png")
    p.add_argument("--out_csv", default="e2e.csv")
    return p.parse_args()

def main():
    a = parse_args()
    results, confirmations, start, stop = asyncio.run(
        runner(a.url, a.rate, a.minutes, a.timeout, a.concurrency, a.pg_dsn, a.table, a.id_column, a.db_timeout, a.ts_column)
    )
    analyze(results, confirmations, start, stop, a.out_png, a.out_csv)

if __name__ == "__main__":
    main()
