#!/usr/bin/env python3
import argparse, asyncio, time, math, csv, uuid, statistics, itertools
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
    sql = f"SELECT {id_column}" + (f", {ts_column}" if ts_column else "") + f" FROM {table} WHERE {id_column} = ANY($1)"
    end = now() + timeout_s
    seen = {}
    while now() < end and len(seen) < len(ids_batch):
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, list(ids_batch))
        for r in rows:
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

async def send_one(client, url, body, timeout, idempotency_header=None, idempotency_value=None):
    t0 = now()
    headers = {}
    if idempotency_header and idempotency_value:
        headers[idempotency_header] = idempotency_value
    try:
        r = await client.post(url, json=body, timeout=timeout, headers=headers)
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

async def run_phase(phase_seconds, rate, client, urls_cycle, timeout, sem, results, inflight, idempotency_header):
    start = now()
    with tqdm(total=phase_seconds, desc="Segundos", unit="s") as bar:
        for sec in range(phase_seconds):
            tasks = []
            for _ in range(rate):
                url = next(urls_cycle)
                body = dict(DEFAULT_BODY)
                client_tx = str(uuid.uuid4())
                body.setdefault("clientTxId", client_tx)
                idem_value = client_tx  # 1-para-1 por request
                async def one(selected_url=url, idem=idem_value, payload=body):
                    async with sem:
                        res = await send_one(client, selected_url, payload, timeout, idempotency_header=idempotency_header, idempotency_value=idem)
                        tid = res["txId"] or payload["clientTxId"]
                        res["txId"] = tid
                        if tid:
                            inflight.append((tid, res["t0"]))
                        results.append(res)
                tasks.append(asyncio.create_task(one()))
            await asyncio.gather(*tasks)
            bar.update(1)
            # pace 1s
            target = start + sec + 1
            delay = target - now()
            if delay > 0:
                await asyncio.sleep(delay)

async def runner(urls, rate, minutes, timeout, concurrency, dsn, table, id_column, db_timeout, ts_column, warmup_s):
    duration_s = minutes * 60
    results = []
    async with httpx.AsyncClient() as client, await ensure_pg(dsn) as pool:
        sem = asyncio.Semaphore(concurrency)
        inflight = deque()  # (txId, t0)
        urls_cycle = itertools.cycle(urls)

        # Warmup (sem contagem para métricas de E2E — mas registramos para TPS do servidor “esquentar”)
        if warmup_s > 0:
            tmp_results = []
            tmp_inflight = deque()
            await run_phase(warmup_s, rate, client, urls_cycle, timeout, sem, tmp_results, tmp_inflight, idempotency_header=None)

        start = now()
        await run_phase(duration_s, rate, client, urls_cycle, timeout, sem, results, inflight, idempotency_header=None)

        # Confirmação em lotes até db_timeout
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
                    inflight.append((tid, t0))
        while inflight:
            tid, _ = inflight.popleft()
            confirmations[tid] = (False, None, None)

    stop = now()
    return results, confirmations, start, stop

def analyze(results, confirmations, start, stop, out_png_prefix, out_csv):
    # TPS por segundo
    buckets = defaultdict(list)
    for r in results:
        sec = int(math.floor(r["t1"] - start))
        buckets[sec].append(r)
    secs = list(range(int(max(0, math.ceil(stop - start))) + 1))
    tps_total = [len(buckets.get(s, [])) for s in secs]
    tps_ok = [sum(1 for r in buckets.get(s, []) if r["ok"]) for s in secs]
    tps_fail = [sum(1 for r in buckets.get(s, []) if not r["ok"]) for s in secs]

    # Latências E2E (request_start -> visto no DB pelo cliente)
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
    plt.legend(); plt.grid(True); plt.tight_layout(); plt.savefig(out_png_prefix + "_tps.png"); plt.close()

    p50 = p95 = p99 = None
    if e2e_latencies:
        xs = sorted(e2e_latencies)
        def q(x): 
            if not xs: return None
            pos = max(0, min(len(xs)-1, int(x * (len(xs)-1))))
            return xs[pos]
        p50, p95, p99 = q(0.50), q(0.95), q(0.99)

        # CDF de latência E2E
        plt.figure(figsize=(10,6))
        ys = [i/len(xs) for i in range(1, len(xs)+1)]
        plt.plot(xs, ys)
        plt.xlabel("Latência E2E (s)"); plt.ylabel("CDF"); plt.title(f"Latência E2E (p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s)")
        plt.grid(True); plt.tight_layout(); plt.savefig(out_png_prefix + "_e2e.png"); plt.close()

    summary = {
        "total_req": len(results),
        "ok_req": sum(1 for r in results if r["ok"]),
        "fail_req": sum(1 for r in results if not r["ok"]),
        "confirmed_db": confirmed,
        "tps_resp_mean": (sum(tps_total)/len(tps_total)) if tps_total else 0,
        "tps_resp_max": max(tps_total) if tps_total else 0,
        "ok_ratio": (sum(1 for r in results if r["ok"]) / len(results)) if results else 0.0,
        "db_confirm_ratio": (confirmed / len(results)) if results else 0.0,
        "p50": p50, "p95": p95, "p99": p99
    }

    print(f"Total req: {summary['total_req']}  OK: {summary['ok_req']}  Confirmadas no DB: {summary['confirmed_db']}")
    print(f"TPS médio: {summary['tps_resp_mean']:.2f}  TPS máx: {summary['tps_resp_max']}")
    if p99 is not None:
        print(f"Lat E2E p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s")
    print(f"Arquivos: {out_csv}, {out_png_prefix}_tps.png, {out_png_prefix}_e2e.png" if p99 is not None else f"Arquivos: {out_csv}, {out_png_prefix}_tps.png (sem e2e)")

    return summary

def parse_args():
    p = argparse.ArgumentParser(description="E2E load: API -> Kafka -> Persister -> Postgres")
    p.add_argument("--url", default="http://localhost:8080/payments", help="(obsoleto se usar --urls)")
    p.add_argument("--urls", default=None, help="Lista de URLs separadas por vírgula para round-robin")
    p.add_argument("--rate", type=int, default=100)
    p.add_argument("--sweep", default=None, help="Lista de rates, ex.: 50,100,200,400")
    p.add_argument("--minutes", type=int, default=1)
    p.add_argument("--warmup_s", type=int, default=0, help="segundos de aquecimento antes de medir")
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--concurrency", type=int, default=400)
    p.add_argument("--pg_dsn", required=True, help="ex: postgresql://user:password@localhost:5432/sd_payments_db")
    p.add_argument("--table", default="payment_entity")
    p.add_argument("--id_column", default="tx_id")
    p.add_argument("--ts_column", default="created_at", help="opcional: coluna TS do DB (ex.: persisted_at)")
    p.add_argument("--db_timeout", type=float, default=60.0, help="tempo máximo para confirmar no DB após envio (s)")
    p.add_argument("--idempotency_header", default=None, help="Nome do header p/ idempotência (opcional)")
    p.add_argument("--out_prefix", default="e2e")  # usado em sweep; no modo single, vira prefixo
    p.add_argument("--out_png", default=None)      # compat: se fornecido, usa para o modo single
    p.add_argument("--out_csv", default=None)      # compat: se fornecido, usa para o modo single
    return p.parse_args()

def run_single(urls, a, label_suffix=""):
    results, confirmations, start, stop = asyncio.run(
        runner(urls, a.rate, a.minutes, a.timeout, a.concurrency, a.pg_dsn, a.table, a.id_column, a.db_timeout, a.ts_column, a.warmup_s)
    )
    prefix = a.out_prefix + (f"_{label_suffix}" if label_suffix else "")
    out_png = a.out_png if a.out_png else f"{prefix}"
    out_csv = a.out_csv if a.out_csv else f"{prefix}.csv"
    return analyze(results, confirmations, start, stop, out_png, out_csv)

def main():
    a = parse_args()
    urls = [u.strip() for u in (a.urls.split(",") if a.urls else [a.url]) if u.strip()]
    if a.sweep:
        rates = [int(x.strip()) for x in a.sweep.split(",") if x.strip()]
        summary_rows = []
        for r in rates:
            a.rate = r
            label = f"rate{r}"
            s = run_single(urls, a, label_suffix=label)
            summary_rows.append({
                "rate": r,
                **s
            })
        # CSV consolidado + gráficos de scaling
        summary_csv = f"{a.out_prefix}_sweep_summary.csv"
        with open(summary_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rate","tps_resp_mean","tps_resp_max","ok_ratio","db_confirm_ratio","p50","p95","p99","total_req","ok_req","fail_req","confirmed_db"])
            for row in summary_rows:
                w.writerow([
                    row["rate"], f'{row["tps_resp_mean"]:.4f}', row["tps_resp_max"],
                    f'{row["ok_ratio"]:.4f}', f'{row["db_confirm_ratio"]:.4f}',
                    f'{row["p50"]:.6f}' if row["p50"] is not None else "",
                    f'{row["p95"]:.6f}' if row["p95"] is not None else "",
                    f'{row["p99"]:.6f}' if row["p99"] is not None else "",
                    row["total_req"], row["ok_req"], row["fail_req"], row["confirmed_db"]
                ])

        # Plots: TPS vs rate / p99 vs rate
        rates_sorted = sorted(summary_rows, key=lambda x: x["rate"])
        xs = [r["rate"] for r in rates_sorted]
        ys_mean = [r["tps_resp_mean"] for r in rates_sorted]
        ys_max = [r["tps_resp_max"] for r in rates_sorted]
        ys_p99 = [r["p99"] if r["p99"] is not None else float('nan') for r in rates_sorted]

        plt.figure(figsize=(10,6))
        plt.plot(xs, ys_mean, marker="o", label="TPS resposta (médio)")
        plt.plot(xs, ys_max, marker="x", linestyle="--", label="TPS resposta (máx)")
        plt.xlabel("Rate solicitado (req/s)"); plt.ylabel("TPS resposta (req/s)"); plt.title("Escalonamento: Rate vs TPS atingido")
        plt.legend(); plt.grid(True); plt.tight_layout(); plt.savefig(f"{a.out_prefix}_sweep_tps_vs_rate.png"); plt.close()

        plt.figure(figsize=(10,6))
        plt.plot(xs, ys_p99, marker="o")
        plt.xlabel("Rate solicitado (req/s)"); plt.ylabel("Latência E2E p99 (s)"); plt.title("Escalonamento: Rate vs p99 E2E")
        plt.grid(True); plt.tight_layout(); plt.savefig(f"{a.out_prefix}_sweep_p99_vs_rate.png"); plt.close()

        print(f"Sweep salvo em: {summary_csv}, {a.out_prefix}_sweep_tps_vs_rate.png, {a.out_prefix}_sweep_p99_vs_rate.png")
    else:
        run_single(urls, a)

if __name__ == "__main__":
    main()
