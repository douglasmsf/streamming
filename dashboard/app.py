"""
Console unico da plataforma (pagina web "tudo num lugar").

Mostra, numa unica pagina que se atualiza sozinha:
  - status (up/down) de cada servico (Kafka, Flink, Trino, Airflow, MinIO,
    Iceberg REST, REST API de serving);
  - contagem de registros por CAMADA (lz -> bronze -> silver -> gold -> semantic);
  - os JOBS Flink rodando (Transformation + Persistence);
  - KPIs e dados de venda em TEMPO REAL (faturamento, UF, segmento, ultimas notas);
  - o diagrama da arquitetura;
  - links para abrir cada UI.

Endpoints:
  GET /                 -> console HTML
  GET /api/console      -> JSON com tudo
  GET /architecture.svg -> diagrama
"""

from __future__ import annotations

import os
import socket

import requests
from flask import Flask, jsonify, send_file
from trino.dbapi import connect

app = Flask(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

# servico -> (host interno, porta, url externa p/ abrir no navegador)
SERVICOS = [
    ("Kafka", "kafka", 9092, "http://localhost:8088"),
    ("Kafka UI", "kafka-ui", 8080, "http://localhost:8088"),
    ("Flink", "flink-jobmanager", 8081, "http://localhost:8081"),
    ("Trino", "trino", 8080, "http://localhost:8080"),
    ("Airflow", "airflow", 8080, "http://localhost:8082"),
    ("MinIO", "minio", 9000, "http://localhost:9001"),
    ("Iceberg REST", "iceberg-rest", 8181, "http://localhost:8181"),
    ("REST API", "serving", 8060, "http://localhost:8060/health"),
]

# camada -> tabela representativa para contar registros
CAMADAS = [
    ("lz", "iceberg.lz.cabecalho"),
    ("bronze", "iceberg.bronze.cabecalho"),
    ("silver", "iceberg.silver.cabecalho"),
    ("gold", "iceberg.gold.nota_fiscal"),
    ("semantic", "iceberg.semantic.mart_faturamento_por_uf"),
]


def porta_aberta(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def run_query(sql: str):
    try:
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="console", catalog="iceberg")
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception:  # noqa: BLE001
        return None


def scalar(sql: str):
    rows = run_query(sql)
    if rows is None:
        return None
    return rows[0][0] if rows else 0


def f(v) -> float:
    return float(v) if v is not None else 0.0


def flink_jobs():
    try:
        r = requests.get("http://flink-jobmanager:8081/jobs/overview", timeout=2)
        jobs = r.json().get("jobs", [])
        return [{"name": j.get("name", "?"), "state": j.get("state", "?")} for j in jobs]
    except Exception:  # noqa: BLE001
        return []


@app.route("/api/console")
def console_data():
    GOLD = "iceberg.gold.nota_fiscal"

    servicos = [
        {"nome": n, "up": porta_aberta(h, p), "url": u} for (n, h, p, u) in SERVICOS
    ]

    camadas = []
    for nome, tabela in CAMADAS:
        c = scalar(f"SELECT count(*) FROM {tabela}")
        camadas.append({"camada": nome, "registros": int(c) if c is not None else None})

    kpi = run_query(
        f"""SELECT count(*), coalesce(sum(valor_produtos),0), coalesce(sum(valor_impostos),0),
                   coalesce(sum(valor_total),0), coalesce(sum(qtd_itens),0)
            FROM {GOLD} WHERE status_nota <> 'CANCELADA'"""
    )
    k = kpi[0] if kpi else (0, 0, 0, 0, 0)

    por_uf = run_query(
        f"""SELECT coalesce(uf_cliente,'N/D'), sum(valor_total) FROM {GOLD}
            WHERE status_nota <> 'CANCELADA' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"""
    ) or []
    por_seg = run_query(
        f"""SELECT coalesce(segmento_cliente,'N/D'), sum(valor_total) FROM {GOLD}
            WHERE status_nota <> 'CANCELADA' GROUP BY 1 ORDER BY 2 DESC LIMIT 8"""
    ) or []
    recentes = run_query(
        f"""SELECT nota_id, coalesce(nome_cliente,'?'), coalesce(uf_cliente,'--'),
                   status_nota, coalesce(valor_total,0), atualizado_em
            FROM {GOLD} ORDER BY atualizado_em DESC LIMIT 12"""
    ) or []

    return jsonify(
        {
            "servicos": servicos,
            "camadas": camadas,
            "flink": flink_jobs(),
            "kpi": {
                "qtd_notas": int(k[0]), "produtos": f(k[1]), "impostos": f(k[2]),
                "faturamento": f(k[3]), "itens": f(k[4]),
            },
            "por_uf": [[r[0], f(r[1])] for r in por_uf],
            "por_segmento": [[r[0], f(r[1])] for r in por_seg],
            "recentes": [[r[0], r[1], r[2], r[3], f(r[4]), str(r[5])] for r in recentes],
        }
    )


@app.route("/architecture.svg")
def arquitetura():
    caminho = "/app/docs/arquitetura.svg"
    if os.path.exists(caminho):
        return send_file(caminho, mimetype="image/svg+xml")
    return "diagrama nao montado", 404


PAGINA = """
<!doctype html><html lang="pt-br"><head><meta charset="utf-8">
<title>Plataforma de Streaming — Console</title>
<style>
  :root{--bg:#0b1020;--card:#151b33;--ink:#e7ecff;--mut:#8a93b8;--ac:#5b8cff;
        --gr:#36d399;--am:#fbbd23;--rd:#f87272;--pp:#a96fb0;}
  *{box-sizing:border-box}
  body{margin:0;font-family:Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--ink)}
  header{display:flex;align-items:center;justify-content:space-between;padding:14px 24px;border-bottom:1px solid #232a47}
  header h1{font-size:17px;margin:0;letter-spacing:.5px}
  .live{display:flex;align-items:center;gap:8px;color:var(--mut);font-size:13px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--gr);animation:p 1.6s infinite}
  @keyframes p{0%{box-shadow:0 0 0 0 rgba(54,211,153,.6)}70%{box-shadow:0 0 0 9px rgba(54,211,153,0)}100%{box-shadow:0 0 0 0 rgba(54,211,153,0)}}
  .wrap{padding:18px 24px;display:grid;gap:16px}
  h3{margin:0 0 10px;font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.6px}
  .card{background:var(--card);border:1px solid #232a47;border-radius:14px;padding:14px}
  .svc{display:grid;grid-template-columns:repeat(8,1fr);gap:10px}
  .svc a{display:block;text-decoration:none;color:var(--ink);background:#0e1430;border:1px solid #232a47;border-radius:10px;padding:10px}
  .svc .n{font-size:12px;font-weight:700}
  .svc .s{font-size:10.5px;color:var(--mut);display:flex;align-items:center;gap:6px;margin-top:4px}
  .sd{width:8px;height:8px;border-radius:50%}
  .flow{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
  .stage{background:#0e1430;border:1px solid #232a47;border-radius:10px;padding:10px 14px;min-width:120px}
  .stage .c{font-size:22px;font-weight:800}
  .stage .l{font-size:11px;color:var(--mut);text-transform:uppercase}
  .arrow{color:var(--mut);font-size:20px}
  .kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}
  .kpi .l{color:var(--mut);font-size:11px;text-transform:uppercase}
  .kpi .v{font-size:23px;font-weight:800;margin-top:4px}
  .cols{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px}
  .bar{display:flex;align-items:center;gap:8px;margin:6px 0;font-size:12px}
  .bar .name{width:90px}.bar .track{flex:1;background:#0e1430;border-radius:6px;height:16px;overflow:hidden}
  .bar .fill{height:100%;background:linear-gradient(90deg,var(--ac),#9b6bff)}.bar .val{width:96px;text-align:right;color:var(--mut)}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #232a47}
  th{color:var(--mut);font-size:10.5px;text-transform:uppercase}
  .tag{padding:2px 7px;border-radius:20px;font-size:10.5px}
  .AUTORIZADA,.EMITIDA{background:rgba(54,211,153,.15);color:var(--gr)}.CANCELADA{background:rgba(248,114,114,.15);color:var(--rd)}
  .RUNNING{color:var(--gr)}.FINISHED{color:var(--mut)}.FAILED,.CANCELED{color:var(--rd)}
  .empty{color:var(--mut);padding:18px;text-align:center}
  .right{text-align:right}
  img.arch{width:100%;border-radius:10px;background:#0b1020}
</style></head><body>
<header>
  <h1>🛰️ PLATAFORMA DE STREAMING CDC — CONSOLE</h1>
  <div class="live"><span class="dot"></span> ao vivo · <span id="ts">--</span></div>
</header>
<div class="wrap">
  <div class="card"><h3>Serviços</h3><div class="svc" id="svc"></div></div>

  <div class="card"><h3>Pipeline — registros por camada (tempo real)</h3>
    <div class="flow" id="flow"></div></div>

  <div class="card"><h3>Jobs Flink (Transformation + Persistence)</h3>
    <div id="flink"></div></div>

  <div class="card"><h3>Vendas — KPIs (gold)</h3>
    <div class="kpis">
      <div><div class="l">Notas</div><div class="v" id="k_notas">0</div></div>
      <div><div class="l">Faturamento</div><div class="v" id="k_fat">R$ 0</div></div>
      <div><div class="l">Produtos</div><div class="v" id="k_prod">R$ 0</div></div>
      <div><div class="l">Impostos</div><div class="v" id="k_imp">R$ 0</div></div>
      <div><div class="l">Itens</div><div class="v" id="k_itens">0</div></div>
    </div></div>

  <div class="cols">
    <div class="card"><h3>Faturamento por UF</h3><div id="uf"></div></div>
    <div class="card"><h3>Faturamento por Segmento</h3><div id="seg"></div></div>
    <div class="card"><h3>Últimas notas (upsert ao vivo)</h3><div id="tab"></div></div>
  </div>

  <div class="card"><h3>Arquitetura</h3><img class="arch" src="/architecture.svg" alt="arquitetura"></div>
</div>
<script>
const brl=v=>v.toLocaleString('pt-BR',{style:'currency',currency:'BRL',maximumFractionDigits:0});
function bars(rows){if(!rows||!rows.length)return '<div class="empty">aguardando dados…</div>';
  const mx=Math.max(...rows.map(r=>r[1]),1);
  return rows.map(r=>`<div class="bar"><span class="name">${r[0]}</span><span class="track"><span class="fill" style="width:${(r[1]/mx*100).toFixed(1)}%"></span></span><span class="val">${brl(r[1])}</span></div>`).join('');}
function svc(rows){return rows.map(s=>`<a href="${s.url}" target="_blank"><div class="n">${s.nome}</div>
  <div class="s"><span class="sd" style="background:${s.up?'var(--gr)':'var(--rd)'}"></span>${s.up?'no ar':'offline'}</div></a>`).join('');}
function flow(rows){const ic={lz:'#8a93b8',bronze:'#b08a2e',silver:'#a96fb0',gold:'#2f9e76',semantic:'#5b8cff'};
  return rows.map((c,i)=>`<div class="stage" style="border-color:${ic[c.camada]}"><div class="l">${c.camada}</div>
    <div class="c">${c.registros===null?'—':c.registros.toLocaleString('pt-BR')}</div></div>${i<rows.length-1?'<span class="arrow">→</span>':''}`).join('');}
function flink(rows){if(!rows||!rows.length)return '<div class="empty">nenhum job submetido ainda — rode: docker compose run --rm flink-sql-submit</div>';
  return `<table><thead><tr><th>Job</th><th>Estado</th></tr></thead><tbody>`+
    rows.map(j=>`<tr><td>${j.name}</td><td class="${j.state}">${j.state}</td></tr>`).join('')+`</tbody></table>`;}
function tab(rows){if(!rows||!rows.length)return '<div class="empty">aguardando dados…</div>';
  return `<table><thead><tr><th>Nota</th><th>Cliente</th><th>UF</th><th>Status</th><th class="right">Total</th><th>Hora</th></tr></thead><tbody>`+
    rows.map(r=>`<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td><span class="tag ${r[3]}">${r[3]}</span></td><td class="right">${brl(r[4])}</td><td>${r[5].substring(11,19)}</td></tr>`).join('')+`</tbody></table>`;}
async function tick(){try{
  const d=await(await fetch('/api/console')).json();
  document.getElementById('svc').innerHTML=svc(d.servicos);
  document.getElementById('flow').innerHTML=flow(d.camadas);
  document.getElementById('flink').innerHTML=flink(d.flink);
  document.getElementById('k_notas').textContent=d.kpi.qtd_notas.toLocaleString('pt-BR');
  document.getElementById('k_fat').textContent=brl(d.kpi.faturamento);
  document.getElementById('k_prod').textContent=brl(d.kpi.produtos);
  document.getElementById('k_imp').textContent=brl(d.kpi.impostos);
  document.getElementById('k_itens').textContent=Math.round(d.kpi.itens).toLocaleString('pt-BR');
  document.getElementById('uf').innerHTML=bars(d.por_uf);
  document.getElementById('seg').innerHTML=bars(d.por_segmento);
  document.getElementById('tab').innerHTML=tab(d.recentes);
  document.getElementById('ts').textContent=new Date().toLocaleTimeString('pt-BR');
}catch(e){console.error(e);}}
tick();setInterval(tick,3000);
</script></body></html>
"""


@app.route("/")
def index():
    return PAGINA


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
