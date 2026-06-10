"""
Console da plataforma — DADOS EM TEMPO REAL direto do Kafka.

Em vez de consultar o Trino (lento sob streaming pesado), o dashboard consome
diretamente os topicos Kafka e mantem o estado em memoria, atualizando a cada
mensagem que chega. Resultado: streaming de verdade, atualizando ao vivo.

  - issuance_nota_gold  -> estado das notas (KPIs, UF, segmento, ultimas notas)
  - issuance_*_lz/bronze -> contadores de eventos por camada (append)
  - issuance_*_silver    -> chaves distintas por camada (upsert)
  - semantic (dbt)       -> contagem ocasional via Trino (camada batch)

Endpoints:
  GET /                 -> console HTML
  GET /api/console      -> JSON com tudo (calculado do estado em memoria)
  GET /architecture.svg -> diagrama
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid

import requests
from confluent_kafka import Consumer
from flask import Flask, jsonify, send_file

app = Flask(__name__)


@app.after_request
def sem_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, max-age=0, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

ENTIDADES = ["cabecalho", "itens", "impostos", "cliente"]
LZ_TOPICS = [f"issuance_{e}_lz" for e in ENTIDADES]
BRONZE_TOPICS = [f"issuance_{e}_bronze" for e in ENTIDADES]
SILVER_TOPICS = [f"issuance_{e}_silver" for e in ENTIDADES]
GOLD_TOPIC = "issuance_nota_gold"
ALL_TOPICS = LZ_TOPICS + BRONZE_TOPICS + SILVER_TOPICS + [GOLD_TOPIC]

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

# ---- estado em memoria (atualizado pela thread consumidora do Kafka) ----
LOCK = threading.Lock()
STATE = {"lz": 0, "bronze": 0, "seq": 0, "semantic": None}
SILVER_KEYS: set = set()
NOTAS: dict = {}

# status de servicos/flink: atualizado por thread de fundo (fora do request)
SERVICOS_STATUS: list = [{"nome": n, "up": False, "url": u} for (n, _h, _p, u) in SERVICOS]
FLINK_STATUS: list = []


def porta_aberta(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def flink_jobs():
    try:
        r = requests.get("http://flink-jobmanager:8081/jobs/overview", timeout=2)
        jobs = r.json().get("jobs", [])
        return [{"name": j.get("name", "?"), "state": j.get("state", "?")} for j in jobs]
    except Exception:  # noqa: BLE001
        return []


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Consumidor Kafka: mantem o estado em tempo real
# ---------------------------------------------------------------------------
def consumir() -> None:
    while True:
        try:
            c = Consumer(
                {
                    "bootstrap.servers": KAFKA,
                    "group.id": f"dashboard-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            c.subscribe(ALL_TOPICS)
            while True:
                msg = c.poll(1.0)
                if msg is None or msg.error():
                    continue
                topic = msg.topic()
                key = msg.key().decode("utf-8") if msg.key() else None
                raw = msg.value()
                with LOCK:
                    if topic.endswith("_lz"):
                        STATE["lz"] += 1
                    elif topic.endswith("_bronze"):
                        STATE["bronze"] += 1
                    elif topic.endswith("_silver"):
                        if key is not None:
                            if raw is None:
                                SILVER_KEYS.discard((topic, key))
                            else:
                                SILVER_KEYS.add((topic, key))
                    elif topic == GOLD_TOPIC and key is not None:
                        if raw is None:
                            NOTAS.pop(key, None)
                        else:
                            try:
                                rec = json.loads(raw)
                            except json.JSONDecodeError:
                                continue
                            STATE["seq"] += 1
                            rec["_seq"] = STATE["seq"]
                            NOTAS[key] = rec
        except Exception as exc:  # noqa: BLE001
            app.logger.warning("consumidor reiniciando: %s", exc)
            time.sleep(3)


def contar_semantic() -> None:
    """Conta a camada semantic (dbt) via Trino, ocasionalmente (batch)."""
    from trino.dbapi import connect
    while True:
        try:
            conn = connect(
                host=TRINO_HOST, port=TRINO_PORT, user="console", catalog="iceberg",
                session_properties={"query_max_execution_time": "10s"},
            )
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM iceberg.semantic.mart_faturamento_por_uf")
            STATE["semantic"] = int(cur.fetchall()[0][0])
            cur.close()
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)


def monitorar() -> None:
    """Atualiza status de servicos e jobs Flink fora do caminho do request."""
    global SERVICOS_STATUS, FLINK_STATUS
    while True:
        SERVICOS_STATUS = [
            {"nome": n, "up": porta_aberta(h, p), "url": u} for (n, h, p, u) in SERVICOS
        ]
        FLINK_STATUS = flink_jobs()
        time.sleep(3)


threading.Thread(target=consumir, daemon=True).start()
threading.Thread(target=contar_semantic, daemon=True).start()
threading.Thread(target=monitorar, daemon=True).start()


@app.route("/api/console")
def console_data():
    with LOCK:
        registros = {
            "lz": STATE["lz"], "bronze": STATE["bronze"],
            "silver": len(SILVER_KEYS), "gold": len(NOTAS), "semantic": STATE["semantic"],
        }
        vivas = [r for r in NOTAS.values() if r.get("status_nota") != "CANCELADA"]
        uf: dict = {}
        seg: dict = {}
        for r in vivas:
            u = r.get("uf_cliente") or "N/D"
            uf[u] = uf.get(u, 0.0) + num(r.get("valor_total"))
            s = r.get("segmento_cliente") or "N/D"
            seg[s] = seg.get(s, 0.0) + num(r.get("valor_total"))
        recent = sorted(NOTAS.values(), key=lambda r: r.get("_seq", 0), reverse=True)[:12]
        kpi = {
            "qtd_notas": len(vivas),
            "produtos": sum(num(r.get("valor_produtos")) for r in vivas),
            "impostos": sum(num(r.get("valor_impostos")) for r in vivas),
            "faturamento": sum(num(r.get("valor_total")) for r in vivas),
            "itens": sum(num(r.get("qtd_itens")) for r in vivas),
        }
        recentes = [
            [r.get("nota_id"), r.get("nome_cliente") or "?", r.get("uf_cliente") or "--",
             r.get("status_nota") or "?", num(r.get("valor_total")), str(r.get("atualizado_em") or "")]
            for r in recent
        ]

    return jsonify(
        {
            "servicos": SERVICOS_STATUS,
            "camadas": [{"camada": c, "registros": registros[c]} for c in ["lz", "bronze", "silver", "gold", "semantic"]],
            "flink": FLINK_STATUS,
            "kpi": kpi,
            "por_uf": sorted(uf.items(), key=lambda x: -x[1])[:10],
            "por_segmento": sorted(seg.items(), key=lambda x: -x[1])[:8],
            "recentes": recentes,
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
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
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
  <div class="live"><span class="dot"></span> streaming ao vivo (Kafka) · <span id="ts">--</span></div>
</header>
<div class="wrap">
  <div class="card"><h3>Serviços</h3><div class="svc" id="svc"></div></div>

  <div class="card"><h3>Pipeline — registros por camada (tempo real)</h3>
    <div class="flow" id="flow"></div></div>

  <div class="card"><h3>Jobs Flink (Transformation + Persistence)</h3>
    <div id="flink"></div></div>

  <div class="card"><h3>Vendas — KPIs (gold, ao vivo)</h3>
    <div class="kpis">
      <div><div class="l">Notas</div><div class="v" id="k_notas">0</div></div>
      <div><div class="l">Faturamento</div><div class="v" id="k_fat">R$ 0</div></div>
      <div><div class="l">Produtos</div><div class="v" id="k_prod">R$ 0</div></div>
      <div><div class="l">Impostos</div><div class="v" id="k_imp">R$ 0</div></div>
      <div><div class="l">Itens</div><div class="v" id="k_itens">0</div></div>
    </div></div>

  <div class="cols">
    <div class="card"><h3>Faturamento por UF</h3>
      <div style="height:260px"><canvas id="ufChart"></canvas></div></div>
    <div class="card"><h3>Faturamento por Segmento</h3>
      <div style="height:260px"><canvas id="segChart"></canvas></div></div>
    <div class="card"><h3>Últimas notas (upsert ao vivo)</h3><div id="tab"></div></div>
  </div>

  <div class="card"><h3>Arquitetura</h3><img class="arch" src="/architecture.svg" alt="arquitetura"></div>
</div>
<script>
const brl=v=>v.toLocaleString('pt-BR',{style:'currency',currency:'BRL',maximumFractionDigits:0});
let ufC,segC;
function mkChart(id,color){ if(typeof Chart==='undefined') return null;
  return new Chart(document.getElementById(id),{type:'bar',
    data:{labels:[],datasets:[{data:[],backgroundColor:color,borderRadius:4}]},
    options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,animation:false,
      plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>brl(c.parsed.x)}}},
      scales:{x:{ticks:{color:'#8a93b8',callback:v=>'R$'+Math.round(v/1000)+'k'},grid:{color:'#232a47'}},
              y:{ticks:{color:'#e7ecff'},grid:{display:false}}}}});}
function updChart(c,rows){ if(!c||!rows) return; c.data.labels=rows.map(r=>r[0]); c.data.datasets[0].data=rows.map(r=>r[1]); c.update('none'); }
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
    rows.map(r=>`<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td><span class="tag ${r[3]}">${r[3]}</span></td><td class="right">${brl(r[4])}</td><td>${(r[5]||'').substring(11,19)}</td></tr>`).join('')+`</tbody></table>`;}
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
  if(!ufC){ufC=mkChart('ufChart','#5b8cff');segC=mkChart('segChart','#a96fb0');}
  updChart(ufC,d.por_uf); updChart(segC,d.por_segmento);
  document.getElementById('tab').innerHTML=tab(d.recentes);
  document.getElementById('ts').textContent=new Date().toLocaleTimeString('pt-BR');
}catch(e){console.error(e);}}
tick();setInterval(tick,1500);
</script></body></html>
"""


@app.route("/")
def index():
    return PAGINA


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, threaded=True)
