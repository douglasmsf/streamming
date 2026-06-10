"""
REST API de serving (camada de consumo da arquitetura).

Espelha o lado direito do desenho (REST API + IDP oAuth2): expoe a camada
SEMANTIC (gerada pelo dbt) atraves de endpoints REST protegidos por um
fluxo OAuth2 Client Credentials simulado (emite um JWT local).

Fluxo:
  1) POST /oauth/token  (client_id + client_secret)  -> { access_token (JWT) }
  2) GET  /api/v1/...   com header  Authorization: Bearer <token>

Exemplo:
  TOKEN=$(curl -s -XPOST localhost:8060/oauth/token \
    -d client_id=potencial -d client_secret=secret | jq -r .access_token)
  curl -s localhost:8060/api/v1/faturamento/uf -H "Authorization: Bearer $TOKEN"
"""

from __future__ import annotations

import os
import time
from functools import wraps

import jwt
from flask import Flask, jsonify, request
from trino.dbapi import connect

app = Flask(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
CLIENT_ID = os.getenv("OAUTH_CLIENT_ID", "potencial")
CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET", "secret")
JWT_SECRET = os.getenv("JWT_SECRET", "troque-este-segredo")
TOKEN_TTL = int(os.getenv("TOKEN_TTL_SECONDS", "3600"))
SEMANTIC = "iceberg.semantic"


def query(sql: str):
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="serving", catalog="iceberg")
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def requer_token(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "missing_bearer_token"}), 401
        try:
            jwt.decode(auth[7:], JWT_SECRET, algorithms=["HS256"])
        except jwt.PyJWTError:
            return jsonify({"error": "invalid_token"}), 401
        return fn(*args, **kwargs)

    return wrapper


@app.post("/oauth/token")
def token():
    """OAuth2 Client Credentials (simulado) -> emite um JWT."""
    cid = request.form.get("client_id") or (request.json or {}).get("client_id")
    secret = request.form.get("client_secret") or (request.json or {}).get("client_secret")
    if cid != CLIENT_ID or secret != CLIENT_SECRET:
        return jsonify({"error": "invalid_client"}), 401
    now = int(time.time())
    payload = {"sub": cid, "scope": "semantic:read", "iat": now, "exp": now + TOKEN_TTL}
    access = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return jsonify({"access_token": access, "token_type": "Bearer", "expires_in": TOKEN_TTL})


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/v1/faturamento/uf")
@requer_token
def faturamento_uf():
    return jsonify(query(f"SELECT * FROM {SEMANTIC}.mart_faturamento_por_uf ORDER BY faturamento DESC"))


@app.get("/api/v1/faturamento/cliente")
@requer_token
def faturamento_cliente():
    return jsonify(
        query(f"SELECT * FROM {SEMANTIC}.mart_faturamento_por_cliente ORDER BY faturamento DESC LIMIT 100")
    )


@app.get("/api/v1/produtos/ranking")
@requer_token
def ranking_produtos():
    return jsonify(query(f"SELECT * FROM {SEMANTIC}.mart_ranking_produtos LIMIT 100"))


@app.get("/api/v1/impostos")
@requer_token
def impostos():
    return jsonify(query(f"SELECT * FROM {SEMANTIC}.mart_resumo_impostos"))


@app.get("/api/v1/vendas/diarias")
@requer_token
def vendas_diarias():
    return jsonify(query(f"SELECT * FROM {SEMANTIC}.mart_vendas_diarias ORDER BY data_emissao_dia DESC"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8060)
