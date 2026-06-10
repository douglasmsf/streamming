# CDC, updates e a "inteligência" de upsert

Um requisito central: **os dados que chegam podem ser alterações de registros
já enviados**. O pipeline precisa convergir para o estado atual sem duplicar.

## Como as alterações são geradas

O `cdc-generator` mantém em memória os registros já emitidos. Com probabilidade
`CDC_UPDATE_RATIO` (padrão 0.35), em vez de criar um registro novo ele
**re-emite um registro existente** com:

- a **mesma chave primária**;
- `op = "U"`;
- um **`lsn` maior** (a mudança é sempre posterior ao insert);
- algum campo alterado (ex.: `status` da nota, `quantidade` do item,
  `segmento` do cliente).

## Por que a chave da mensagem Kafka importa

O `folder-producer` publica cada evento usando a **PK como chave Kafka**. Isso
garante que todas as versões de um mesmo registro caiam na **mesma partição, em
ordem**, e habilita a semântica de **changelog/compaction**.

## Tratamento por camada

### Bronze — histórico (append)
Conector `kafka` comum. **Todo** evento vira uma linha (inclusive os updates).
Serve como **auditoria**: dá para ver a evolução completa de cada nota.

```sql
-- exemplo: quantas mudancas cada nota recebeu
SELECT nota_id, count(*) FROM iceberg.bronze.venda_cabecalho GROUP BY nota_id;
```

### Silver — estado atual (upsert + dedup)
Conector `upsert-kafka`: o Flink lê o tópico como **changelog por PK** e mantém
sempre a **última versão** de cada registro — isto é a **deduplicação**. O sink
Iceberg usa `format-version=2` + `write.upsert.enabled=true`, gravando
*equality deletes* + dados para refletir a versão corrente.

> Resultado: se uma nota foi inserida e depois teve o `status` alterado para
> `CANCELADA`, a silver mostra **apenas** a versão cancelada (1 linha), enquanto
> a bronze mostra as **duas** versões (histórico).

### Gold — consolidação reativa
As fontes da gold também são `upsert-kafka` (changelogs). Os agregados de itens
e impostos são `GROUP BY nota_id`, e o resultado é juntado ao cabeçalho e ao
cliente. Como tudo é changelog, **qualquer alteração** em um item, imposto,
cabeçalho ou cliente **recalcula a linha da nota** e faz upsert na gold.

## Sobre deletes

Para manter o exemplo simples e idempotente, não há *hard delete*: um
cancelamento é modelado como **update de `status` para `CANCELADA`**
(soft delete). As consultas analíticas filtram `status_nota <> 'CANCELADA'`.
Caso queira deletes físicos, basta o producer enviar uma mensagem com **valor
nulo** (tombstone) na chave correspondente — o `upsert-kafka` interpreta como
remoção e o Iceberg propaga o delete.

## Garantia de commit no Iceberg

O sink Iceberg do Flink **só materializa** os arquivos Parquet **no checkpoint**.
Por isso o checkpointing está habilitado (`execution.checkpointing.interval:
30s`). Ou seja: após subir os jobs, os dados aparecem no Trino a cada ~30s.
