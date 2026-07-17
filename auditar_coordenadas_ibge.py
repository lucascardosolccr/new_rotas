#!/usr/bin/env python3
"""
auditar_coordenadas_ibge.py  —  Auditoria sistêmica de coordenadas da base de municípios.

POR QUE ISSO EXISTE
-------------------
A base viva de municípios (kelvins/municipios-brasileiros e seus forks, ex. abralvs) traz o CÓDIGO IBGE
correto, mas para alguns municípios uma COORDENADA (sede) ERRADA. Como a geodésica de Karney E a rota do
OSRM saem da MESMA coordenada, um erro de coordenada produz uma rota curta "fisicamente consistente" que
NENHUMA barreira do app pega (foi o caso Alto Paraíso/PR → Pato Branco/PR: 18 km em vez de ~439 km).

Um detector INGÊNUO ("sede longe do centróide") NÃO serve: municípios amazônicos enormes têm a sede
ribeirinha legitimamente longe do centro geométrico. O discriminador correto e à prova de falso-positivo:

    ERRO REAL  ⇔  a sede cai FORA do polígono oficial do próprio município
                  E DENTRO do polígono de OUTRO município distante.

Fontes independentes:
  • Base viva  : https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/json/municipios.json
  • Geometria  : https://raw.githubusercontent.com/tbrugz/geodata-br/master/geojson/geojs-100-mun.json
                 (malha municipal oficial do IBGE — NÃO deriva das bases de lat/lon, então é auditoria real)

COMO USAR
---------
    python3 auditar_coordenadas_ibge.py
Ele imprime os erros confirmados e um bloco Python pronto para colar/atualizar `_CORRECOES_COORDENADA_IBGE`
no app. Rode-o sempre que atualizar a base de municípios. A correção em si continua EXPLÍCITA e AUDITÁVEL
(tabela curada aplicada na carga) — nada é corrigido "no escuro" em produção.

Requer: requests (ou urllib). Sem dependência de shapely (ray casting embutido).
"""
import json, math, sys, urllib.request

URL_BASE = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/json/municipios.json"
URL_GEO  = "https://raw.githubusercontent.com/tbrugz/geodata-br/master/geojson/geojs-100-mun.json"
LIMIAR_KM = 50.0   # só investiga divergências grandes; erros reais achados foram de 163–769 km


def _baixar(url):
    with urllib.request.urlopen(url, timeout=90) as r:
        return json.loads(r.read().decode("utf-8-sig"))


def _rings(geom):
    t, c = geom.get("type"), geom.get("coordinates")
    if t == "Polygon" and c:
        return [c[0]]
    if t == "MultiPolygon":
        return [p[0] for p in c if p]
    return []


def _pip(lat, lon, ring):
    inside, n, j = False, len(ring), len(ring) - 1
    for i in range(n):
        xi, yi = ring[i]; xj, yj = ring[j]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def _hav(a, b):
    R = 6371.0088
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dp, dl = math.radians(b[0] - a[0]), math.radians(b[1] - a[1])
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.asin(math.sqrt(x))


def main():
    print("Baixando base viva e malha oficial do IBGE...", file=sys.stderr)
    base = _baixar(URL_BASE)
    geo = _baixar(URL_GEO)

    feats = []
    for f in geo["features"]:
        rs = _rings(f.get("geometry") or {})
        if not rs:
            continue
        pts = [p for r in rs for p in r]
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        centro = (sum(ys) / len(ys), sum(xs) / len(xs))
        feats.append((str(f["properties"].get("id")), f["properties"].get("name"), rs,
                      (min(xs), max(xs), min(ys), max(ys)), centro))
    by_code = {c[0]: c for c in feats}

    def contem(lat, lon):
        for cod, nome, rs, (xmn, xmx, ymn, ymx), _ in feats:
            if xmn <= lon <= xmx and ymn <= lat <= ymx and any(_pip(lat, lon, r) for r in rs):
                return cod, nome
        return None, None

    confirmados = []
    for r in base:
        cod = str(r.get("codigo_ibge")); lat, lon = r.get("latitude"), r.get("longitude")
        if cod not in by_code or lat is None:
            continue
        _, _, rs, _, centro = by_code[cod]
        if any(_pip(lat, lon, rr) for rr in rs):
            continue                       # sede dentro do próprio polígono → OK
        if _hav((lat, lon), centro) <= LIMIAR_KM:
            continue                       # divergência pequena (borda/costa/simplificação) → ignora
        ocod, onome = contem(lat, lon)     # a sede cai dentro de OUTRO município?
        if ocod and ocod != cod:
            confirmados.append((cod, r.get("nome"), r.get("codigo_uf"), (lat, lon), centro, onome,
                                _hav((lat, lon), centro)))

    print(f"\n=== {len(confirmados)} ERRO(S) DE COORDENADA CONFIRMADO(S) ===")
    for cod, nome, cuf, sede, centro, onome, d in sorted(confirmados, key=lambda x: -x[6]):
        print(f"  {cod}  {nome} (UF {cuf}): sede {sede[0]:.4f},{sede[1]:.4f} caía em '{onome}' "
              f"({d:.0f} km) → centróide {centro[0]:.4f},{centro[1]:.4f}")

    print("\n# --- cole/atualize em _CORRECOES_COORDENADA_IBGE (revise cada linha) ---")
    for cod, nome, cuf, sede, centro, onome, d in sorted(confirmados, key=lambda x: -x[6]):
        print(f'    "{cod}": {{"lat": {centro[0]:.5f}, "lon": {centro[1]:.5f}, "nome": {json.dumps(nome, ensure_ascii=False)}, '
              f'"uf": "?", "fonte": "centróide do polígono oficial IBGE — corrige {sede[0]:.3f},{sede[1]:.3f} '
              f'({d:.0f} km, caía em {onome})"}},')
    print("\nObs.: para a SEDE exata (melhor alvo de rota que o centróide em municípios irregulares), "
          "confirme a coordenada oficial no IBGE/Wikipédia antes de fixar.")


if __name__ == "__main__":
    main()
