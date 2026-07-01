# ==============================================================================
# VERSÃO: 3.8
# DATA: 2026-06
# DESCRIÇÃO: Motor Nacional de Roteirização Inteligente — Plataforma Corporativa B2B
#
# ==============================================================================
# MAPA DE ARQUITETURA (para manutenção — Etapa 7: explicabilidade)
# ------------------------------------------------------------------------------
# A aplicação é um Streamlit single-file organizado em camadas:
#   1. CONFIGURAÇÃO E DADOS BASE (linhas ~30-1200): imports, executores globais
#      (EXECUTOR_GLOBAL p/ pipeline, EXECUTOR_APIS p/ geocodificação, FILA_NOMINATIM
#      rate-limited 1 req/s), caches em disco (DiskCache), carregamento IBGE cacheado
#      (@st.cache_data, pickle 30 dias), bounding boxes dos 27 estados, helpers de UI.
#   2. MOTOR SEMÂNTICO (classe MotorEnderecoCanônico, ~1227): normalização de texto
#      (memoizada), resolução de contexto administrativo (memoizada), classificação de
#      entrada. ParserGeograficoBR extrai CEP/número/complemento (memoizado).
#   3. MOTOR GEODÉSICO (~1467-1565): validar_coordenada_brasil, calcular_distancia_
#      linha_reta (GeographicLib Karney → Geopy → Haversine IUGG 6371.0088),
#      _distancia_consenso_km (mesma matemática sem lock de métrica), cascata_postal.
#   4. GEOCODIFICAÇÃO (~1660-2260): APIs paralelas (ArcGIS, Nominatim, Photon),
#      consenso Bayesiano (processar_consenso_dinamico), cache L1/L2, reverse geocoding.
#   5. ROTEAMENTO (~2260-2700): API_OSRM_Routing (alternatives=3, menor distância),
#      extrair_dados_reais_google (scraper), regra de menor distância Google×OSRM (2%),
#      calcular_pipeline_logistico (orquestra geo+rota), RotaPipeline NamedTuple (35
#      campos), executar_pipeline_unificado, embrulhar_task_paralela.
#   6. PROCESSAMENTO EM LOTE (~2715-2800): rodar_pipeline_lote, processar_chunk_rotas,
#      _montar_dataframe_final, geocodificar_endpoints_paralelo,
#      calcular_matriz_competitiva_vetorizada (alocação).
#   7. INTERFACE (10 abas, ~3200+): Individual, Processamento (máquina de estados em
#      chunks), Alocação (idem), Analytics (cross-filtering Altair), Calculadora,
#      Classificação, Enciclopédia, Manual, Motores, Auditoria.
#
# FLUXO DE PROCESSAMENTO EM LOTE (abas Processamento e Alocação):
#   clique único → FASE 1 (extrai pares únicos + pré-aquece geocodificação) →
#   FASE 2 (processa chunks de 200 rotas, auto-continua via st.rerun, monitora ao
#   vivo) → FASE 3 (monta DataFrame, recalcula Linha Reta, exporta). Checkpoint em
#   session_state garante continuidade sem timeout de WebSocket e retomada após falha.
#
# INVARIANTES CRÍTICOS (não quebrar):
#   - RotaPipeline: índices 0-34 alinhados (res[0]=distância, res[4]=linha_reta,
#     res[19-22]=lat/lon origem/destino, res[28]=motivo, res[30]=status_linha_reta,
#     res[31-34]=concorrência). Score = 0.35*origem + 0.35*destino + 0.30*rota.
#   - Haversine usa raio IUGG 6371.0088 em todo lugar (individual e vetorizado).
#   - Memoizações retornam cópias quando o chamador faz .update() (thread-safe, 50k).
#   - cache_historico_lotes alimenta o estimador de tempo (não remover os campos).
# ==============================================================================
#
# HISTÓRICO DE VERSÕES:
#   v1.0–v2.3 → 13 rodadas (performance, precisão, escala, UX, FIX-LOTE)
#   v2.4 → CORREÇÃO + ACELERAÇÃO DA ABA DE ALOCAÇÃO (FIX-ALOC)
#   v2.5 → AUDITORIA TÉCNICA COMPLETA (linha por linha) — refinamentos + documentação
#   v2.6 → IDENTIFICAÇÃO GEOGRÁFICA + PAINEL DE ALOCAÇÃO (FIX-GEO)
#   v2.7 → CONSISTÊNCIA DE FONTE ÚNICA DE ROTAS (FIX-FONTE)
#   v2.8 → TRAÇADO REAL DA ROTA OSRM NO LINK E MAPA (FIX-OSRM-GEO)
#   v2.9 → LINK OSRM COM TRAJETO GARANTIDO VIA GEOJSON (FIX-OSRM-LINK)
#   v3.0 → PLANO DE CONTINGÊNCIA: LINK COMPARTILHÁVEL VIA GOOGLE MAPS (CONTINGENCIA-OSRM)
#   v3.1 → EVOLUÇÃO ANALÍTICA: COMPARATIVO + ESTATÍSTICA DESCRITIVA
#   v3.2 → ARQUITETURA DEFINITIVA DE ROTAS: GOOGLE MAPS AUDITÁVEL (ARQ-GOOGLE)
#   v3.3 → PRIORIZAÇÃO DE MUNICÍPIOS NO LINK + REMOÇÃO DO OSM DA APRESENTAÇÃO
#   v3.4 → EXPORTAÇÕES AVANÇADAS PARA GIS (EXPORT-GIS)
#   v3.5 → MUNICÍPIO POR COORDENADAS + REMOÇÃO TOTAL DO OSRM
#   v3.6 → RETORNO AO MODELO HÍBRIDO GOOGLE + OSRM, REESTRUTURADO E SUPERIOR (ARQ-HIBRIDO)
#   v3.7 → MAPA DO GOOGLE COM TRAÇADO COMPLETO + NOMES GUIAM A APRESENTAÇÃO
#   v3.8 → MAPA SEMPRE DESENHA A ROTA + LINK POR NOME (comparativo c/ versão antiga de referência)
#   v3.8 (41ª geração) → MITIGAÇÃO ATIVA DO SNAP EXCESSIVO DO OSRM [SNAP-MITIGA]
#     A 40ª geração MEDIU e validou o snap; esta AGE sobre ele (o usuário pediu mitigação real, não
#     só medição). Quando o OSRM projeta origem/destino longe da via (> 1,5 km), o sistema reúne
#     coordenadas candidatas de múltiplos geocoders (ArcGIS/Nominatim/Photon) + o ponto atual, mede
#     o snap de cada uma via OSRM /nearest (sem rotear) e escolhe a coordenada road-adjacent de
#     MENOR deslocamento DENTRO da UF, re-roteando o OSRM com ela. A coordenada validada (canônica)
#     não muda — a road-adjacent é usada só p/ o /route do OSRM. Helpers _osrm_nearest e
#     _melhor_coordenada_para_osrm; memoizado por município (_MITIGA_SNAP_CACHE → custo amortizado
#     no lote); só dispara em snap grande (rotas urbanas intactas, sem latência extra). Se nenhum
#     candidato melhora (malha OSM esparsa), informa e mantém validação+guard. Auditoria ampliada:
#     candidatos avaliados (fonte, coord, snap), coord road-adjacent escolhida, snap antes→depois e
#     rota OSRM antes→depois. Provado por teste isolado (menor snap na UF; descarte de candidato
#     fora da UF; memoização; re-rota só com melhora >300m; disparo só >1500m). Ex. do brief:
#     origem 2346m→210m. Docs: Enciclopédia seção 16. Sem regressão; latência amortizada e restrita.
#   v3.8 (40ª geração) → CAUSA RAIZ DA DIVERGÊNCIA OSRM (SNAP) + VALIDAÇÃO ESPACIAL
#     [OSRM-SNAP + VALID-ESPACIAL]. CAUSA RAIZ COMPROVADA: Google e OSRM já recebem a MESMA
#     origem/destino validados; a divergência vem do OSRM PROJETAR (snap) a coordenada enviada
#     no nó viário mais próximo da malha OSM — em área rural esparsa, isso desloca origem/destino
#     em km. Prova: o OSRM retorna os waypoints "snapados" + a distância do snap (agora capturados
#     no índice 5 do retorno de API_OSRM_Routing; score_rota fixado em 88 p/ não colidir). O
#     painel de auditoria passa a exibir coordenada ENVIADA × USADA (pós-snap) × deslocamento (m).
#     VALIDAÇÃO ESPACIAL: confere se os pontos snapados seguem dentro da bounding box da UF pedida
#     e se o snap ficou no limiar (3 km); alertas exibidos. GUARD de confiança: se o snap jogar
#     origem/destino p/ FORA da UF (erro objetivo), o OSRM NÃO vence o Google (a regra "menor
#     distância" é mantida; só rejeita resultado do OSRM comprovadamente inválido — atende ao
#     pedido "só aceitar rota com confiança mínima"); sem Google, usa o OSRM com o alerta. Provado
#     por teste isolado (6 cenários: snap pequeno/grande, cross-UF, sem Google, sem UF, resposta
#     vazia). Docs: Enciclopédia seção 16. Sem regressão; nenhuma perda de exatidão/desempenho.
#   v3.8 (39ª geração) → ALOCAÇÃO 100% CONTÍNUA + CAMADA ÚNICA + AUDITORIA DE MOTORES
#     [FLUXO-CONTINUO + AUDIT-MOTORES]. (1) A Alocação tinha um estol RESTANTE: a geocodificação
#     dos destinos era síncrona numa única execução (a 38ª geração só havia dado time-box ao
#     roteamento). Agora ela é uma FASE própria time-boxed ('geo_destinos': mini-lotes ~8s +
#     rerun → matriz competitiva → roteamento), tornando a Alocação tão contínua quanto o Lote.
#     (2) Camada única de identificação CONFIRMADA e tornada TRANSPARENTE: origem/destino já
#     passam por uma só geocodificação validada (com anti-alucinação); o Google recebe o NOME
#     oficial e o OSRM recebe a COORDENADA validada (mesma do geocode) — ambos partem do mesmo
#     ponto (o OSRM não reinterpreta o texto). (3) Novo campo RotaPipeline.auditoria_motores
#     (índice 39, default None → 40 campos; 0-38 preservados) captura o rastro: texto original →
#     normalizado → validado → coordenada → parâmetros/URLs de cada motor → consenso/divergência.
#     Novo painel na rota individual: "🔎 Auditoria das Consultas aos Motores de Rota". Provado
#     por teste isolado (rastro, ordem lon/lat do OSRM, divergência, fallback, coords nulas).
#     Docs: Enciclopédia seção 15. Sem regressão, sem perda de desempenho/exatidão.
#   v3.8 (38ª geração) → FLUXO CONTÍNUO: FIM DAS INTERRUPÇÕES NO LOTE [FLUXO-CONTINUO]
#     CAUSA RAIZ do "para no meio e exige novo clique": execuções longas do Streamlit (um chunk
#     fixo de 200 rotas esperava a rota mais lenta; e o pré-aquecimento geocodificava TODOS os
#     endpoints de forma síncrona numa só execução) estouravam o timeout do WebSocket do Streamlit
#     Cloud ANTES de chegar ao st.rerun() — a tela ficava no último frame renderizado, à espera de
#     interação. SOLUÇÃO (Processamento E Alocação): (1) pré-aquecimento vira uma FASE time-boxed
#     (mini-lotes até ~8s, checkpoint, rerun); (2) roteamento vira TIME-BOXED por orçamento de tempo
#     de parede (~8s/execução) em vez de nº fixo de rotas. Cada execução é sempre curta → o WebSocket
#     nunca cai antes do rerun e cada rerun troca mensagens (mantém a conexão "quente"). Adapta-se à
#     rede (rápida = muitas rotas/execução; lenta = menos, mas execução curta). Provado por simulação:
#     5k/100k rotas concluídas; toda execução ≤ orçamento+1 mini-lote; sem reprocessamento; cauda/bordas
#     ok. Avaliados e documentados: thread em background / fila (RQ+Redis) e @st.fragment(run_every) —
#     preteridos por exigirem infraestrutura externa/contexto de thread, incompatíveis com o modelo
#     single-file/Cloud; o padrão checkpoint+execuções-curtas é o mais robusto e sem dependências.
#     Malha territorial IBGE + GeoPandas/DuckDB Spatial: avaliados e NÃO adotados (dependências
#     pesadas GDAL/GEOS, malha não obtível offline, memória do Cloud, modelo single-file) — a
#     validação município↔coordenada já é coberta por centróide IBGE + UF + reverse-geo. Sem regressão.
#   v3.8 (37ª geração) → COMPARAÇÃO DUPLA DE ROTAS + RANKING MULTI-INDICADOR + DOCS
#     [VIS-DUAL + CLASS-MULTI]. (1) A tela individual passa a exibir SEMPRE os dois mapas e os
#     dois links (vencedor + comparativo), não importa quem vença: Google vence → mapa Google
#     (principal) + mapa OSRM (comparativo, geometria exata); OSRM vence → mapa OSRM (principal)
#     + mapa Google (comparativo). Dois campos novos no RotaPipeline (37/38, com default: 39
#     campos no total; índices 0-36 preservados). Bloco de UI estritamente aditivo — não altera
#     o bloco do vencedor. (2) Nova seção "Ranking Multi-Indicador por Rota" na aba Classificação:
#     ordena/filtra rota a rota por indicadores derivados (sinuosidade = viária÷reta, tempo/km,
#     km/min, velocidade média, diferença viária−reta, scores), com critério de desempate,
#     filtros (motor/UF/top N) e download CSV+XLSX. Divisões por zero tratadas (sem crash).
#     (3) Docs atualizadas: guia da aba, Enciclopédia Core (seções 12 e 13, IBGE código/centróide,
#     mapas duais) e Manual (seção 6). AUDITADO o cálculo da linha reta: já é padrão-ouro
#     (GeographicLib/Karney WGS-84 <1mm → Geopy → Haversine IUGG 6371.0088) — nada a alterar.
#     Sem regressão, sem perda de exatidão/desempenho.
#   v3.8 (36ª geração) → BASE NACIONAL IBGE: CÓDIGO OFICIAL + CENTRÓIDE MUNICIPAL REARMADO
#     [BASE-IBGE-COD + BASE-IBGE-CENTROIDE]. A base do IBGE já era a fonte nacional integrada
#     (carregar_dados_ibge: municípios/estados/distritos, pickle 30 dias) e o reconhecimento por
#     nome (com/sem acento, forma curta, fuzzy) já existia (FIX-MUN-CLASS + anti-alucinação).
#     DOIS GAPS REAIS CORRIGIDOS: (1) o payload /localidades/municipios traz o código IBGE em
#     mun["id"] mas ele NÃO era armazenado → agora é (custo de rede zero, pkl v2). (2) esse mesmo
#     endpoint NÃO traz lat/lon (todos os municípios ficavam lat=0.0), o que mantinha o atalho de
#     centróide offline e a BLINDAGEM ANTI-ALUCINAÇÃO praticamente DESLIGADOS em produção (exigiam
#     lat≠0 que nunca existia). Novo resolvedor _centroide_municipio rearma ambos: usa lat/lon
#     offline se existir, senão o centróide por cidade+UF (ArcGIS/Nominatim — centro da cidade,
#     nunca POI), memorizado em RAM. Município reconhecido vira a referência oficial ANTES da
#     cascata (mais rápido) e nunca mais é confundido com rua/hotel. Fall-through preservado:
#     se nenhum centróide responder, mantém o fluxo antigo. ViaCEP/Correios já no cascata de CEP;
#     gazetteers externos (GeoNames/Natural Earth/OSM) dispensados (sem ganho p/ municípios BR já
#     100% cobertos offline pelo IBGE). Cache V65→V66. Sem regressão, sem perda de precisão.
#   v3.8++ → APRESENTAÇÃO DINÂMICA POR PROVEDOR VENCEDOR [VIS-DINAMICA / VIS-OSRM-LINK - 30ª geração]:
#     PROBLEMA: "independentemente do vencedor, o mapa embarcado continuava sendo só o do OSRM".
#     CAUSA RAIZ: no cenário Google-vence, quando a extração da polyline do Google falhava (frequente),
#       o mapa caía na geometria do OSRM como "traçado de referência" — daí parecer "sempre OSRM".
#     SOLUÇÃO (arquitetura por vencedor, mapa=link sempre):
#       • GOOGLE vence → mapa embarcado EXCLUSIVAMENTE do Google (embed http: Embed API se houver a
#         chave GOOGLE_MAPS_EMBED_API_KEY, senão ?saddr&daddr&output=embed COM NOMES) + 1 ÚNICO link
#         (Google). NUNCA usa geometria do OSRM. Mapa e link saem dos MESMOS params (nome qualificado).
#       • OSRM vence → mapa embarcado EXCLUSIVAMENTE do OSRM (Leaflet, geometria exata, nomes) + 2 links:
#         (1) Google Maps (comparação) e (2) VISUALIZADOR PRÓPRIO via "?rota=osrm&g=<polyline>&o&d&km&t"
#         — o próprio app entra em modo visualizador e reproduz FIELMENTE o mesmo mapa (mesma geometria,
#         mesmos nomes), sem depender de serviço externo. Mantém o download HTML (fidelidade offline).
#         Salvaguarda: se a URL do visualizador ficar longa demais (>7,5k), recai no download.
#       • Geodésico → ligação direta estimada (Leaflet) + 1 link + aviso (inalterado).
#     RotaPipeline: +índice 36 (link_osrm_viewer, default ""); CACHE_VERSION V61→V62.
#   v3.8+ → AVALIAÇÃO CRÍTICA DE 13 MELHORIAS PROPOSTAS (auditoria de impacto):
#     IMPLEMENTADAS (ganho real, risco ~zero): M2 (executores ThreadPool como SINGLETONS via
#       @st.cache_resource — elimina recriação do pool a cada rerun) e M14-parcial (TomTom via
#       st.secrets — SMTP já usava secrets). 11 itens documentados como NÃO IMPLEMENTAR por
#       premissa inválida (M2-isolamento, M14-SMTP, M5-progresso já nativo), benefício marginal
#       na escala real dominada por rede (M1, M4, M13), risco de regressão de precisão (M11),
#       ou reescrita massiva sem ganho real em escala limitada por rate-limit de API (M9, M12),
#       além de M7/M8/M10 (dependências pesadas / incompatível com arquitetura de arquivo único).
# DIAGNÓSTICO COMPARATIVO (versão antiga × atual):
#   A versão ANTIGA desenhava a rota no mapa embarcado porque usava o embed clássico do
#   Google com TEXTO (nomes): maps?saddr={nome}&daddr={nome}&output=embed — esse endpoint
#   renderiza direções (a rota) e mostra nomes. PORÉM tinha o bug município→POI (texto cru
#   ambíguo) e depende de um endpoint hoje instável.
#   A versão ATUAL (v3.5-3.7) trocou os parâmetros por COORDENADAS (para corrigir o POI) e
#   passou a extrair a polyline do Google (frágil). Quando a extração falha, recaía no embed
#   clássico de COORDENADAS → só marcadores + coords. Era exatamente o que o usuário via.
#
# SOLUÇÃO SUPERIOR [VIS-ALWAYS-DRAW + VIS-NAMES-LINK]:
#   1) O mapa embarcado AGORA SEMPRE desenha o traçado (Leaflet autocontido), nunca só
#      marcadores. Hierarquia de geometria (degradação graciosa, sempre com NOMES):
#        a) geometria do próprio Google (extraída e validada) → idêntica ao Google;
#        b) se falhar, a geometria CONFIÁVEL do OSRM (que já roda no híbrido) → traçado
#           praticamente idêntico, claramente rotulado como referência;
#        c) sem nenhuma, a ligação direta origem→destino (ainda com nomes).
#      Isso elimina DE VEZ o "mapa só com 2 marcadores" (a versão antiga dependia de um
#      endpoint frágil; aqui nós mesmos desenhamos — mais robusto e moderno).
#   2) O LINK e o mapa passam a usar o NOME OFICIAL totalmente qualificado do município
#      ("Corumbá de Goiás, Goiás, Brasil") em vez de coordenadas. Resolve o Problema #2
#      (coordenadas na apresentação) e, por ser o nome OFICIAL e QUALIFICADO (não o texto
#      cru), mantém a robustez contra o POI. As coordenadas seguem como âncora interna.
#   3) Até o fallback geodésico desenha um mapa Leaflet (ligação direta) com nomes.
#
# Validação: link de município → nome qualificado (testado); mapa sempre desenha traçado
#   (Google→OSRM→linha direta); decoder vs vetor canônico; RotaPipeline íntegra (0-35);
#   retorno do scraper de 7 elementos retrocompatível. Sem regressão.
# ------------------------------------------------------------------------------
#   [Detalhes das versões anteriores abaixo]
#
# PRIORIDADE MÁXIMA Nº 1 RESOLVIDA (mapa do Google só mostrava 2 marcadores):
#   O endpoint clássico ?saddr&daddr&output=embed tornou-se instável e renderiza só dois
#   marcadores, sem o traçado. SOLUÇÃO [VIS-GOOGLE-GEO]: o scraper EXTRAI A GEOMETRIA
#   (polyline) da rota do Google (índice 6, aditivo), VALIDA-A geograficamente (rota deve
#   começar perto da origem, terminar perto do destino, dentro da caixa plausível) e
#   desenha o TRAÇADO COMPLETO num mapa Leaflet autocontido (fit bounds + zoom). Se a
#   extração falhar, cai no embed clássico (degradação graciosa, zero risco de rota errada).
#   Há download do mapa HTML também para o Google. Unificado em _gerar_mapa_leaflet_rota.
#
# NOMES GUIAM A APRESENTAÇÃO [VIS-NAMES]: mapas rotulam origem/destino pelo NOME OFICIAL
#   (não lat/lon); badge mostra provedor, distância, tempo e nomes. Novo _escapar_js()
#   blinda os nomes no HTML/JS do mapa.
#
# Validação: extração + validação geográfica testadas (aceita rota correta, REJEITA
#   polyline de outra região, degrada sem geometria); decoder vs vetor canônico; retorno
#   de 7 elementos retrocompatível (callers usam 0-5); RotaPipeline íntegra. Sem regressão.
# ------------------------------------------------------------------------------
#   [Detalhes da v3.6 abaixo]
#
# MUDANÇA DE DIREÇÃO (decisão do usuário): restaurar o modelo híbrido (Google + OSRM)
# com seleção automática de MENOR DISTÂNCIA, porém reestruturado e muito superior ao
# que existia — mais auditável, mais visual, mais robusto.
#
# NOVA ARQUITETURA [ARQ-HIBRIDO]:
#   - Os DOIS motores (Google Maps + OSRM) são executados em toda rota.
#   - A aplicação compara as distâncias e adota a MENOR (tolerância de 2% a favor do
#     Google, que tem link de navegação 100% auditável — evita alternância sem ganho).
#   - GOOGLE vence → mapa embarcado EXCLUSIVAMENTE do Google (embed http, rota traçada,
#     nomes) + 1 ÚNICO link (Google). NUNCA usa geometria do OSRM. + OSRM no comparativo.
#   - OSRM vence → mapa embarcado EXCLUSIVAMENTE do OSRM (Leaflet, GEOMETRIA EXATA, nomes)
#     + 2 links: (1) Google (comparação) e (2) VISUALIZADOR PRÓPRIO da rota OSRM + DOWNLOAD
#     do mapa HTML autocontido (rota exata, offline) + comparativo obrigatório.
#   - Comparativo RICO e VISUAL: cards lado a lado, selo do vencedor (🏆), diferença
#     absoluta/percentual/tempo, leitura automática de convergência/divergência.
#
# LINK DA ROTA OSRM [VIS-OSRM-LINK - 30ª geração] — SOLUÇÃO FINAL (visualizador próprio):
#   Reconfirmado que NÃO há link COMPARTILHÁVEL público robusto/documentado que abra a
#   geometria exata do OSRM (geojson.io/map.project-osrm são frágeis/legados). SOLUÇÃO
#   adotada, robusta e auditável, DENTRO do modelo single-file: a própria aplicação serve um
#   VISUALIZADOR via query param ("?rota=osrm&g=<polyline>&o&d&km&t") — ao abrir o link, o app
#   entra num modo visualizador que reproduz FIELMENTE o mesmo mapa embarcado (mesma geometria
#   decodificada, mesmos nomes), sem hospedagem extra nem dependência externa. Complementos:
#   (1) mapa Leaflet embarcado desenha a geometria EXATA; (2) DOWNLOAD de HTML autocontido
#   (fidelidade offline); (3) link Google para comparação. Salvaguarda: URL muito longa
#   (>7,5k) → recai no download.
#
# Validação: 5 cenários testados (OSRM vence; Google vence; empate→Google; só OSRM;
#   só Google); comparativo correto; mapa OSRM desenha geometria; RotaPipeline íntegra;
#   priorização de município por coordenadas (FIX-MUN-COORD) preservada. Sem regressão.
# ------------------------------------------------------------------------------
# MELHORIAS APLICADAS v2.4 → v2.5:
#   [PERF-UI1] Contagem de rotas únicas da prévia de estimativa agora é cacheada
#         (@st.cache_data) pela identidade do arquivo. Antes recalculava set(zip(...))
#         sobre TODO o DataFrame a cada rerun (cada tecla no campo de operador) —
#         desperdício real em planilhas de 100k linhas. Args grandes não-hasheados
#         (prefixo _) para o cache não custar mais que o cálculo. Lógica idêntica
#         (validada em casos-limite + 50k linhas). Zero regressão.
#   [UX-POLISH] Corrigidos ícones quebrados/ausentes em botões e colunas de link
#         ("Limpar Filtros", "Abrir no Maps", "Exportar Relatório", "Baixar Tabela")
#         que renderizavam como espaço vazio — aparência mais profissional e consistente.
#   [DOC] Adicionado mapa de arquitetura e fluxo no cabeçalho (Etapa 7: explicabilidade)
#         para facilitar manutenção corporativa.
#
# DECISÃO DOCUMENTADA (regra: zero regressão):
#   - Hardening de colunas duplicadas pós-normalização (str.title pode colidir "origem"
#     e "Origem"): adicionaria robustez, mas alterar a normalização de colunas pode
#     mudar o comportamento de arquivos que hoje funcionam. Documentado, não implementado.
#   - Polars/DuckDB/asyncio/Numba: avaliados em rodadas anteriores — gargalo é rede,
#     não tabela/CPU-numérico. Sem ganho no caminho dominante.
#
# Todas as correções e otimizações anteriores preservadas (FIX-LOTE, FIX-ALOC,
# SPEED-1..4, PERF-Q1..3, regra de menor distância, consenso Bayesiano, etc).
# ==============================================================================

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import requests
import time
import math
import io
import re
import os
import pickle
import collections
import hashlib
import threading
import json
import urllib.parse
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import NamedTuple, Optional, List
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from unidecode import unidecode
from rapidfuzz import process, fuzz
from diskcache import Cache
from sklearn.cluster import DBSCAN
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache as _lru_cache
try:
    from cachetools import LRUCache as _CacheToolsLRU
    _CACHETOOLS_DISPONIVEL = True
except ImportError:
    _CACHETOOLS_DISPONIVEL = False

# Motores Geodésicos Estratificados
try:
    from geographiclib.geodesic import Geodesic
    GEOGRAPHICLIB_DISPONIVEL = True
except ImportError:
    GEOGRAPHICLIB_DISPONIVEL = False

try:
    from geopy.distance import geodesic
    GEOPY_DISPONIVEL = True
except ImportError:
    GEOPY_DISPONIVEL = False

# ==============================================================================
# CONFIGURAÇÃO DE LOGS E AUDITORIA CRÍTICA
# ==============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MotorGeodesicoCorp")

# [M21] Log estruturado: adiciona campos extras a cada evento de relevância
def _log_api(fonte: str, sucesso: bool, latencia_ms: float, query: str = ""):
    logger.info(
        "api_call",
        extra={"fonte": fonte, "sucesso": sucesso, "latencia_ms": round(latencia_ms, 1), "query": query[:120]}
    )

# [M11] RotaPipeline NamedTuple — substitui tupla posicional de 31+ elementos
# Acesso por nome elimina bugs de deslocamento de índice ao adicionar campos
class RotaPipeline(NamedTuple):
    distancia: float
    tempo: str
    link_rota: str
    balsas: str
    dist_linha_reta: float
    fonte_rota: str
    score_rota: float
    confianca_origem: str
    score_num_origem: float
    distrito_origem: str
    municipio_origem: str
    fonte_geo_origem: str
    endereco_oficial_origem: str
    confianca_destino: str
    score_num_destino: float
    distrito_destino: str
    municipio_destino: str
    fonte_geo_destino: str
    endereco_oficial_destino: str
    lat_origem: float
    lon_origem: float
    lat_destino: float
    lon_destino: float
    tempo_geocoding: float
    tempo_roteamento: float
    tempo_total: float
    xai_origem: List[str]
    xai_destino: List[str]
    motivo_roteamento: str
    link_embed: str
    status_linha_reta: str
    # Campos de alocação competitiva (opcionais, preenchidos na aba Alocação)
    concorrente: str = "N/A"
    dist_concorrente: float = 0.0
    link_concorrente: str = "N/A"
    justificativa: str = "N/A"
    # [COMP-PROV - 21ª geração] Comparativo entre provedores (Google × OSRM). String
    # codificada "km_g|tempo_g|km_o|tempo_o|fonte" ou "" quando só um provedor respondeu.
    # Adicionado APÓS todos os campos existentes (índice 35) com default — preserva
    # integralmente os índices 0-34 e a compatibilidade de toda a aplicação.
    comparativo_provedores: str = ""
    # [VIS-DINAMICA - 30ª geração] Link do VISUALIZADOR PRÓPRIO da rota OSRM (índice 36,
    # default ""). Quando o OSRM vence, guarda um link relativo "?rota=osrm&g=...&o=...&d=..."
    # que abre o próprio app num visualizador que reproduz EXATAMENTE o mapa embarcado
    # (mesma geometria, mesmos nomes). Vazio quando o Google vence ou no fallback geodésico.
    link_osrm_viewer: str = ""
    # [VIS-DUAL - 37ª geração] Mapa + link do provedor COMPARATIVO (o NÃO-vencedor), para que
    # AMBAS as rotas (Google e OSRM) sejam sempre visualizáveis — auditabilidade máxima.
    # Índices 37/38, default "" (preserva 0-36 e toda a compatibilidade).
    #   link_embed_comparativo: Google vence → mapa Leaflet do OSRM (geometria exata);
    #     OSRM vence → embed do Google (URL). Vazio se o outro provedor não respondeu / geodésico.
    #   link_rota_comparativo: link do provedor comparativo (viewer OSRM ou link Google navegável).
    link_embed_comparativo: str = ""
    link_rota_comparativo: str = ""
    # [AUDIT-MOTORES - 39ª geração] Rastro completo das consultas enviadas a cada motor de rota:
    # texto original → normalizado → validado → coordenadas → parâmetros/URLs por motor + consenso.
    # Índice 40, default None (aditivo; não afeta índices 0-39). É um dicionário estruturado.
    auditoria_motores: dict = None

def _montar_comparativo_provedores(km_g, tempo_g, km_o, tempo_o, fonte_vencedora):
    """[COMP-PROV - 21ª geração] Codifica os dados de comparação entre Google e OSRM
    num formato compacto e à prova de parsing (sem JSON, sem caracteres problemáticos):
    'km_g|tempo_g_min|km_o|tempo_o_min|fonte_vencedora'. Valores ausentes viram ''.
    Usado para exibir, na aba de geocodificação, um comparativo claro quando ambos os
    provedores responderam — conforme a evolução solicitada (transparência total)."""
    def _fmt(v):
        return str(v) if v is not None and v != "" else ""
    return f"{_fmt(km_g)}|{_fmt(tempo_g)}|{_fmt(km_o)}|{_fmt(tempo_o)}|{_fmt(fonte_vencedora)}"

def _parsear_comparativo_provedores(s):
    """[COMP-PROV] Decodifica a string de comparação. Retorna dict com floats/strings ou
    None se indisponível/malformada. Robusto a campos vazios."""
    if not s or "|" not in s:
        return None
    partes = s.split("|")
    if len(partes) < 5:
        return None
    try:
        km_g = float(partes[0]) if partes[0] else None
        km_o = float(partes[2]) if partes[2] else None
        if km_g is None or km_o is None:
            return None
        return {
            "km_google": km_g,
            "tempo_google": partes[1],
            "km_osrm": km_o,
            "tempo_osrm": partes[3],
            "fonte_vencedora": partes[4],
        }
    except (ValueError, IndexError):
        return None

METRICAS_DISTANCIA = {
    "total_calculos": 0,
    "sucesso_geographiclib": 0,
    "sucesso_geopy": 0,
    "fallback_haversine": 0,
    "correcoes_automaticas": 0,
    "falhas_criticas": 0,
    "cache_unpoisoned": 0,
    "barreira_territorial": 0,
    "desambiguacoes_estritas": 0,
    "_inicio_metricas": time.time()  # [M24] timestamp para cálculo de taxa por período
}

_LOCK_METRICAS = threading.Lock()

def _incrementar_metrica(campo: str, valor: int = 1):
    with _LOCK_METRICAS:
        METRICAS_DISTANCIA[campo] += valor

# ==============================================================================
# CONFIGURAÇÃO DE UI/UX E AMBIENTE
# ==============================================================================
st.set_page_config(
    page_title="Motor Nacional de Roteirização Inteligente",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.claude.com',
        'About': "### Motor Nacional de Roteirização Inteligente\nPlataforma corporativa B2B de geocodificação, inferência Bayesiana e auditoria logística. Versão 1.3."
    }
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif !important;
    }
    
    .stApp {
        background-color: #0E1117;
    }
    
    [data-testid="stSidebar"] {
        background-color: #161A25;
        border-right: 1px solid #2D3342;
    }
    
    [data-testid="stMetric"] {
        background-color: #1E232F;
        border: 1px solid #2D3342;
        padding: 1.2rem;
        border-radius: 8px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        border-left: 4px solid #3B82F6;
        transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
    }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.15), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    }
    
    [data-testid="stMetricLabel"] {
        color: #9CA3AF !important;
        font-weight: 500;
        font-size: 0.95rem;
        margin-bottom: 0.5rem;
    }
    
    [data-testid="stMetricValue"] {
        color: #F9FAFB !important;
        font-weight: 700;
        font-size: 1.8rem;
    }
    
    [data-testid="stMetricDelta"] {
        font-size: 0.85rem;
    }
    
    [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
    }
    
    [data-baseweb="tab"] {
        background-color: #161A25;
        border: 1px solid #2D3342;
        border-bottom: none;
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
        color: #9CA3AF;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    [data-baseweb="tab"]:hover {
        color: #F9FAFB;
        background-color: #1E232F;
    }
    
    [data-baseweb="tab"][aria-selected="true"] {
        background-color: #3B82F6;
        color: #FFFFFF;
        border-color: #3B82F6;
    }
    
    .stButton > button {
        border-radius: 6px;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    .stButton > button[kind="primary"] {
        background-color: #3B82F6;
        color: white;
        border: none;
    }
    
    .stButton > button[kind="primary"]:hover {
        background-color: #2563EB;
        box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.5);
    }
    
    [data-testid="stExpander"] {
        background-color: #1E232F;
        border: 1px solid #2D3342;
        border-radius: 8px;
    }
    
    [data-testid="stExpander"] summary {
        font-weight: 600;
        color: #E5E7EB;
    }
    
    [data-testid="stDataFrame"] {
        border: 1px solid #2D3342;
        border-radius: 8px;
        overflow: hidden;
    }
    
    .corporate-header {
        background: linear-gradient(135deg, #161A25 0%, #1E232F 100%);
        padding: 24px;
        border-radius: 12px;
        margin-bottom: 30px;
        border-left: 6px solid #3B82F6;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    
    .corporate-title {
        color: #F9FAFB;
        margin: 0;
        font-weight: 700;
        font-size: 24px;
        letter-spacing: -0.5px;
    }
    
    .corporate-subtitle {
        color: #9CA3AF;
        margin: 5px 0 0 0;
        font-size: 15px;
        font-weight: 400;
    }
    
    .filter-badge {
        display: inline-block;
        background-color: #3B82F6;
        color: white;
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        margin-right: 8px;
        margin-bottom: 8px;
    }

    /* ==========================================================================
       DESIGN SYSTEM v1.3 — Tokens, Acessibilidade, Componentes [UX 2ª geração]
       ========================================================================== */

    /* Acessibilidade: foco visível por teclado (WCAG 2.4.7) */
    button:focus-visible, a:focus-visible, input:focus-visible,
    [data-baseweb="tab"]:focus-visible, select:focus-visible {
        outline: 3px solid #60A5FA !important;
        outline-offset: 2px !important;
        border-radius: 6px;
    }

    /* Respeita usuários que preferem menos movimento (WCAG 2.3.3) */
    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
            animation-duration: 0.001ms !important;
            transition-duration: 0.001ms !important;
        }
    }

    /* Pílulas de status — semáforo de confiança consistente em todo o app */
    .ds-pill {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 3px 12px; border-radius: 999px;
        font-size: 12.5px; font-weight: 600; line-height: 1.6;
    }
    .ds-pill::before { content: ''; width: 8px; height: 8px; border-radius: 50%; }
    .ds-pill-excelente { background: rgba(46,204,113,.15); color: #2ECC71; }
    .ds-pill-excelente::before { background: #2ECC71; }
    .ds-pill-boa { background: rgba(52,152,219,.15); color: #3498DB; }
    .ds-pill-boa::before { background: #3498DB; }
    .ds-pill-aceitavel { background: rgba(241,196,15,.15); color: #F1C40F; }
    .ds-pill-aceitavel::before { background: #F1C40F; }
    .ds-pill-revisar { background: rgba(230,126,34,.15); color: #E67E22; }
    .ds-pill-revisar::before { background: #E67E22; }
    .ds-pill-erro { background: rgba(231,76,60,.15); color: #E74C3C; }
    .ds-pill-erro::before { background: #E74C3C; }

    /* Card informativo do Design System */
    .ds-card {
        background: #1E232F; border: 1px solid #2D3342;
        border-radius: 10px; padding: 16px 18px; margin-bottom: 12px;
    }
    .ds-card-title { color: #E5E7EB; font-weight: 600; font-size: 14px; margin: 0 0 6px 0; }
    .ds-card-body  { color: #9CA3AF; font-size: 13px; margin: 0; line-height: 1.6; }

    /* Skeleton loading — placeholder animado durante carregamento */
    .ds-skeleton {
        background: linear-gradient(90deg, #1E232F 25%, #2D3342 50%, #1E232F 75%);
        background-size: 200% 100%;
        animation: ds-shimmer 1.4s ease-in-out infinite;
        border-radius: 8px;
    }
    @keyframes ds-shimmer {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }

    /* Barra de confiança visual (0-100) */
    .ds-confbar-track { background:#2D3342; border-radius:999px; height:8px; width:100%; overflow:hidden; }
    .ds-confbar-fill  { height:8px; border-radius:999px; transition: width .4s ease; }

    /* Tooltip nativo aprimorado em elementos com [data-ds-tip] */
    [data-ds-tip] { position: relative; cursor: help; border-bottom: 1px dotted #6B7280; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# DESIGN SYSTEM — Helpers de UI reutilizáveis [UX 2ª geração]
# ==============================================================================
def ds_status_pill(status: str) -> str:
    """Retorna HTML de uma pílula de status com semáforo de cor consistente."""
    mapa = {
        "Excelente": "ds-pill-excelente", "Boa": "ds-pill-boa",
        "Aceitável": "ds-pill-aceitavel", "Revisar": "ds-pill-revisar",
        "Erro": "ds-pill-erro", "Erro Crítico de Processamento": "ds-pill-erro",
    }
    classe = mapa.get(status, "ds-pill-revisar")
    return f"<span class='ds-pill {classe}'>{status}</span>"

def ds_barra_confianca(score: float) -> str:
    """Retorna HTML de uma barra de confiança 0-100 com cor por faixa."""
    score = max(0.0, min(100.0, float(score)))
    cor = "#2ECC71" if score >= 85 else "#3498DB" if score >= 75 else "#F1C40F" if score >= 60 else "#E74C3C"
    return (f"<div class='ds-confbar-track'><div class='ds-confbar-fill' "
            f"style='width:{score}%; background:{cor};'></div></div>")

# ==============================================================================
# GUIA PADRONIZADO "COMO USAR ESTA ABA" — obrigatório em todas as abas [4ª geração]
# ==============================================================================
# Conteúdo estruturado por aba. Cada entrada segue o mesmo esqueleto pedagógico:
# o_que_faz, quando_usar, dados, preenchimento, apos_executar, interpretar,
# exemplos, erros_comuns, dicas. Linguagem para quem nunca viu geocodificação.
_GUIA_ABAS = {
    "geocodificacao": {
        "o_que_faz": "Descobre **onde fica** um endereço no mapa (latitude/longitude) e calcula a **distância real de carro** entre dois pontos. É o teste rápido de uma única rota.",
        "quando_usar": "Quando você quer conferir uma rota específica na hora, sem montar planilha. Ideal para validar um endereço suspeito ou tirar uma dúvida pontual.",
        "dados": "Dois textos: uma **Origem** (de onde sai) e um **Destino** (para onde vai). Cada um pode ser um endereço, o nome de um lugar conhecido (ex: \"Aeroporto de Brasília\") ou coordenadas no formato `-15.79, -47.88`.",
        "preenchimento": "1. Digite a origem no primeiro campo.\n        2. Digite o destino no segundo.\n        3. **Sempre que puder, inclua a sigla do estado** (ex: `, GO`) — isso evita confusão entre cidades de mesmo nome.\n        4. Clique em **Calcular Rota Individual**.",
        "apos_executar": "O sistema consulta várias fontes de mapa ao mesmo tempo (ArcGIS, OpenStreetMap, TomTom), cruza as respostas, escolhe a mais confiável e mede a distância real pela estrada e em linha reta.",
        "interpretar": "**Distância Viária** = km reais por asfalto. **Linha Reta** = voo de pássaro (serve de árbitro contra fretes inflados). **Barra de confiança**: verde = ótima localização; amarela/vermelha = vale revisar o endereço digitado.",
        "exemplos": "`Ribeirão Cascalheira, MT` → `São Miguel do Araguaia, GO`. Ou coordenadas puras: `-15.79, -47.88` → `-16.68, -49.25`.",
        "erros_comuns": "Esquecer a sigla do estado (pode achar cidade homônima errada); digitar só o nome de uma rua sem a cidade; deixar um dos campos vazio.",
        "dicas": "Quanto mais completo o endereço, melhor. Abra a 'Auditoria Detalhada' para ver exatamente quais fontes concordaram e por quê.",
    },
    "processamento": {
        "o_que_faz": "Processa **milhares de rotas de uma vez**. Você envia uma planilha Excel com colunas de Origem e Destino, e ele devolve a mesma planilha preenchida com distâncias, tempos, coordenadas e nível de confiança de cada linha.",
        "quando_usar": "Quando você tem uma lista grande de rotas (entregas, fretes, visitas) e precisa calcular todas de uma vez, em vez de uma por uma.",
        "dados": "Um arquivo **.xlsx** contendo obrigatoriamente duas colunas chamadas exatamente **Origem** e **Destino**. Pode ter outras colunas — elas serão preservadas.",
        "preenchimento": "1. Clique em **Selecionar Arquivo Excel** e escolha sua planilha.\n        2. Confira a mensagem de validação (verde = pronto).\n        3. (Opcional) Informe seu nome/matrícula.\n        4. Clique **uma única vez** em **Iniciar Processamento em Lote** e acompanhe a barra de progresso.",
        "apos_executar": "O processamento é **contínuo e automático**: após um único clique, o sistema pré-aquece a geocodificação e processa todas as rotas até o fim **sem precisar clicar de novo**. Ele extrai apenas as rotas **únicas** (evita recalcular repetidas), processa várias em paralelo em execuções curtas (para nunca cair a conexão) e reaproveita o cache. Ao terminar, mostra um Scorecard de Qualidade.",
        "interpretar": "Durante o processamento, o painel ao vivo mostra progresso, velocidade e tempo restante (ETA) — e avança sozinho. O **Scorecard** ao final resume a saúde do lote: taxa de sucesso, quantas rotas são confiáveis e quantas têm anomalias. Baixe o Excel pronto no botão azul.",
        "exemplos": "Uma planilha com 2.000 linhas de entregas: coluna Origem = endereço do depósito, coluna Destino = endereço do cliente.",
        "erros_comuns": "Colunas com nome errado (tem que ser 'Origem' e 'Destino'); arquivo em formato antigo (.xls em vez de .xlsx); células vazias no meio da planilha (são ignoradas e marcadas como erro).",
        "dicas": "Você **não precisa** ficar clicando: o lote continua sozinho até concluir, mesmo com dezenas de milhares de linhas. Rotas repetidas entre lotes são reaproveitadas automaticamente. Limite atual: 100.000 linhas por arquivo.",
    },
    "alocacao": {
        "o_que_faz": "Descobre **qual base/depósito é o mais próximo** de cada cliente. Você dá uma lista de clientes e uma lista de bases, e o sistema calcula automaticamente o melhor par para cada cliente.",
        "quando_usar": "Quando você tem vários centros de distribuição e precisa decidir qual atende cada cliente com o menor trajeto — clássico problema de logística de hubs.",
        "dados": "**Duas planilhas .xlsx**: uma com os endereços dos clientes (Origens) e outra com os municípios/bases (Destinos). Você escolhe qual coluna usar em cada arquivo.",
        "preenchimento": "1. Envie a planilha de **clientes** no primeiro campo.\n        2. Envie a planilha de **bases** no segundo.\n        3. Selecione a coluna correta de cada arquivo nos menus.\n        4. Clique em **Processar Cruzamento Espacial**.",
        "apos_executar": "Para cada cliente, o sistema mede a distância até todas as bases, escolhe a mais próxima (vizinho mais próximo geográfico) e ainda mostra qual seria a segunda opção, com a justificativa da escolha.",
        "interpretar": "A coluna **Concorrente Analisado** mostra a 2ª base mais próxima; a **Justificativa** explica por que a base vencedora foi escolhida. Quanto menor a distância, melhor a alocação.",
        "exemplos": "10 centros de distribuição × 500 clientes → o sistema descobre o CD ideal para cada um dos 500.",
        "erros_comuns": "Escolher a coluna errada nos menus; misturar clientes e bases nos arquivos trocados; bases sem endereço resolvível.",
        "dicas": "Use nomes de cidade com a sigla do estado nas bases para máxima precisão. O número de combinações cresce rápido (clientes × bases) — comece com listas menores para testar.",
    },
    "analytics": {
        "o_que_faz": "Um **painel interativo estilo Power BI** que transforma o resultado do seu lote em gráficos, mapas e indicadores. Clicar em um gráfico filtra todos os outros ao mesmo tempo.",
        "quando_usar": "Depois de processar um lote, quando você quer **explorar visualmente** os dados, apresentar resultados em reunião ou descobrir padrões por região/estado.",
        "dados": "Nenhum upload aqui — usa automaticamente o último lote processado na aba **Processamento Lote**.",
        "preenchimento": "1. Processe um lote primeiro.\n        2. Venha para esta aba.\n        3. Clique nas fatias, barras ou arraste o mouse nos gráficos para filtrar.\n        4. Use os filtros avançados nas caixas expansíveis para recortes específicos.",
        "apos_executar": "Os gráficos e o mapa se atualizam instantaneamente conforme você clica. Os filtros são **bidirecionais**: selecionar um estado no mapa filtra os gráficos, e vice-versa.",
        "interpretar": "Cada gráfico responde a uma pergunta: distribuição por região, status de qualidade, dispersão distância×tempo, etc. Os **Insights Automáticos** no topo destacam o que mais chama atenção nos dados.",
        "exemplos": "Clicar na fatia 'SP' no gráfico de pizza → todos os indicadores passam a mostrar apenas rotas de São Paulo.",
        "erros_comuns": "Entrar aqui sem ter processado um lote (não há dados); aplicar filtros que esvaziam a base (ex: Nordeste + SP) e estranhar gráficos vazios.",
        "dicas": "Use o botão **Limpar Todos os Filtros** no topo quando os gráficos sumirem. Combine filtros de região + faixa de distância para análises ricas.",
    },
    "calculadora": {
        "o_que_faz": "Uma **calculadora analítica de autoatendimento** (self-service BI). Permite somar, contar e cruzar os dados do lote do jeito que você quiser, criando tabelas dinâmicas sob medida.",
        "quando_usar": "Quando os gráficos prontos não bastam e você precisa de um número específico — ex: 'qual a distância média por estado?' ou 'quantas rotas de revisão por região?'.",
        "dados": "Usa o último lote processado. Você escolhe o que agrupar e qual operação aplicar.",
        "preenchimento": "1. Escolha a coluna para **Agrupar por** (ex: Região).\n        2. Escolha a coluna do **Valor** (ex: Distância).\n        3. Escolha a **Operação** (Soma, Média, Contagem...).\n        4. O resultado aparece na hora, com gráfico e tabela para download.",
        "apos_executar": "O sistema agrupa os dados e aplica a operação estatística escolhida, montando uma tabela dinâmica e um gráfico correspondente.",
        "interpretar": "A tabela mostra o resultado por grupo; o gráfico ilustra visualmente. Você pode baixar tudo em Excel (inclusive com o gráfico embutido).",
        "exemplos": "Agrupar por 'Regiao_Sintetica_Origem' + Média de 'Distancia' = distância média de cada região.",
        "erros_comuns": "Aplicar operações numéricas (Soma/Média) em colunas de texto; esquecer de processar um lote antes.",
        "dicas": "Use 'Contagem Distinta' para descobrir quantos municípios/clientes únicos existem em cada grupo. Exporte a 'Multi-Abas' para entregar à chefia.",
    },
    "classificacao": {
        "o_que_faz": "Tem **duas ferramentas**: (1) agrupa municípios em **faixas personalizadas** (ex: 'Cidades Críticas', 'Normais') por distância ou volume, gerando uma tabela mestre de segmentação; e (2) um **Ranking Multi-Indicador por Rota** que ordena e filtra rota a rota por dezenas de indicadores logísticos derivados (sinuosidade, tempo/km, velocidade média, diferença viária−reta, scores...).",
        "quando_usar": "Use as **faixas** para criar regras de frete por distância/volume. Use o **Ranking Multi-Indicador** para achar gargalos, rotas anômalas (muito sinuosas, muito lentas) e comparar desempenho entre motores.",
        "dados": "Usa o último lote processado. Nas faixas você define os limites; no ranking você escolhe o indicador de ordenação, o critério de desempate e os filtros.",
        "preenchimento": "1. Escolha a base da classificação (Distância ou Volume) e edite as faixas (limites, rótulos, cores).\n        2. Role até o **Ranking Multi-Indicador por Rota**: escolha o critério principal (ex: Índice de Sinuosidade), um desempate opcional, a ordem (crescente/decrescente) e filtre por motor vencedor / UF / top N.",
        "apos_executar": "As faixas geram mapa temático + tabela de segmentação. O ranking gera uma tabela ordenável com todos os indicadores derivados e **botões de download (CSV e XLSX)** dos dados que originaram o ranking.",
        "interpretar": "Nas faixas, cada cor é um nível. No ranking: *Índice de Sinuosidade* = viária ÷ linha reta (1,0 = rota reta; quanto maior, mais serpenteia); *Tempo por km* e *Velocidade Média* revelam gargalos; *Diferença Viária−Reta* destaca grandes desvios geográficos.",
        "exemplos": "Faixa 1–500 km = Verde (Normal); 501 km+ = Vermelho (Frete majorado). No ranking: ordene por *Índice de Sinuosidade* (decrescente) para ver as rotas que mais desviam da linha reta — candidatas a revisão de trajeto ou de geocodificação.",
        "erros_comuns": "Faixas sobrepostas ou com lacunas; não processar um lote antes; no ranking, esquecer que rotas com distância viária 0 (pontos coincidentes) aparecem com alguns indicadores em branco (divisão indefinida).",
        "dicas": "Combine critério principal + desempate no ranking para segmentações compostas (ex: sinuosidade desc, depois score global desc). Baixe o CSV/XLSX para auditar externamente.",
    },
    "enciclopedia": {
        "o_que_faz": "É o **repositório mestre de conhecimento** da plataforma. Explica, do zero e sem pressa, toda a jornada técnica de um dado dentro do sistema — da limpeza do texto à validação geométrica anti-colisão.",
        "quando_usar": "Quando você quer **entender como o sistema funciona por dentro**, aprender os conceitos de geocodificação ou tirar dúvidas técnicas profundas.",
        "dados": "Nenhum dado de entrada — é conteúdo de leitura, organizado em seções expansíveis.",
        "preenchimento": "Não há campos. Basta abrir as seções (expanders) que interessam e ler no seu ritmo.",
        "apos_executar": "Não há processamento — é documentação pura, sempre disponível.",
        "interpretar": "Cada seção cobre um estágio do pipeline. Leia na ordem para uma visão completa, ou pule direto para o tópico que precisa.",
        "exemplos": "Quer saber o que é 'consenso Bayesiano' ou 'linha reta geodésica'? Estão explicados aqui em linguagem acessível.",
        "erros_comuns": "Nenhum — é apenas leitura. Se um termo parecer difícil, há sempre uma analogia do cotidiano.",
        "dicas": "Comece pela 'Visão Geral' se for novo. Use esta aba como referência sempre que encontrar um termo técnico nas outras telas.",
    },
    "manual": {
        "o_que_faz": "É o **manual operacional prático** — o passo a passo do dia a dia de cada funcionalidade, voltado a todos os usuários, do iniciante ao avançado.",
        "quando_usar": "Quando você quer um guia rápido de 'como faço tal coisa' sem precisar entender a teoria por trás.",
        "dados": "Nenhum — é conteúdo de leitura organizado por tarefa.",
        "preenchimento": "Não há campos. Abra a seção da tarefa que você quer executar e siga os passos.",
        "apos_executar": "Não há processamento — é guia de referência sempre disponível.",
        "interpretar": "Cada seção é um 'como fazer' independente. Leia a que corresponde à sua necessidade imediata.",
        "exemplos": "'Como processar uma planilha em lote?' → a seção correspondente traz o passo a passo completo.",
        "erros_comuns": "Nenhum — é leitura. O FAQ ao final responde as dúvidas mais frequentes.",
        "dicas": "Combine com a Enciclopédia: o Manual diz **como fazer**, a Enciclopédia explica **por que funciona**.",
    },
    "motores": {
        "o_que_faz": "Mostra a **saúde técnica** do sistema: quais APIs de mapa estão respondendo bem, tempos de resposta, taxa de falhas e a integridade matemática do motor geodésico.",
        "quando_usar": "Quando o sistema está lento ou um resultado parece estranho, e você quer verificar se algum provedor de mapas caiu ou está instável.",
        "dados": "Usa as estatísticas acumuladas das chamadas de API e o último lote processado.",
        "preenchimento": "Não há campos a preencher — apenas leitura dos painéis. Abra 'Capacidade do Servidor' para ver os recursos disponíveis.",
        "apos_executar": "Não há processamento — os painéis refletem o estado atual em tempo real conforme você usa o sistema.",
        "interpretar": "**Verde/Estável** = provedor saudável. **Instável/Erros** = aquele provedor falhou e o sistema usou fallbacks automáticos. Latência alta = rede lenta naquele parceiro.",
        "exemplos": "Os dois motores de rota (Google + OSRM) rodam sempre; se um falhar, o outro assume. Se ambos falharem, entra a Projeção Geodésica (estimativa por linha reta).",
        "erros_comuns": "Estranhar 'N/A' antes de processar qualquer rota (ainda não há estatística); confundir lentidão de rede com erro do sistema.",
        "dicas": "Tempos médios altos não significam erro — significam que as APIs externas estão lentas. O sistema sempre tem motores de reserva.",
    },
    "auditoria": {
        "o_que_faz": "É a **caixa-preta aberta** do sistema (XAI - Inteligência Artificial Explicável). Mostra, para cada coordenada, exatamente qual algoritmo decidiu, quais fontes concordaram e por que outras opções foram descartadas.",
        "quando_usar": "Quando você desconfia que o sistema colocou um endereço na cidade errada e quer ver o **raciocínio completo** por trás daquela decisão.",
        "dados": "Usa os logs de decisão do último lote e da última alocação processados.",
        "preenchimento": "Não há campos — apenas consulte as tabelas de decisão. Use a busca do navegador (Ctrl+F) para achar um endereço específico.",
        "apos_executar": "Não há processamento — exibe o histórico de decisões já tomadas, com total rastreabilidade.",
        "interpretar": "A coluna **XAI Explicabilidade** narra a dedução lógica: quais APIs foram consultadas, qual venceu, e o cruzamento que levou à coordenada final.",
        "exemplos": "Pesquise pela rua suspeita na tabela → veja que '3 de 4 fontes concordaram no ponto X, a 4ª foi descartada por estar fora do estado'.",
        "erros_comuns": "Entrar aqui sem ter processado nada (tabelas vazias); esperar dados de rotas individuais (a auditoria cobre lotes).",
        "dicas": "Esta é a aba da transparência total: nenhum resultado é caixa-preta. Use-a para justificar decisões a clientes ou auditores.",
    },
}

def renderizar_guia_aba(chave_aba: str):
    """[F-NEW2 - 4ª geração] Renderiza a seção padronizada e obrigatória
    '❓ Como usar esta aba (passo a passo para iniciantes)' de forma consistente
    em todas as 10 abas. Conteúdo escrito para quem nunca viu geocodificação.
    """
    import streamlit as _st
    g = _GUIA_ABAS.get(chave_aba)
    if not g:
        return
    with _st.expander("❓ Como usar esta aba (passo a passo para iniciantes)", expanded=False):
        _st.markdown(f"""
        **📌 O que esta aba faz**
        {g['o_que_faz']}

        **🕐 Quando utilizar**
        {g['quando_usar']}

        **📥 Quais dados inserir**
        {g['dados']}

        **✍️ Como preencher corretamente**
        {g['preenchimento']}

        **⚙️ O que acontece após executar**
        {g['apos_executar']}

        **📊 Como interpretar os resultados**
        {g['interpretar']}

        **💡 Exemplos práticos**
        {g['exemplos']}

        **⚠️ Erros mais comuns**
        {g['erros_comuns']}

        **✅ Dicas e boas práticas**
        {g['dicas']}
        """)

def _formatar_duracao(segundos: float) -> str:
    """Formata segundos em texto legível: 'X minuto(s) e Y segundo(s)'."""
    segundos = max(0, int(round(segundos)))
    if segundos < 60:
        return f"{segundos} segundo{'s' if segundos != 1 else ''}"
    minutos = segundos // 60
    resto = segundos % 60
    if minutos < 60:
        txt = f"{minutos} minuto{'s' if minutos != 1 else ''}"
        if resto > 0:
            txt += f" e {resto} segundo{'s' if resto != 1 else ''}"
        return txt
    horas = minutos // 60
    min_resto = minutos % 60
    txt = f"{horas} hora{'s' if horas != 1 else ''}"
    if min_resto > 0:
        txt += f" e {min_resto} minuto{'s' if min_resto != 1 else ''}"
    return txt

def estimar_tempo_processamento(n_rotas_unicas: int, tipo="lote"):
    """[SPEED-2 / Etapa 5 - 9ª geração] Estimativa DINÂMICA de tempo de processamento.
    
    Baseia-se no histórico REAL de execuções (cache_historico_lotes). Calcula a média
    ponderada de 'Tempo Médio/Rota (s)' das execuções passadas, dando mais peso às
    recentes (que refletem o estado atual da rede/cache). Quanto mais a aplicação é
    usada, mais precisa fica a estimativa. Retorna (texto_estimativa, baseline_usado,
    n_amostras). Se não há histórico suficiente, usa um baseline conservador documentado.
    """
    try:
        registros = []
        prefixo = "alocacao_" if tipo == "alocacao" else "lote_"
        for chave in cache_historico_lotes:
            if not str(chave).startswith(prefixo):
                continue
            try:
                d = cache_historico_lotes.get(chave)
                if d and d.get("Tempo Médio/Rota (s)", 0) > 0 and d.get("Linhas Validadas", 0) > 0:
                    registros.append((float(chave.split("_", 1)[1]), d))  # (timestamp, dado)
            except Exception:
                continue
                
        if registros:
            # Ordena por timestamp (mais recente por último) e pondera exponencialmente
            registros.sort(key=lambda x: x[0])
            amostras = registros[-20:]  # últimas 20 execuções
            soma_pond = 0.0
            soma_pesos = 0.0
            for i, (_ts, d) in enumerate(amostras):
                peso = 1.5 ** i  # execuções recentes pesam mais
                soma_pond += d["Tempo Médio/Rota (s)"] * peso
                soma_pesos += peso
            tempo_por_rota = soma_pond / soma_pesos if soma_pesos > 0 else 0.5
            n_amostras = len(amostras)
            baseline = "histórico real"
        else:
            # Baseline conservador (cache vazio): ~0.4s/rota com pré-aquecimento e cache.
            # Valor documentado; será substituído por dados reais após o 1º lote.
            tempo_por_rota = 0.4
            n_amostras = 0
            baseline = "estimativa inicial (sem histórico ainda)"
            
        tempo_estimado = n_rotas_unicas * tempo_por_rota
        return _formatar_duracao(tempo_estimado), baseline, n_amostras, tempo_por_rota
    except Exception:
        return None, "indisponível", 0, 0.0

@st.cache_data(show_spinner=False)
def _contar_rotas_unicas_preview(file_id, n_linhas, _origens, _destinos):
    """[PERF-UI1 - 15ª geração] Conta rotas únicas (pares origem-destino válidos) com
    cache do Streamlit, chaveado APENAS pela identidade do arquivo (file_id = nome+
    tamanho) e nº de linhas. Os argumentos _origens/_destinos têm prefixo '_' para que
    o Streamlit NÃO os inclua no hash da chave de cache (senão hashear 100k itens
    custaria tanto quanto o cálculo). Antes, a prévia recalculava set(zip(...)) sobre
    TODO o DataFrame a CADA rerun (cada tecla no campo de operador) — desperdício real
    em planilhas grandes. Agora só recalcula quando o arquivo muda. Lógica idêntica."""
    pares = set()
    for o, d in zip(_origens, _destinos):
        o_s = str(o).strip() if o is not None else ''
        d_s = str(d).strip() if d is not None else ''
        if o_s and d_s and o_s.lower() != 'nan' and d_s.lower() != 'nan':
            pares.add((o_s, d_s))
    return len(pares)

def renderizar_scorecard_qualidade(df_resultado):
    """[F-NEW1 - 3ª geração] Painel de Qualidade dos Dados Geográficos.
    
    Calcula e exibe indicadores agregados de qualidade da geocodificação de um lote:
    taxa de sucesso, distribuição de confiança, detecção de anomalias e cobertura
    de fontes. Tudo derivado de colunas já existentes — custo computacional trivial.
    Atende Etapa 7 (Analytics: indicadores de qualidade, precisão e sucesso).
    """
    import streamlit as _st
    total = len(df_resultado)
    if total == 0:
        return

    # --- Métricas de sucesso e falha ---
    lat_o = pd.to_numeric(df_resultado.get('Lat Origem', 0), errors='coerce').fillna(0)
    lat_d = pd.to_numeric(df_resultado.get('Lat Destino', 0), errors='coerce').fillna(0)
    geocodificados = int(((lat_o != 0) & (lat_d != 0)).sum())
    taxa_sucesso = round(100 * geocodificados / total, 1) if total else 0.0

    # --- Distribuição de confiança (Score Final Global) ---
    score_col = pd.to_numeric(df_resultado.get('Score Final Global', 0), errors='coerce').fillna(0)
    excelente = int((score_col >= 90).sum())
    boa = int(((score_col >= 80) & (score_col < 90)).sum())
    aceitavel = int(((score_col >= 70) & (score_col < 80)).sum())
    revisar = int(((score_col > 0) & (score_col < 70)).sum())

    # --- Detecção de anomalias geográficas ---
    dist_via = pd.to_numeric(df_resultado.get('Distancia', 0), errors='coerce').fillna(0)
    linha_reta = pd.to_numeric(df_resultado.get('Linha Reta', 0), errors='coerce').fillna(0)
    # Anomalia: distância viária absurdamente maior que linha reta (possível erro de rota)
    mask_ratio = linha_reta > 0
    ratio = (dist_via[mask_ratio] / linha_reta[mask_ratio]).replace([float('inf')], 0)
    anomalias_ratio = int((ratio > 4.0).sum())
    # Rotas com distância zero mas pontos distintos (suspeita de cache poisoning residual)
    zeros_suspeitos = int(((dist_via == 0) & (lat_o != 0) & (lat_d != 0)).sum())

    _st.markdown("### 🎯 Scorecard de Qualidade dos Dados Geográficos")
    _st.caption("Indicadores automáticos de confiabilidade do lote processado. Quanto mais verde, mais confiável o resultado.")

    c1, c2, c3, c4 = _st.columns(4)
    c1.metric("Taxa de Geocodificação", f"{taxa_sucesso}%",
              help="Percentual de rotas em que origem E destino foram localizados com sucesso na malha geográfica.")
    c2.metric("Alta Confiança (≥80)", f"{excelente + boa}",
              delta=f"{round(100*(excelente+boa)/total,1)}% do total" if total else "0%",
              help="Rotas com score de confiança igual ou superior a 80 — mercadoria chega à porta correta.")
    c3.metric("Requerem Revisão (<70)", f"{revisar}",
              delta=f"-{round(100*revisar/total,1)}%" if total else "0%", delta_color="inverse",
              help="Rotas com baixa confiança que merecem checagem manual do endereço.")
    c4.metric("Anomalias Detectadas", f"{anomalias_ratio + zeros_suspeitos}",
              delta_color="inverse",
              help="Rotas com desvio viário implausível (>4× a linha reta) ou distância zero suspeita.")

    # Barra de distribuição visual de qualidade
    _st.markdown("**Distribuição de Qualidade do Lote:**")
    if total > 0:
        seg = lambda n, cor, lbl: (f"<div style='flex:{max(n,0.001)}; background:{cor}; height:28px; "
                                   f"display:flex; align-items:center; justify-content:center; "
                                   f"color:white; font-size:11px; font-weight:600;' "
                                   f"title='{lbl}: {n}'>{n if n/total > 0.04 else ''}</div>")
        barra = ("<div style='display:flex; width:100%; border-radius:6px; overflow:hidden; margin:8px 0;'>"
                 + seg(excelente, "#2ECC71", "Excelente")
                 + seg(boa, "#3498DB", "Boa")
                 + seg(aceitavel, "#F1C40F", "Aceitável")
                 + seg(revisar, "#E74C3C", "Revisar")
                 + "</div>")
        _st.markdown(barra, unsafe_allow_html=True)
        _st.caption("🟢 Excelente (≥90) · 🔵 Boa (80-89) · 🟡 Aceitável (70-79) · 🔴 Revisar (<70)")

    # Alertas acionáveis de auditoria
    if zeros_suspeitos > 0:
        _st.warning(f"⚠️ {zeros_suspeitos} rota(s) com distância zero entre pontos distintos. "
                    f"O motor anti-cache-poisoning normalmente corrige isso, mas vale auditar na aba 🔍 Auditoria.")
    if anomalias_ratio > 0:
        _st.warning(f"⚠️ {anomalias_ratio} rota(s) com desvio viário acima de 4× a linha reta. "
                    f"Pode indicar travessia de balsa, barreira geográfica real, ou erro de roteamento. Verifique os links.")
    if taxa_sucesso == 100.0 and anomalias_ratio == 0 and zeros_suspeitos == 0:
        _st.success("✅ Lote íntegro: 100% geocodificado, sem anomalias geográficas detectadas.")

def gerar_insights_automaticos(df_kpi):
    """[F-NEW3 - 4ª geração] Descoberta automática de padrões e anomalias.
    
    Varre o DataFrame filtrado e gera frases de insight em linguagem natural,
    destacando o que mais chama atenção: concentração geográfica, outliers de
    distância, faixas de qualidade dominantes e fontes de geocodificação.
    Tudo via agregações pandas vetorizadas — custo trivial, zero chamadas externas.
    Retorna lista de tuplas (tipo, texto) onde tipo ∈ {info, sucesso, alerta}.
    """
    insights = []
    total = len(df_kpi)
    if total == 0:
        return insights
    try:
        # 1. Concentração geográfica (regra de Pareto)
        if 'UF_Sintetica_Origem' in df_kpi.columns:
            top_uf = df_kpi['UF_Sintetica_Origem'].value_counts()
            if len(top_uf) > 0:
                uf_lider = top_uf.index[0]
                pct = round(100 * top_uf.iloc[0] / total, 1)
                if pct >= 40:
                    insights.append(("info", f"📍 **Concentração geográfica:** {pct}% das rotas partem de **{uf_lider}**. "
                                             f"Uma única UF domina a operação — considere otimizar logística regional."))

        # 2. Distância: outliers e média
        if 'Distancia' in df_kpi.columns:
            dist = pd.to_numeric(df_kpi['Distancia'], errors='coerce').fillna(0)
            dist_validas = dist[dist > 0]
            if len(dist_validas) > 0:
                media = dist_validas.mean()
                p95 = dist_validas.quantile(0.95)
                maxd = dist_validas.max()
                if maxd > media * 3 and len(dist_validas) >= 5:
                    insights.append(("alerta", f"📏 **Outlier de distância:** a rota mais longa ({maxd:.0f} km) é "
                                               f"{round(maxd/media,1)}× a média ({media:.0f} km). Vale conferir se não há erro de endereço."))
                insights.append(("info", f"📊 **Perfil de distância:** média de {media:.0f} km; "
                                         f"95% das rotas têm até {p95:.0f} km."))

        # 3. Qualidade dominante
        if 'Status da Rota' in df_kpi.columns:
            status = df_kpi['Status da Rota'].value_counts()
            if len(status) > 0:
                revisar = int(df_kpi['Status da Rota'].isin(['Revisar', 'Erro', 'Erro Crítico de Processamento']).sum())
                pct_revisar = round(100 * revisar / total, 1)
                if pct_revisar >= 20:
                    insights.append(("alerta", f"⚠️ **Atenção à qualidade:** {pct_revisar}% das rotas ({revisar}) precisam de revisão. "
                                               f"Endereços incompletos podem ser a causa — adicione cidade e UF."))
                elif pct_revisar <= 5:
                    insights.append(("sucesso", f"✅ **Alta qualidade:** apenas {pct_revisar}% das rotas requerem revisão. "
                                                f"Excelente padronização dos endereços de entrada."))

        # 4. Fonte de geocodificação predominante
        if 'Fonte Geocoding Origem' in df_kpi.columns:
            fontes = df_kpi['Fonte Geocoding Origem'].value_counts()
            if len(fontes) > 0:
                fonte_lider = fontes.index[0]
                pct_f = round(100 * fontes.iloc[0] / total, 1)
                insights.append(("info", f"🛰️ **Fonte dominante:** {pct_f}% das geocodificações vieram de **{fonte_lider}**. "
                                         f"Diversidade de fontes aumenta a robustez do consenso."))

        # 5. Uso de balsas (insight logístico específico)
        if 'Balsas' in df_kpi.columns:
            balsas = int((df_kpi['Balsas'] == 'Sim').sum())
            if balsas > 0:
                insights.append(("alerta", f"⛴️ **Travessias de balsa:** {balsas} rota(s) exigem balsa. "
                                           f"Isso impacta prazo e custo — sinalize no planejamento."))
    except Exception:
        pass
    return insights

# [M14 - 29ª geração] Credencial TomTom via st.secrets (não mais hardcoded no corpo do
# código). Se não houver secrets.toml ou a chave não estiver definida, recai para string
# vazia → o motor TomTom é desativado graciosamente (mesmo comportamento atual, mas agora
# a chave pode ser configurada sem editar o código). As credenciais SMTP já usavam secrets.
try:
    TOMTOM_API_KEY = st.secrets.get("TOMTOM_API_KEY", "")
except Exception:
    TOMTOM_API_KEY = ""  # Sem secrets configurados → TomTom desativado (degradação graciosa)

# [VIS-DINAMICA - 30ª geração] Chave (opcional) da Google Maps Embed API. Se configurada,
# o mapa embarcado do cenário "Google vence" usa o endpoint OFICIAL e suportado
# /maps/embed/v1/directions (traça a rota de forma garantida, com fit bounds e nomes).
# Sem chave, recai para o embed clássico ?saddr&daddr&output=embed COM NOMES (que também
# desenha as direções). Configurar a chave garante 100% o traçado da rota do Google.
try:
    GOOGLE_MAPS_EMBED_API_KEY = st.secrets.get("GOOGLE_MAPS_EMBED_API_KEY", "")
except Exception:
    GOOGLE_MAPS_EMBED_API_KEY = ""

# ==============================================================================
# CONSTANTES GLOBAIS — Definidas uma única vez, referenciadas em todo o sistema
# ==============================================================================
CACHE_VERSION = "V66"  # Incrementar ao alterar esquema de cache
CACHE_EXPIRE_PADRAO = 2592000  # 30 dias em segundos

NOVAS_COLUNAS_PADRAO = [
    'Distancia', 'Tempo', 'Link da Rota', 'Balsas', 'Motivo Roteamento',
    'Status Linha Reta', 'Linha Reta', 'Fonte da Rota', 'Score da Rota',
    'Confianca Origem', 'Score Num Origem', 'Distrito Origem', 'Municipio Origem',
    'Fonte Geocoding Origem', 'Endereco Oficial Origem', 'Confianca Destino',
    'Score Num Destino', 'Distrito Destino', 'Municipio Destino',
    'Fonte Geocoding Destino', 'Endereco Oficial Destino',
    'Lat Origem', 'Lon Origem', 'Lat Destino', 'Lon Destino',
    'Tempo Geocoding (s)', 'Tempo Roteamento (s)', 'Tempo Total (s)',
    'Score Final Global', 'Status da Rota'
]

COLUNAS_NUMERICAS_PADRAO = [
    'Distancia', 'Linha Reta', 'Score da Rota', 'Score Num Origem',
    'Score Num Destino', 'Lat Origem', 'Lon Origem', 'Lat Destino',
    'Lon Destino', 'Tempo Geocoding (s)', 'Tempo Roteamento (s)',
    'Tempo Total (s)', 'Score Final Global'
]

NOVAS_COLUNAS_ALOCACAO = NOVAS_COLUNAS_PADRAO + [
    'Concorrente Analisado', 'Distancia Concorrente',
    'Link Rota Concorrente', 'Justificativa de Alocacao'
]

COLUNAS_NUMERICAS_ALOCACAO = COLUNAS_NUMERICAS_PADRAO + ['Distancia Concorrente']

def _df_para_geojson(df):
    """[EXPORT-GIS - 24ª geração] Converte o DataFrame de rotas processadas em GeoJSON
    (padrão aberto RFC 7946). Cada rota vira: um ponto de origem, um ponto de destino e
    uma LineString conectando-os (representação O→D). Compatível com QGIS, ArcGIS, Google
    Earth, Mapbox, Leaflet, kepler.gl, etc. Puramente aditivo: lê colunas já existentes
    (Lat/Lon Origem/Destino), sem afetar o processamento. Coordenadas em [lon, lat]."""
    features = []
    for _, row in df.iterrows():
        try:
            lat_o = float(row.get('Lat Origem', 0) or 0); lon_o = float(row.get('Lon Origem', 0) or 0)
            lat_d = float(row.get('Lat Destino', 0) or 0); lon_d = float(row.get('Lon Destino', 0) or 0)
        except (ValueError, TypeError):
            continue
        if lat_o == 0 and lon_o == 0 and lat_d == 0 and lon_d == 0:
            continue
        props_base = {
            "origem": str(row.get('Endereco Oficial Origem', row.get('Origem', ''))),
            "destino": str(row.get('Endereco Oficial Destino', row.get('Destino', ''))),
            "distancia_km": row.get('Distancia', ''),
            "tempo": str(row.get('Tempo', '')),
            "fonte_rota": str(row.get('Fonte da Rota', '')),
            "municipio_origem": str(row.get('Municipio Origem', '')),
            "municipio_destino": str(row.get('Municipio Destino', '')),
        }
        if lat_o != 0 or lon_o != 0:
            features.append({"type": "Feature",
                "properties": {**props_base, "tipo": "origem", "marker-color": "#16a34a"},
                "geometry": {"type": "Point", "coordinates": [round(lon_o, 6), round(lat_o, 6)]}})
        if lat_d != 0 or lon_d != 0:
            features.append({"type": "Feature",
                "properties": {**props_base, "tipo": "destino", "marker-color": "#dc2626"},
                "geometry": {"type": "Point", "coordinates": [round(lon_d, 6), round(lat_d, 6)]}})
        if (lat_o != 0 or lon_o != 0) and (lat_d != 0 or lon_d != 0):
            features.append({"type": "Feature",
                "properties": {**props_base, "tipo": "rota", "stroke": "#2563eb", "stroke-width": 3},
                "geometry": {"type": "LineString", "coordinates": [[round(lon_o, 6), round(lat_o, 6)], [round(lon_d, 6), round(lat_d, 6)]]}})
    return json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=1)

def _escapar_xml(texto):
    """Escapa caracteres especiais para XML (KML/GPX)."""
    s = str(texto)
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("'", "&apos;"))

def _escapar_js(texto):
    """[VIS-NAMES - 27ª geração] Escapa um texto para uso seguro dentro de strings JS e
    HTML embarcados no mapa Leaflet (data URI). Neutraliza aspas, barras, sinais de < >
    e quebras de linha — impedindo que um nome de localidade quebre o HTML/JS do mapa."""
    s = str(texto)
    return (s.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
             .replace("<", "&lt;").replace(">", "&gt;")
             .replace("\n", " ").replace("\r", " ").replace("\u2028", " ").replace("\u2029", " "))

def _df_para_kml(df):
    """[EXPORT-GIS - 24ª geração] Converte o DataFrame em KML (Google Earth/Maps). Cada
    rota vira um Placemark de origem, um de destino e uma linha conectando-os. Abre
    diretamente no Google Earth e no QGIS. Lê apenas colunas já existentes."""
    linhas = ['<?xml version="1.0" encoding="UTF-8"?>',
              '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>',
              '<name>Rotas - Motor Nacional de Roteirizacao</name>',
              '<Style id="origem"><IconStyle><color>ff16a34a</color></IconStyle></Style>',
              '<Style id="rota"><LineStyle><color>ffeb6325</color><width>3</width></LineStyle></Style>']
    for _, row in df.iterrows():
        try:
            lat_o = float(row.get('Lat Origem', 0) or 0); lon_o = float(row.get('Lon Origem', 0) or 0)
            lat_d = float(row.get('Lat Destino', 0) or 0); lon_d = float(row.get('Lon Destino', 0) or 0)
        except (ValueError, TypeError):
            continue
        if lat_o == 0 and lon_o == 0 and lat_d == 0 and lon_d == 0:
            continue
        org = _escapar_xml(row.get('Endereco Oficial Origem', row.get('Origem', '')))
        dst = _escapar_xml(row.get('Endereco Oficial Destino', row.get('Destino', '')))
        dist = _escapar_xml(row.get('Distancia', '')); tmp = _escapar_xml(row.get('Tempo', ''))
        desc = f"<description>Distancia: {dist} km | Tempo: {tmp}</description>"
        if lat_o != 0 or lon_o != 0:
            linhas.append(f'<Placemark><name>Origem: {org}</name>{desc}<Point><coordinates>{lon_o},{lat_o},0</coordinates></Point></Placemark>')
        if lat_d != 0 or lon_d != 0:
            linhas.append(f'<Placemark><name>Destino: {dst}</name>{desc}<Point><coordinates>{lon_d},{lat_d},0</coordinates></Point></Placemark>')
        if (lat_o != 0 or lon_o != 0) and (lat_d != 0 or lon_d != 0):
            linhas.append(f'<Placemark><name>Rota: {org} - {dst}</name>{desc}<styleUrl>#rota</styleUrl>'
                          f'<LineString><coordinates>{lon_o},{lat_o},0 {lon_d},{lat_d},0</coordinates></LineString></Placemark>')
    linhas.append('</Document></kml>')
    return "\n".join(linhas)

def _df_para_gpx(df):
    """[EXPORT-GIS - 24ª geração] Converte o DataFrame em GPX (GPS Exchange Format), para
    dispositivos GPS, Garmin, e apps de navegação. Cada rota vira um waypoint de origem,
    um de destino e uma <rte> (rota) com os dois pontos. Lê apenas colunas existentes."""
    linhas = ['<?xml version="1.0" encoding="UTF-8"?>',
              '<gpx version="1.1" creator="Motor Nacional de Roteirizacao" xmlns="http://www.topografix.com/GPX/1/1">']
    rotas_xml = []
    for idx, row in df.iterrows():
        try:
            lat_o = float(row.get('Lat Origem', 0) or 0); lon_o = float(row.get('Lon Origem', 0) or 0)
            lat_d = float(row.get('Lat Destino', 0) or 0); lon_d = float(row.get('Lon Destino', 0) or 0)
        except (ValueError, TypeError):
            continue
        if lat_o == 0 and lon_o == 0 and lat_d == 0 and lon_d == 0:
            continue
        org = _escapar_xml(row.get('Municipio Origem', row.get('Origem', '')))
        dst = _escapar_xml(row.get('Municipio Destino', row.get('Destino', '')))
        if lat_o != 0 or lon_o != 0:
            linhas.append(f'<wpt lat="{lat_o}" lon="{lon_o}"><name>{org}</name></wpt>')
        if lat_d != 0 or lon_d != 0:
            linhas.append(f'<wpt lat="{lat_d}" lon="{lon_d}"><name>{dst}</name></wpt>')
        if (lat_o != 0 or lon_o != 0) and (lat_d != 0 or lon_d != 0):
            rotas_xml.append(f'<rte><name>{org} - {dst}</name>'
                             f'<rtept lat="{lat_o}" lon="{lon_o}"><name>{org}</name></rtept>'
                             f'<rtept lat="{lat_d}" lon="{lon_d}"><name>{dst}</name></rtept></rte>')
    linhas.extend(rotas_xml)
    linhas.append('</gpx>')
    return "\n".join(linhas)

def _contar_rotas_geo_validas(df):
    """Conta quantas linhas têm ao menos um par de coordenadas válido (para o usuário
    saber se a exportação GIS terá conteúdo)."""
    n = 0
    for _, row in df.iterrows():
        try:
            coords = [float(row.get(c, 0) or 0) for c in ('Lat Origem', 'Lon Origem', 'Lat Destino', 'Lon Destino')]
            if any(c != 0 for c in coords):
                n += 1
        except (ValueError, TypeError):
            continue
    return n

MAPA_PRIORIDADE_GLOBAL = {
    "CEP": 1, "ENDERECO_COMPLETO": 2, "POI": 3, "CONDOMINIO": 3,
    "MUNICIPIO": 4, "BAIRRO": 5, "RURAL": 6, "LOGRADOURO": 7
}

CONFIANCA_ALTISSIMA = "ALTISSIMA"
CONFIANCA_ALTA      = "ALTA"
CONFIANCA_MEDIA     = "MEDIA"
CONFIANCA_BAIXA     = "BAIXA"
CONFIANCA_REVISAO   = "REVISAO_MANUAL"
CONFIANCA_MUNICIPAL = "MUNICIPAL"
CONFIANCA_ABSOLUTA  = "ABSOLUTA"

# ==============================================================================
# PERSISTÊNCIA EM DISCO E HIGIENIZAÇÃO DE AMBIENTE (GARBAGE COLLECTION)
# ==============================================================================
cache_classificacao = Cache("./cache_classificacao")
cache_fuzzy = Cache("./cache_fuzzy")
cache_geo = Cache("./cache_geo")
cache_rotas = Cache("./cache_rotas")
cache_poi = Cache("./cache_poi")
cache_cep = Cache("./cache_cep")
cache_google = Cache("./cache_google")
cache_reverse = Cache("./cache_reverse")
cache_base_local = Cache("./cache_base_local")
cache_aprendizado = Cache("./cache_aprendizado")
cache_aprendizado_auto = Cache("./cache_aprendizado_auto")
cache_api_health = Cache("./cache_api_health")
cache_historico_lotes = Cache("./cache_historico_lotes")


# [M12] Thread-safe LRU Cache — substitui LRUDict manual sem proteção de concorrência
# cachetools.LRUCache usa lock interno; fallback para OrderedDict+Lock se não disponível
if _CACHETOOLS_DISPONIVEL:
    _lru_cache_lock = threading.Lock()
    class LRUDict:
        """Wrapper thread-safe sobre cachetools.LRUCache para compatibilidade de API."""
        def __init__(self, maxsize=5000):
            self.maxsize = maxsize
            self._cache = _CacheToolsLRU(maxsize=maxsize)
            self._lock = threading.Lock()
        def __contains__(self, key):
            with self._lock: return key in self._cache
        def __setitem__(self, key, value):
            with self._lock: self._cache[key] = value
        def __getitem__(self, key):
            with self._lock: return self._cache[key]
        def get(self, key, default=None):
            with self._lock:
                try: return self._cache[key]
                except KeyError: return default
        def __len__(self): return len(self._cache)
else:
    class LRUDict(collections.OrderedDict):
        """Fallback: OrderedDict com lock explícito para thread-safety."""
        def __init__(self, maxsize=5000):
            super().__init__()
            self.maxsize = maxsize
            self._lock = threading.Lock()
        def __setitem__(self, key, value):
            with self._lock:
                super().__setitem__(key, value)
                self.move_to_end(key)
                if len(self) > self.maxsize:
                    self.popitem(last=False)
        def __getitem__(self, key):
            with self._lock:
                value = super().__getitem__(key)
                self.move_to_end(key)
                return value
        def __contains__(self, key):
            with self._lock: return super().__contains__(key)

# [P33 - 3ª geração] L1 cache ampliado 5.000 → 20.000 entradas. Cada entrada ~2KB,
# então 20k ≈ 40MB de RAM — custo trivial que eleva a taxa de cache-hit em lotes
# grandes com muitas rotas repetidas (cenário B2B comum: mesmos hubs, muitos clientes).
CACHE_L1_ROTAS = LRUDict(maxsize=20000)

# [PICKLE-SAFE - 34ª geração] CORREÇÃO DEFINITIVA do _pickle.PicklingError.
# O diskcache serializa valores via pickle. Embora o RotaPipeline seja definido em nível
# de módulo (picklável) e os executores sejam @st.cache_resource (não serializados), uma
# gravação de cache JAMAIS deve poder derrubar o processamento de um lote inteiro. Este
# wrapper torna TODA escrita em diskcache resiliente: se por qualquer caminho raro um valor
# não puder ser serializado (PicklingError, TypeError, etc.), o erro é absorvido e logado —
# o cache L1 (RAM) continua válido e o valor é recomputável. Resultado: a falha de
# serialização deixa de ser fatal, em definitivo, sem afetar precisão nem resultado.
def _cache_set_seguro(cache, chave, valor, expire=2592000):
    try:
        cache.set(chave, valor, expire=expire)
        return True
    except Exception as e:  # PicklingError/TypeError/AttributeError/etc. — nunca fatal
        try:
            logger.warning(f"[PICKLE-SAFE] Cache não persistido (degradação graciosa, L1 mantém): {type(e).__name__}")
        except Exception:
            pass
        return False

# [M20] Pré-instanciar 3 DBSCANs com eps fixos — elimina instanciação por geocodificação
_DBSCAN_PRESETS = {
    0.5:  DBSCAN(eps=0.5 / 6371.0,  min_samples=2, metric='haversine'),
    2.0:  DBSCAN(eps=2.0 / 6371.0,  min_samples=2, metric='haversine'),
    10.0: DBSCAN(eps=10.0 / 6371.0, min_samples=2, metric='haversine'),
}

# [M22] Migração por schema: não limpar caches válidos entre sessões
# Apenas marca a sessão atual como inicializada; dados persistem entre reloads
if f"cache_inicializado_{CACHE_VERSION}" not in st.session_state:
    # Limpa apenas se schema mudou — compara tag de versão armazenada no cache
    schema_tag_key = f"__schema_version__"
    schema_atual = cache_geo.get(schema_tag_key, "")
    if schema_atual != CACHE_VERSION:
        logger.info(f"[M22] Schema alterado ({schema_atual} → {CACHE_VERSION}). Limpando caches estruturais.")
        for c in [cache_classificacao, cache_fuzzy, cache_geo, cache_rotas, cache_poi,
                  cache_cep, cache_google, cache_reverse, cache_base_local,
                  cache_aprendizado, cache_aprendizado_auto]:
            c.clear()
        _cache_set_seguro(cache_geo, schema_tag_key, CACHE_VERSION, expire=None)
    else:
        logger.info(f"[M22] Schema {CACHE_VERSION} compatível. Caches preservados.")
    st.session_state[f"cache_inicializado_{CACHE_VERSION}"] = True

def realizar_manutencao_logs_google():
    diretorio_logs = "logs_google"
    os.makedirs(diretorio_logs, exist_ok=True)
    limite_tempo = time.time() - (30 * 86400)
    try:
        for arquivo in os.listdir(diretorio_logs):
            caminho_completo = os.path.join(diretorio_logs, arquivo)
            if os.path.isfile(caminho_completo) and os.path.getmtime(caminho_completo) < limite_tempo:
                os.remove(caminho_completo)
    except Exception:
        pass

realizar_manutencao_logs_google()

session = requests.Session()
retry_strategy = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
# [PERF-NET - 33ª geração] pool_maxsize alinhado ao TETO de workers (32). O pool de rotas
# pode chegar a min(32, cpu*4)=32 threads, todas batendo no MESMO host (OSRM/Google) na
# fase de roteamento — a fase de rede dominante em lotes cidade-a-cidade (geocodificação
# de municípios é offline via IBGE). Com pool_maxsize=24, as 8 conexões excedentes eram
# DESCARTADAS pelo urllib3 ("connection pool is full"), pagando handshake TLS NOVO (~100-300ms)
# a cada chamada. Com 32, todas reusam conexões keep-alive do pool. Conexões são lazy (criadas
# sob demanda), então em máquinas pequenas (8 workers) não há desperdício. Zero risco.
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=32, pool_maxsize=32)
session.mount("https://", adapter)
session.mount("http://", adapter)
# [G21] Cookie CONSENT hardcoded removido — token de 2023 expirado e desnecessário
# User-Agent moderno suficiente para requests de roteamento

CACHE_IBGE_PATH = "municipios_ibge_v2.pkl"   # [BASE-IBGE-COD] v2: base enriquecida com código IBGE oficial; força reconstrução do pkl antigo

# ==============================================================================
# INFRAESTRUTURA DE CONCORRÊNCIA E FILAS (THREAD-SAFE GLOBALS)
# ==============================================================================
# [P32 - 3ª geração] Workers adaptativos ao hardware. Carga é I/O-bound (espera de
# rede), então o nº de threads pode exceder o nº de CPUs com segurança. Fórmula:
# min(32, cpu*4) para o pool de rotas — maximiza throughput sem saturar o agendador.
_CPU_COUNT = os.cpu_count() or 4
WORKERS_DISPONIVEIS = min(32, max(8, _CPU_COUNT * 4))

# [M2 - 29ª geração] Executores como SINGLETONS via @st.cache_resource.
# CAUSA RAIZ: como este é o script principal do Streamlit, todo o corpo do módulo
# RE-EXECUTA a cada rerun. Definir os ThreadPoolExecutor como globais soltas fazia um
# NOVO pool ser criado a cada interação (churn de threads / GC do pool antigo a cada
# rerun, inclusive entre os chunks do lote, que dependem de st.rerun). Com cache_resource,
# o pool é criado UMA vez e reusado em todos os reruns — estável durante todo o lote.
# NOTA: cache_resource é um singleton por PROCESSO (compartilhado entre sessões), igual a
# uma global — não isola por usuário (isso não muda o comportamento atual de compartilhar
# o pool; apenas elimina a recriação por rerun). Os nomes globais abaixo são preservados,
# então TODAS as referências existentes (EXECUTOR_GLOBAL, etc.) continuam funcionando.
@st.cache_resource(show_spinner=False)
def _obter_executor_global():
    return ThreadPoolExecutor(max_workers=WORKERS_DISPONIVEIS, thread_name_prefix="rota")

@st.cache_resource(show_spinner=False)
def _obter_fila_nominatim():
    return ThreadPoolExecutor(max_workers=1, thread_name_prefix="nominatim")  # rate-limit 1 req/s obrigatório

@st.cache_resource(show_spinner=False)
def _obter_executor_apis():
    return ThreadPoolExecutor(max_workers=min(24, _CPU_COUNT * 3), thread_name_prefix="geoapi")

EXECUTOR_GLOBAL = _obter_executor_global()
FILA_NOMINATIM = _obter_fila_nominatim()
EXECUTOR_APIS = _obter_executor_apis()

# [NOMINATIM-THROTTLE - 33ª geração] Rate limiter DELTA-BASED para o Nominatim.
# GARGALO: a política do Nominatim exige no máximo 1 req/s. Antes, cada chamada dormia
# 1.1s FIXO *antes* da request (time.sleep(1.1)) — esse sleep SOMAVA ao tempo da própria
# request, espaçando os INÍCIOS em ~1.1s + t_request (ex.: 1.6s) → throughput efetivo de
# só ~0.62 req/s. Aqui dormimos apenas o tempo RESTANTE para manter 1.1s entre INÍCIOS de
# chamada (1 req / 1.1s ≈ 0.91 req/s) — respeitando a política, porém ~45% mais rápido no
# Nominatim, que é o ponto serial dominante em lotes com endereços/POIs/reverse.
# SEGURO: todas as chamadas ao Nominatim são serializadas por FILA_NOMINATIM (max_workers=1),
# então o timestamp abaixo é lido/escrito por UMA thread por vez (sem condição de corrida).
# A 1ª chamada não dorme (timestamp inicial 0.0 → espera negativa → sleep 0).
_NOMINATIM_INTERVALO = 1.1   # segundos entre inícios de chamada (1 req/s + margem de 10%)
_NOMINATIM_ULTIMO = 0.0      # epoch da última chamada (atualizado dentro da FILA serial)

def _throttle_nominatim():
    """Espera apenas o delta necessário para manter ~1 req/s no Nominatim (ver nota acima)."""
    global _NOMINATIM_ULTIMO
    espera = _NOMINATIM_INTERVALO - (time.time() - _NOMINATIM_ULTIMO)
    if espera > 0:
        time.sleep(espera)
    _NOMINATIM_ULTIMO = time.time()


# Padrões Regex Globais de Otimização Scraper Google
_RE_DIST_G1 = re.compile(r'\"([\d\.,]+)\s*km\"')
_RE_DIST_G2 = re.compile(r'([\d\.,]+)\s*km')
_RE_DIST_G3 = re.compile(r'\\x22([\d\.,]+)\s*km\\x22')
_RE_DIST_G4 = re.compile(r'(\d+)\s*km')
_RE_TIME_G1 = re.compile(r'\"(\d+\s*h\s*\d+\s*min|\d+\s*h|\d+\s*min)\"')
_RE_TIME_G2 = re.compile(r'(\d+\s*h\s*\d+\s*min|\d+\s*h|\d+\s*min)')
_RE_TIME_G3 = re.compile(r'\\x22(\d+\s*h\s*\d+\s*min|\d+\s*h|\d+\s*min)\\x22')

# [VIS-GOOGLE-GEO - 27ª geração] Padrões para EXTRAIR A GEOMETRIA (polyline codificada)
# da rota a partir da resposta do endpoint maps/preview/directions do Google. A resposta
# embute a polyline da rota em blocos como [[...]],"<polyline>" — capturamos o trecho
# codificado (caracteres ASCII imprimíveis típicos de polyline) para desenhar o traçado
# completo no mapa, em vez de apenas dois marcadores. Vários formatos são tentados em
# ordem de especificidade. A polyline do Google usa precisão 5 (mesma do OSRM polyline).
_RE_GOOG_POLY1 = re.compile(r'\\"([a-zA-Z0-9_~`?@\[\]\\^{|}<>=;:/.\-+*&%$#!()\']{30,})\\"')
_RE_GOOG_POLY2 = re.compile(r'"([a-zA-Z0-9_~`?@\[\]\^{|}<>=;:/.\-+*&%$#!()\']{30,})"')

# [M13] Padrões de rodovia pré-compilados como constante global
# Eliminam recompilação em loop duplo de candidatos no consenso Bayesiano
_PADROES_RODOVIA_COMPILADOS = [
    re.compile(r'\bBR[- ]?\d+\b'), re.compile(r'\bSP[- ]?\d+\b'),
    re.compile(r'\bMG[- ]?\d+\b'), re.compile(r'\bGO[- ]?\d+\b'),
    re.compile(r'\bDF[- ]?\d+\b'), re.compile(r'\bRJ[- ]?\d+\b'),
    re.compile(r'\bPR[- ]?\d+\b'), re.compile(r'\bSC[- ]?\d+\b'),
    re.compile(r'\bRS[- ]?\d+\b'),
]
_RE_RODOVIA_GENERICA = re.compile(r'\b(RODOVIA|KM|ESTRADA)\b')

# ==============================================================================
# DADOS GLOBAIS THREAD-SAFE E EXPANSÃO SEMÂNTICA
# ==============================================================================
SINONIMOS_SEMANTICOS = {
    "UNB": "UNIVERSIDADE DE BRASILIA", 
    "CATOLICA": "UNIVERSIDADE CATOLICA",
    "JK": "JUSCELINO KUBITSCHEK", 
    "HBDF": "HOSPITAL DE BASE DO DISTRITO FEDERAL",
    "HRAN": "HOSPITAL REGIONAL DA ASA NORTE", 
    "RODOVIARIA": "TERMINAL RODOVIARIO",
    "CD": "CENTRO DE DISTRIBUICAO", 
    "HUB": "CENTRO LOGISTICO",
    "FILIAL": "BASE OPERACIONAL", 
    "TECA": "TERMINAL DE CARGAS"
}

# [M14] Globals de buffer de telemetria — flush periódico ao DiskCache
_TELEMETRIA_BUFFER: dict = {}
_TELEMETRIA_CONTADORES: dict = {}

def registrar_telemetria(fonte, sucesso, tempo_gasto):
    global _TELEMETRIA_BUFFER, _TELEMETRIA_CONTADORES
    with _LOCK_METRICAS:
        buf = _TELEMETRIA_BUFFER.setdefault(fonte, {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0})
        buf["calls"] += 1
        buf["tempo_total"] += tempo_gasto
        if sucesso:
            buf["hits"] += 1
        else:
            buf["falhas"] += 1
        _TELEMETRIA_CONTADORES[fonte] = _TELEMETRIA_CONTADORES.get(fonte, 0) + 1
        # Flush a cada 50 chamadas por fonte (balanceia frescor vs I/O)
        if _TELEMETRIA_CONTADORES[fonte] >= 50:
            m = cache_api_health.get(fonte, {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0})
            m["hits"] += buf["hits"]
            m["calls"] += buf["calls"]
            m["falhas"] += buf["falhas"]
            m["tempo_total"] += buf["tempo_total"]
            cache_api_health.set(fonte, m, expire=None)
            _TELEMETRIA_BUFFER[fonte] = {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0}
            _TELEMETRIA_CONTADORES[fonte] = 0

def _flush_telemetria_forcado():
    """Flush imediato de todo o buffer de telemetria — chamado ao final do processamento em lote."""
    with _LOCK_METRICAS:
        for fonte, buf in _TELEMETRIA_BUFFER.items():
            if buf["calls"] > 0:
                m = cache_api_health.get(fonte, {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0})
                m["hits"] += buf["hits"]; m["calls"] += buf["calls"]
                m["falhas"] += buf["falhas"]; m["tempo_total"] += buf["tempo_total"]
                cache_api_health.set(fonte, m, expire=None)
        _TELEMETRIA_BUFFER.clear()
        _TELEMETRIA_CONTADORES.clear()

@st.cache_data
def carregar_dados_ibge():
    if os.path.exists(CACHE_IBGE_PATH):
        if time.time() - os.path.getmtime(CACHE_IBGE_PATH) > (30 * 86400):
            os.remove(CACHE_IBGE_PATH)
        else:
            try:
                with open(CACHE_IBGE_PATH, "rb") as f:
                    d = pickle.load(f)
                    if len(d.get("municipios", {})) > 1000:
                        return d.get("municipios", {}), d.get("estados", {}), d.get("distritos", {}), list(d.get("municipios", {}).keys()) + list(d.get("distritos", {}).keys())
            except Exception: 
                pass

    base_mun, base_est, base_dist = {}, {}, {}
    try:
        r_est = session.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=8)
        if r_est.status_code == 200:
            for est in r_est.json():
                base_est[est["sigla"]] = unidecode(est["nome"]).upper()
                
        r_mun = session.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios", timeout=12)
        if r_mun.status_code == 200:
            for mun in r_mun.json():
                nome_norm = unidecode(mun["nome"]).upper().strip()
                uf_sigla = mun["microrregiao"]["mesorregiao"]["UF"]["sigla"].upper()
                if nome_norm not in base_mun: 
                    base_mun[nome_norm] = []
                base_mun[nome_norm].append({
                    "uf": uf_sigla, 
                    "municipio": nome_norm,
                    "codigo_ibge": mun.get("id"),   # [BASE-IBGE-COD] código oficial 7 díg. (já vem no payload, custo de rede zero)
                    "lat": mun.get("lat", 0.0), 
                    "lon": mun.get("lon", 0.0)
                })
                
        r_dist = session.get("https://servicodados.ibge.gov.br/api/v1/localidades/distritos", timeout=12)
        if r_dist.status_code == 200:
            for dist in r_dist.json():
                nome_dist = unidecode(dist["nome"]).upper().strip()
                nome_muni = unidecode(dist["municipio"]["nome"]).upper().strip()
                uf_dist = dist["municipio"]["microrregiao"]["mesorregiao"]["UF"]["sigla"].upper()
                if nome_dist not in base_dist: 
                    base_dist[nome_dist] = []
                base_dist[nome_dist].append({
                    "uf": uf_dist, 
                    "municipio": nome_muni,
                    "codigo_ibge": dist.get("id"),   # [BASE-IBGE-COD] código oficial do distrito
                    "lat": dist.get("lat", 0.0), 
                    "lon": dist.get("lon", 0.0)
                })
                
        if len(base_mun) > 1000:
            with open(CACHE_IBGE_PATH, "wb") as f:
                pickle.dump({"municipios": base_mun, "estados": base_est, "distritos": base_dist}, f)
    except Exception: 
        pass
        
    lista_completa = list(base_mun.keys()) + list(base_dist.keys())
    return base_mun, base_est, base_dist, lista_completa

IBGE_MUNICIPIOS, IBGE_ESTADOS, IBGE_DISTRITOS, LISTA_TOPONIMOS = carregar_dados_ibge()

# Construção ultra veloz O(1) de Dicionário por UF
IBGE_MUNICIPIOS_POR_UF = {}
for mun, lista_itens in IBGE_MUNICIPIOS.items():
    for item in lista_itens:
        uf = item["uf"]
        if uf not in IBGE_MUNICIPIOS_POR_UF:
            IBGE_MUNICIPIOS_POR_UF[uf] = {}
        if mun not in IBGE_MUNICIPIOS_POR_UF[uf]:
            IBGE_MUNICIPIOS_POR_UF[uf][mun] = []
        IBGE_MUNICIPIOS_POR_UF[uf][mun].append(item)

@st.cache_data
def inicializar_listas_fuzzy(ibge_mun, ibge_dist):
    lista_fuzzy = []
    for k, v_list in ibge_mun.items(): 
        for v in v_list: 
            lista_fuzzy.append(f"{k} {v['uf']}")
    for k, v_list in ibge_dist.items(): 
        for v in v_list: 
            lista_fuzzy.append(f"{k} {v['uf']}")
    return list(set(lista_fuzzy))

LISTA_CONTEXTO_FUZZY = inicializar_listas_fuzzy(IBGE_MUNICIPIOS, IBGE_DISTRITOS)

POI_KEYWORDS = [
    "AEROPORTO", "HOSPITAL", "UNIVERSIDADE", "FACULDADE", "ESCOLA", "SHOPPING", 
    "HOTEL", "RODOVIARIA", "ESTADIO", "MINISTERIO", "AGENCIA", "BANCO", 
    "IGREJA", "FORUM", "TRIBUNAL", "DELEGACIA", "PREFEITURA", "CLINICA",
    "CENTRO DE DISTRIBUICAO", "TERMINAL", "BASE OPERACIONAL"
]

BOUNDING_BOXES_UF = {
    # [G22 - 2ª geração] Cobertura nacional completa dos 27 estados (antes: só DF, SP, GO).
    # A barreira territorial agora valida geocodificações em TODO o Brasil, não apenas 3 UFs.
    # Margens de ~0.3° adicionadas para tolerar pontos de fronteira legítimos.
    "AC": {"lat_min": -11.20, "lat_max": -7.10,  "lon_min": -74.00, "lon_max": -66.60},
    "AL": {"lat_min": -10.60, "lat_max": -8.80,  "lon_min": -38.30, "lon_max": -35.10},
    "AP": {"lat_min": -1.30,  "lat_max": 4.50,   "lon_min": -54.90, "lon_max": -49.80},
    "AM": {"lat_min": -9.90,  "lat_max": 2.30,   "lon_min": -73.90, "lon_max": -56.00},
    "BA": {"lat_min": -18.40, "lat_max": -8.50,  "lon_min": -46.70, "lon_max": -37.30},
    "CE": {"lat_min": -7.90,  "lat_max": -2.70,  "lon_min": -41.50, "lon_max": -37.20},
    "DF": {"lat_min": -16.05, "lat_max": -15.50, "lon_min": -48.30, "lon_max": -47.30},
    "ES": {"lat_min": -21.30, "lat_max": -17.80, "lon_min": -41.90, "lon_max": -39.60},
    "GO": {"lat_min": -19.50, "lat_max": -12.40, "lon_min": -53.30, "lon_max": -45.90},
    "MA": {"lat_min": -10.30, "lat_max": -1.00,  "lon_min": -48.80, "lon_max": -41.70},
    "MT": {"lat_min": -18.10, "lat_max": -7.30,  "lon_min": -61.70, "lon_max": -50.20},
    "MS": {"lat_min": -24.10, "lat_max": -17.10, "lon_min": -58.20, "lon_max": -50.80},
    "MG": {"lat_min": -22.95, "lat_max": -14.20, "lon_min": -51.10, "lon_max": -39.80},
    "PA": {"lat_min": -9.90,  "lat_max": 2.70,   "lon_min": -58.95, "lon_max": -46.00},
    "PB": {"lat_min": -8.40,  "lat_max": -6.00,  "lon_min": -38.80, "lon_max": -34.70},
    "PR": {"lat_min": -26.80, "lat_max": -22.40, "lon_min": -54.70, "lon_max": -48.00},
    "PE": {"lat_min": -9.60,  "lat_max": -7.20,  "lon_min": -41.50, "lon_max": -34.70},
    "PI": {"lat_min": -10.99, "lat_max": -2.70,  "lon_min": -45.99, "lon_max": -40.30},
    "RJ": {"lat_min": -23.45, "lat_max": -20.70, "lon_min": -44.99, "lon_max": -40.90},
    "RN": {"lat_min": -6.80,  "lat_max": -4.70,  "lon_min": -38.70, "lon_max": -34.90},
    "RS": {"lat_min": -33.85, "lat_max": -27.00, "lon_min": -57.80, "lon_max": -49.60},
    "RO": {"lat_min": -13.80, "lat_max": -7.90,  "lon_min": -66.90, "lon_max": -59.70},
    "RR": {"lat_min": -1.70,  "lat_max": 5.40,   "lon_min": -64.95, "lon_max": -58.80},
    "SC": {"lat_min": -29.50, "lat_max": -25.90, "lon_min": -54.00, "lon_max": -48.30},
    "SP": {"lat_min": -25.50, "lat_max": -19.50, "lon_min": -53.50, "lon_max": -44.00},
    "SE": {"lat_min": -11.70, "lat_max": -9.40,  "lon_min": -38.30, "lon_max": -36.30},
    "TO": {"lat_min": -13.60, "lat_max": -5.10,  "lon_min": -50.90, "lon_max": -45.60},
}

IBGE_MUN_UF_SET = {
    (nome, item["uf"])
    for nome, items in IBGE_MUNICIPIOS.items()
    for item in items
}
IBGE_DIST_UF_SET = {
    (nome, item["uf"])
    for nome, items in IBGE_DISTRITOS.items()
    for item in items
}

# ==============================================================================
# CONSTANTES DE ANALYTICS — definidas UMA vez no módulo [PERF-2 - 5ª geração]
# Antes eram recriadas a cada rerun dentro da aba Analytics. Mover para o escopo
# do módulo elimina reconstrução repetida de dicts e recompilação implícita.
# Benefício líquido puro: mesmos objetos, criados uma única vez.
# ==============================================================================
MAPA_ESTADOS_FULL = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA", "CEARA": "CE", "DISTRITO FEDERAL": "DF",
    "ESPIRITO SANTO": "ES", "GOIAS": "GO", "MARANHAO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI", "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS", "RONDONIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SAO PAULO": "SP", "SERGIPE": "SE", "TOCANTINS": "TO"
}

REGIOES_BRASIL = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"], "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"], "Sudeste": ["ES", "MG", "RJ", "SP"], "Sul": ["PR", "RS", "SC"]
}

# Dict de lookup invertido UF→Região (O(1)) — usado no mapeamento vetorizado
_UF_PARA_REGIAO = {uf: regiao for regiao, ufs in REGIOES_BRASIL.items() for uf in ufs}

# Regex de UF pré-compilada (antes recompilada a cada chamada de extrair_uf_precisa)
_RE_UF_SIGLA = re.compile(r'\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b')

@_lru_cache(maxsize=8192)
def extrair_uf_precisa(endereco):
    """[PERF-2] Extrai a UF de um endereço textual. Cacheada (lru) pois endereços
    se repetem muito em lotes B2B. Regex pré-compilada no módulo. Lógica idêntica."""
    if not isinstance(endereco, str):
        return "Indefinido"
    end_upper = unidecode(endereco.upper())
    for nome, sigla in MAPA_ESTADOS_FULL.items():
        if f" {nome} " in f" {end_upper} " or end_upper.endswith(nome) or f", {nome}," in end_upper:
            return sigla
    partes = [p.strip() for p in end_upper.split(',')]
    for p in reversed(partes):
        match = _RE_UF_SIGLA.search(p)
        if match:
            return match.group(1)
    return "Indefinido"


# ==============================================================================
# ENGINE DE RESOLUÇÃO UNIVERSAL E ENDEREÇAMENTO CANÔNICO
# ==============================================================================
class ParserGeograficoBR:
    _CEP_REGEX = re.compile(r'\b\d{5}-?\d{3}\b')
    _NUM_REGEX = re.compile(r'\b(?:N|NO|NUMERO|NUM)?\s*(\d{1,5})\b', re.IGNORECASE)
    _COMP_REGEX = re.compile(r'\b(BLOCO|BL|APTO|APT|APARTAMENTO|SALASL|SALA|CONJUNTO|CJ|CASA|LOJA|PAVIMENTO)\s*([A-Z0-9]+)\b', re.IGNORECASE)
    # [PERF-Q3 - 11ª geração] Memo thread-safe de extrair_componentes. Esta staticmethod
    # pura faz 3 buscas de regex e é chamada até 3× sobre o MESMO texto_norm no caminho
    # de geocodificação (construir_endereco_canonico, consenso, geo core). Depende só do
    # texto + regexes fixas → determinística. Retornamos cópia (callers só leem, mas a
    # cópia blinda contra mutação futura). Bounded 50k.
    _memo_comp = {}
    _memo_comp_lock = threading.Lock()

    @staticmethod
    def extrair_componentes(texto):
        cached = ParserGeograficoBR._memo_comp.get(texto)
        if cached is not None:
            return dict(cached)
        componentes = {"cep": "", "numero": "", "complemento": "", "resto": texto}
        cep_match = ParserGeograficoBR._CEP_REGEX.search(componentes["resto"])
        if cep_match:
            componentes["cep"] = cep_match.group(0).replace("-", "")
            componentes["resto"] = componentes["resto"].replace(cep_match.group(0), "").strip(" ,-")
            
        num_match = ParserGeograficoBR._NUM_REGEX.search(componentes["resto"])
        if num_match: 
            componentes["numero"] = num_match.group(1)
            
        comp_match = ParserGeograficoBR._COMP_REGEX.search(componentes["resto"])
        if comp_match: 
            componentes["complemento"] = f"{comp_match.group(1)} {comp_match.group(2)}"
            
        with ParserGeograficoBR._memo_comp_lock:
            if len(ParserGeograficoBR._memo_comp) >= 50000:
                ParserGeograficoBR._memo_comp.clear()
            ParserGeograficoBR._memo_comp[texto] = dict(componentes)
        return componentes

class MotorEnderecoCanônico:
    def __init__(self):
        self.rural_keys = ["FAZENDA", "SITIO", "ASSENTAMENTO", "CHACARA", "GLEBA", "NUCLEO RURAL"]
        self.bairro_keys = ["BAIRRO", "VILA", "JARDIM", "PARQUE", "RESIDENCIAL", "SETOR", "ASA SUL", "ASA NORTE", "LAGO SUL", "LAGO NORTE"]
        self.condo_keys = [re.compile(r"\bCONDOMINIO\b"), re.compile(r"\bCOND\."), re.compile(r"\bRESIDENCIAL\b"), re.compile(r"\bRES\."), re.compile(r"\bLOTEAMENTO\b")]
        self.via_keys = [
            "RUA", "AVENIDA", "TRAVESSA", "ALAMEDA", "RODOVIA", "ESTRADA", "QUADRA", 
            "SQN", "SQS", "SHIS", "SHIN", "SCRN", "SCS", "SRTVN", "CLS", "CLN",
            "QNL", "QNM", "QNN", "QNG", "QNJ", "QNK", "QI", "QE", "QC", "QR", "QS", "QSC"
        ]
        abreviacoes_raw = {
            r'\bAV\b': 'AVENIDA', r'\bR\b': 'RUA', r'\bQD\b': 'QUADRA', r'\bLT\b': 'LOTE',
            r'\bCJ\b': 'CONJUNTO', r'\bCONJ\b': 'CONJUNTO', r'\bBL\b': 'BLOCO', r'\bAPT\b': 'APARTAMENTO',
            r'\bST\b': 'SETOR', r'\bCH\b': 'CHACARA', r'\bROD\b': 'RODOVIA', r'\bKM\b': 'QUILOMETRO', 
            r'\bAL\b': 'ALAMEDA', r'\bTR\b': 'TRAVESSA', r'\bTV\b': 'TRAVESSA', 
            r'\bPCA\b': 'PRACA', r'\bPQ\b': 'PARQUE', r'\bSQN\b': 'SUPERQUADRA NORTE', 
            r'\bSQS\b': 'SUPERQUADRA SUL', r'\bCLN\b': 'COMERCIO LOCAL NORTE', r'\bCLS\b': 'COMERCIO LOCAL SUL'
        }
        self.abreviacoes = {re.compile(k): v for k, v in abreviacoes_raw.items()}
        self.padrao_rodovia = re.compile(r'\b(BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\s*[-]?\s*(\d+)(?:\s*(?:KM|QUILOMETRO)\s*(\d+))?\b')
        self.sinonimos = {re.compile(r'\b' + k + r'\b'): v for k, v in SINONIMOS_SEMANTICOS.items()}
        self.zeros_regex = re.compile(r'\b0+(\d{1,4})\b')
        self.invalid_chars_regex = re.compile(r'[\x00-\x1F\x7F-\x9F]')
        # [SPEED-4 - 10ª geração] Memoização thread-safe de normalizar(). Esta função
        # faz trabalho pesado de regex (unidecode + várias substituições + 2 loops sobre
        # abreviações/sinônimos) e é chamada repetidamente sobre as MESMAS strings: no
        # loop de prioridade, na chave de cache de rota (2× por rota) e na geocodificação.
        # cache_aprendizado é somente-leitura em execução e as regras são fixas, logo a
        # saída é determinística por entrada → memoização é 100% segura (zero regressão).
        self._memo_norm = {}
        self._memo_norm_lock = threading.Lock()
        # [PERF-Q1] Memo de resolver_contexto_administrativo (pura sobre texto_norm)
        self._memo_ctx = {}
        self._memo_ctx_lock = threading.Lock()

    def normalizar(self, texto):
        if not texto or pd.isna(texto):
            return ""
        # Fast-path: consulta memo antes de qualquer processamento pesado
        _memo_key = str(texto).strip().upper()
        if _memo_key:
            cached = self._memo_norm.get(_memo_key)
            if cached is not None:
                return cached
        resultado = self._normalizar_impl(texto)
        # Armazena no memo (bounded: limpa se exceder 50k entradas para evitar crescimento ilimitado)
        if _memo_key:
            with self._memo_norm_lock:
                if len(self._memo_norm) >= 50000:
                    self._memo_norm.clear()
                self._memo_norm[_memo_key] = resultado
        return resultado

    def _normalizar_impl(self, texto):
        if not texto or pd.isna(texto): 
            return ""
        t_raw = str(texto).strip()
        chave_aprendizado = t_raw.upper()
        if chave_aprendizado in cache_aprendizado:
            dado_salvo = cache_aprendizado[chave_aprendizado]
            if isinstance(dado_salvo, str): 
                t_raw = dado_salvo
        t_raw = t_raw.replace(',', ' ').replace(';', ' ')
        t = self.invalid_chars_regex.sub('', t_raw)
        t = unidecode(t).upper()
        t = self.zeros_regex.sub(r'\1', t)
        
        def padronizar_rodovia(match):
            sigla = match.group(1)
            numero = match.group(2).zfill(3)
            km_str = f" KM {match.group(3)}" if match.group(3) else ""
            return f"{sigla}-{numero}{km_str}"
            
        t = self.padrao_rodovia.sub(padronizar_rodovia, t)
        for padrao, expansao in self.abreviacoes.items(): 
            t = padrao.sub(expansao, t)
        for padrao, expansao in self.sinonimos.items(): 
            t = padrao.sub(expansao, t)
        return re.sub(r'\s+', ' ', t).strip()

    def classificar_entrada(self, texto_norm):
        if texto_norm in cache_classificacao: 
            return cache_classificacao[texto_norm]
        tipo = "LOGRADOURO"
        ctx_temp = self.resolver_contexto_administrativo(texto_norm)
        mun_temp = ctx_temp.get("municipio", "")
        uf_temp = ctx_temp.get("uf", "")
        
        texto_limpo_mun = _regex_palavra(uf_temp).sub('', texto_norm).strip() if uf_temp else texto_norm
        texto_limpo_mun = texto_limpo_mun.replace("BRASIL", "").strip()
        
        if ParserGeograficoBR._CEP_REGEX.search(texto_norm): 
            tipo = "CEP"
        elif any(p.search(texto_norm) for p in self.condo_keys): 
            tipo = "CONDOMINIO"
        elif any(k in texto_norm for k in POI_KEYWORDS): 
            tipo = "POI"
        elif any(k in texto_norm for k in self.rural_keys): 
            tipo = "RURAL"
        elif any(k in texto_norm for k in self.via_keys) and bool(re.search(r'\d+', texto_norm)): 
            tipo = "ENDERECO_COMPLETO"
        elif any(k in texto_norm for k in self.bairro_keys): 
            tipo = "BAIRRO"
        elif mun_temp and (
            texto_limpo_mun == mun_temp or texto_norm == mun_temp or
            texto_norm == f"{mun_temp} {uf_temp}".strip() or
            # [FIX-MUN-CLASS - 31ª geração] CAUSA RAIZ do POI ("Corumbá" virava hotel/rua):
            # o usuário digita a forma CURTA ("Corumbá") mas o nome oficial IBGE é mais longo
            # ("Corumbá de Goiás"). O resolver_contexto (FIX-GEO4) já resolve o município
            # corretamente, MAS esta classificação exigia igualdade textual exata e falhava →
            # a entrada caía em "LOGRADOURO" e batia nas APIs, que devolvem POIs (hotéis, ruas)
            # dentro da cidade em vez do CENTRÓIDE. Aqui aceitamos como MUNICIPIO quando o termo
            # do usuário (sem a UF) é PREFIXO do nome oficial, ou quando todos os seus tokens
            # pertencem ao nome oficial — sinal inequívoco de que se quis a CIDADE, não um POI.
            # Seguro: POI/CEP/condomínio/rural/endereço-com-número/bairro são testados ANTES,
            # então um endereço real nunca chega aqui; e exige-se que o município já tenha sido
            # resolvido (mun_temp) e que o termo tenha ≥3 chars (evita fragmentos triviais).
            (texto_limpo_mun and len(texto_limpo_mun) >= 3 and (
                mun_temp.startswith(texto_limpo_mun + " ") or
                set(texto_limpo_mun.split()).issubset(set(mun_temp.split()))
            ))
        ): 
            tipo = "MUNICIPIO"
        elif texto_norm in IBGE_MUNICIPIOS: 
            tipo = "MUNICIPIO"
        elif texto_norm in IBGE_DISTRITOS: 
            tipo = "DISTRITO"
            
        cache_classificacao.set(texto_norm, tipo, expire=2592000)
        return tipo

    def aplicar_fuzzy_multidimensional(self, texto_norm):
        if texto_norm in cache_fuzzy: 
            return cache_fuzzy[texto_norm]
        tokens = texto_norm.split()
        for token in tokens:
            if len(token) >= 5 and token not in IBGE_MUNICIPIOS and token not in IBGE_DISTRITOS:
                top_matches = process.extract(token, LISTA_CONTEXTO_FUZZY, scorer=fuzz.WRatio, limit=5, processor=None)
                if top_matches and top_matches[0][1] >= 85:
                    melhor_match = max(top_matches, key=lambda m: fuzz.token_set_ratio(texto_norm, m[0], processor=None))
                    ts_ratio = fuzz.token_set_ratio(texto_norm, melhor_match[0], processor=None)
                    if melhor_match[1] >= 85 and ts_ratio >= 90:
                        cidade_corrigida = melhor_match[0].rsplit(' ', 1)[0]
                        texto_norm = texto_norm.replace(token, cidade_corrigida)
                        # [M19] Early-exit: score >= 95 indica correspondência excelente
                        # Não faz sentido continuar buscando para outros tokens
                        if ts_ratio >= 95:
                            break
        cache_fuzzy.set(texto_norm, texto_norm, expire=2592000)
        return texto_norm

    def resolver_contexto_administrativo(self, texto_norm):
        # [PERF-Q1 - 11ª geração] Memoização thread-safe. Esta função é chamada
        # repetidamente sobre o mesmo texto_norm (em classificar_entrada, no consenso
        # Bayesiano e na geocodificação) e faz trabalho caro: 2 loops de regex sobre
        # as 27 UFs + geração de n-gramas + até 2 buscas fuzzy (process.extractOne)
        # sobre listas de milhares de cidades. Depende apenas de texto_norm e de dados
        # IBGE estáticos → pura e determinística. Memoizar é seguro (zero regressão).
        # Retornamos uma CÓPIA para que o .update() do chamador não corrompa o cache.
        if texto_norm in self._memo_ctx:
            return dict(self._memo_ctx[texto_norm])
        resultado = self._resolver_contexto_administrativo_impl(texto_norm)
        with self._memo_ctx_lock:
            if len(self._memo_ctx) >= 50000:
                self._memo_ctx.clear()
            self._memo_ctx[texto_norm] = dict(resultado)
        return resultado

    def _resolver_contexto_administrativo_impl(self, texto_norm):
        uf_explicita = None
        for sigla in IBGE_ESTADOS.keys():
            if re.search(rf'\b{sigla}\b', texto_norm):
                uf_explicita = sigla
                break
        if not uf_explicita:
            for sigla, nome in IBGE_ESTADOS.items():
                if re.search(rf'\b{nome}\b', texto_norm):
                    uf_explicita = sigla
                    break
                    
        resultado = {"uf": uf_explicita if uf_explicita else "", "municipio": "", "distrito": ""}
        cidades_para_busca = IBGE_MUNICIPIOS
        
        if uf_explicita:
            if uf_explicita in IBGE_MUNICIPIOS_POR_UF:
                cidades_para_busca = IBGE_MUNICIPIOS_POR_UF[uf_explicita]
            else:
                cidades_para_busca = {}
                
        tokens = texto_norm.split()
        # [M18] N-gramas limitados a max 6 tokens — maior cidade BR ("Santa Rita do Passa
        # Quatro", "São José do Rio Preto") tem 6 palavras. Limite preserva 100% da precisão
        # de detecção (Etapa 3 > Etapa 2) e ainda elimina O(n²) em logradouros longos (10+ tokens).
        max_ngram = min(6, len(tokens))
        for i in range(len(tokens)):
            for j in range(i + 1, min(i + max_ngram + 1, len(tokens) + 1)):
                chunk = " ".join(tokens[i:j])
                if chunk in cidades_para_busca:
                    resultado.update({"uf": cidades_para_busca[chunk][0]["uf"], "municipio": chunk})
                    return resultado
                    
        if uf_explicita and not resultado["municipio"]:
            chaves = list(cidades_para_busca.keys())
            if chaves:
                # [FIX-GEO4 - 16ª geração] Resolução robusta de nome curto dentro da UF.
                # CAUSA RAIZ do bug Corumbá: o usuário digita a forma curta ("Corumbá, GO")
                # mas o nome oficial IBGE é "Corumbá de Goiás". O match exato falha e o
                # fuzzy sobre o texto inteiro (com a sigla "GO") podia não bater. Aqui,
                # DENTRO da UF informada (busca segura, não cria ambiguidade entre estados),
                # tentamos: (1) cidade cujo nome COMEÇA com o termo do usuário (prefixo),
                # (2) cidade que CONTÉM o termo, e por fim (3) o fuzzy original. Removemos a
                # sigla da UF do texto antes de comparar, isolando o nome da localidade.
                texto_sem_uf = _regex_palavra(uf_explicita).sub('', texto_norm)
                texto_sem_uf = texto_sem_uf.replace("BRASIL", "").strip()
                termo = re.sub(r'\s+', ' ', texto_sem_uf).strip()
                
                if termo and len(termo) >= 3:
                    # (1) Prefixo: "CORUMBA" → "CORUMBA DE GOIAS". Só aceita se houver
                    # um ÚNICO candidato por prefixo (evita ambiguidade silenciosa).
                    candidatos_prefixo = [c for c in chaves if c.startswith(termo + " ") or c == termo]
                    if len(candidatos_prefixo) == 1:
                        resultado.update({"municipio": candidatos_prefixo[0]})
                        return resultado
                    # (2) Se o termo é exatamente uma cidade da UF (match direto pós-limpeza)
                    if termo in cidades_para_busca:
                        resultado.update({"municipio": termo})
                        return resultado
                    # (3) Contém: termo aparece como palavra inicial de exatamente uma cidade
                    candidatos_contem = [c for c in chaves if termo in c.split(" ")[0:1] or c.split(" ")[0] == termo]
                    if len(candidatos_contem) == 1:
                        resultado.update({"municipio": candidatos_contem[0]})
                        return resultado
                        
                # (4) Fuzzy original sobre o texto completo (rede de segurança)
                melhor_match = process.extractOne(texto_norm, chaves, scorer=fuzz.token_set_ratio, processor=None)
                if melhor_match and melhor_match[1] >= 65:
                    resultado.update({"municipio": melhor_match[0]})
                    return resultado
                # (5) Fuzzy adicional só sobre o termo limpo (sem a UF), com limiar mais alto
                if termo and len(termo) >= 4:
                    melhor_termo = process.extractOne(termo, chaves, scorer=fuzz.WRatio, processor=None)
                    if melhor_termo and melhor_termo[1] >= 88:
                        resultado.update({"municipio": melhor_termo[0]})
                        return resultado
                    
        if not resultado["municipio"] and not uf_explicita and len(texto_norm) > 4:
            melhor_match_global = process.extractOne(texto_norm, LISTA_CONTEXTO_FUZZY, scorer=fuzz.WRatio, processor=None)
            if melhor_match_global and melhor_match_global[1] >= 85:
                cidade_uf = melhor_match_global[0]
                resultado.update({"uf": cidade_uf.rsplit(' ', 1)[1], "municipio": cidade_uf.rsplit(' ', 1)[0]})
                
        return resultado

    def construir_endereco_canonico(self, texto_cru):
        texto_norm = self.normalizar(texto_cru)
        parsed = ParserGeograficoBR.extrair_componentes(texto_norm)
        
        if parsed["cep"]:
            logr, bair, loca, uf, lat_cep, lon_cep = cascata_postal_tripla(parsed["cep"])
            if loca:
                num_str = f", {parsed['numero']}" if parsed["numero"] else ""
                comp_str = f", {parsed['complemento']}" if parsed["complemento"] else ""
                if parsed["numero"] or parsed["complemento"]: 
                    lat_cep, lon_cep = 0.0, 0.0
                nome_estado_cep = IBGE_ESTADOS.get(uf, uf) if uf else ""
                return f"{logr}{num_str}{comp_str}, {bair}, {loca}, {nome_estado_cep}, BRASIL", "CEP", parsed["cep"], lat_cep, lon_cep
                
        contexto_pre = self.resolver_contexto_administrativo(texto_norm)
        if not contexto_pre.get("municipio"):
            texto_fuzzy = self.aplicar_fuzzy_multidimensional(texto_norm)
            contexto = self.resolver_contexto_administrativo(texto_fuzzy)
        else:
            texto_fuzzy = texto_norm
            contexto = contexto_pre
            
        tipo = self.classificar_entrada(texto_fuzzy)
        endereco_canonico = texto_fuzzy if texto_fuzzy else texto_norm
        return endereco_canonico, tipo, "", 0.0, 0.0

semantica = MotorEnderecoCanônico()

@_lru_cache(maxsize=64)
def _normalizar_uf(uf: str) -> str:
    """Normaliza nome de UF com cache (27 UFs + variações — custo único)."""
    return unidecode(IBGE_ESTADOS.get(uf, uf)).upper()

@_lru_cache(maxsize=512)
def _regex_palavra(termo: str):
    """Retorna re.Pattern compilado e cacheado para evitar recompilação por endereço."""
    return re.compile(rf"\b{re.escape(termo)}\b", re.IGNORECASE)

# ==============================================================================
# VALIDADOR PRÉ-GEOCODING E LÓGICA GEODÉSICA CORPORATIVA (MULTI-CAMADA)
# ==============================================================================

@_lru_cache(maxsize=256)
def parse_tempo_minutos(t_str):
    if not isinstance(t_str, str): 
        return 999999
    try:
        h = re.search(r'(\d+)\s*h', t_str)
        m = re.search(r'(\d+)\s*min', t_str)
        horas = int(h.group(1)) if h else 0
        mins = int(m.group(1)) if m else 0
        
        if not h and not m:
            nums = re.findall(r'\d+', t_str)
            if nums: 
                return int(nums[0])
            return 999999
        return horas * 60 + mins
    except Exception:
        return 999999

def validar_coordenada_brasil(lat, lon):
    try:
        lat_f, lon_f = float(lat), float(lon)
        if (-35.0 <= lat_f <= 6.0) and (-75.0 <= lon_f <= -28.0):
            return True, lat_f, lon_f
        if (-35.0 <= lon_f <= 6.0) and (-75.0 <= lat_f <= -28.0):
            return True, lon_f, lat_f 
        return False, lat_f, lon_f
    except (ValueError, TypeError):
        return False, 0.0, 0.0

def calcular_distancia_linha_reta(lat1, lon1, lat2, lon2, contexto=""):
    global METRICAS_DISTANCIA
    _incrementar_metrica("total_calculos")
    try:
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        dist_final = 0.0
        status_final = ""
        
        if lat1 == 0.0 or lon1 == 0.0 or lat2 == 0.0 or lon2 == 0.0:
             return 0.0, "Falha Operacional (Coordenadas Ausentes)"
             
        if lat1 == lat2 and lon1 == lon2:
             return 0.0, "Calculada Normalmente (Pontos Coincidentes)"
             
        calculado_sucesso = False
        if GEOGRAPHICLIB_DISPONIVEL:
            try:
                # [G23 - 2ª geração] GeographicLib (algoritmo de Karney) = padrão-ouro WGS-84.
                # Precisão de ~15 nanômetros — exatidão geodésica máxima tecnicamente possível.
                dist_metros = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2)['s12']
                dist_km = dist_metros / 1000.0
                if dist_km > 0:
                    _incrementar_metrica("sucesso_geographiclib")
                    dist_final, status_final = round(dist_km, 3), "Calculada via GeographicLib WGS-84 (Karney, erro <1mm)"
                    calculado_sucesso = True
            except Exception as e:
                logger.warning(f"GeographicLib falhou: {e}")
                
        if not calculado_sucesso and GEOPY_DISPONIVEL:
            try:
                dist_km = geodesic((lat1, lon1), (lat2, lon2)).km
                if dist_km > 0:
                    _incrementar_metrica("sucesso_geopy")
                    dist_final, status_final = round(dist_km, 3), "Calculada via Geopy Geodesic (elipsoide WGS-84)"
                    calculado_sucesso = True
            except Exception as e:
                logger.warning(f"Geopy falhou: {e}")
                
        if not calculado_sucesso:
            lat1_r, lon1_r, lat2_r, lon2_r = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2_r - lat1_r
            dlon = lon2_r - lon1_r
            a = math.sin(dlat / 2)**2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2)**2
            c = 2 * math.asin(math.sqrt(a))
            # [G23 - 2ª geração] Raio médio autálico IUGG (6371.0088 km) em vez de 6371.0.
            # Reduz o erro sistemático do Haversine esférico em ~8,8m por 1000km.
            dist_haversine = 6371.0088 * c
            
            if dist_haversine >= 0.01:
                _incrementar_metrica("fallback_haversine")
                dist_final, status_final = round(dist_haversine, 3), "Calculada via Fallback Haversine (esfera IUGG, erro ~0.5%)"
            else:
                logger.error(f"FALHA CRÍTICA PREVENIDA: Distância zerada para pontos diferentes. {lat1},{lon1} a {lat2},{lon2} | Ctx: {contexto}")
                _incrementar_metrica("correcoes_automaticas")
                dist_final, status_final = 0.01, "Calculada após reprocessamento (Correção Anti-Zero)"
                
        if dist_final > 5000.0:
            logger.error(f"ANOMALIA TERRITORIAL: Distância de {dist_final}km excede fisicamente os limites do Brasil. Ctx: {contexto}")
            _incrementar_metrica("barreira_territorial")
            return 0.01, "Falha de Bounding Box (Distância Transcontinental Impossível)"
            
        return dist_final, status_final
    except Exception as e:
        logger.error(f"Erro fatal no motor de distância geodésica ({contexto}): {e}")
        _incrementar_metrica("falhas_criticas")
        return 0.0, "Falha Operacional Crítica no Motor Geodésico"

def _distancia_consenso_km(lat1, lon1, lat2, lon2):
    """[PERF-Q2 - 11ª geração] Distância geodésica para comparações INTERNAS do
    consenso Bayesiano (loop O(n²) entre candidatos de API). Usa EXATAMENTE a mesma
    matemática de calcular_distancia_linha_reta (GeographicLib Karney → Geopy →
    Haversine IUGG), mas SEM incrementar os contadores globais de telemetria.

    Dois benefícios líquidos, zero perda:
    1) PERFORMANCE: elimina a contenção do _LOCK_METRICAS no loop O(n²) executado por
       múltiplas threads em paralelo (cada chamada antiga pegava o lock 1-2×).
    2) QUALIDADE/AUDITORIA: METRICAS_DISTANCIA passa a refletir apenas distâncias de
       ROTAS reais, não comparações internas de consenso — métricas de auditoria mais
       fiéis (ex: 'total_calculos' deixa de ser inflado por uso interno).
    O valor numérico retornado é idêntico ao da função pública para os mesmos pontos.
    """
    try:
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        if lat1 == 0.0 or lon1 == 0.0 or lat2 == 0.0 or lon2 == 0.0:
            return 0.0
        if lat1 == lat2 and lon1 == lon2:
            return 0.0
        if GEOGRAPHICLIB_DISPONIVEL:
            try:
                dist_km = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2)['s12'] / 1000.0
                if dist_km > 0:
                    return round(dist_km, 3)
            except Exception:
                pass
        if GEOPY_DISPONIVEL:
            try:
                dist_km = geodesic((lat1, lon1), (lat2, lon2)).km
                if dist_km > 0:
                    return round(dist_km, 3)
            except Exception:
                pass
        lat1_r, lon1_r, lat2_r, lon2_r = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2_r - lat1_r
        dlon = lon2_r - lon1_r
        a = math.sin(dlat / 2)**2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2)**2
        c = 2 * math.asin(math.sqrt(a))
        return round(6371.0088 * c, 3)
    except Exception:
        return 0.0

def cascata_postal_tripla(cep_limpo):
    if cep_limpo in cache_cep:
        d = cache_cep[cep_limpo]
        if len(d) == 4: 
            return d[0], d[1], d[2], d[3], 0.0, 0.0
        return d
        
    lat, lon = 0.0, 0.0
    try:
        r = session.get(f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}", timeout=4).json()
        if "city" in r:
            loc = r.get("location", {}).get("coordinates", {})
            if loc and "latitude" in loc and "longitude" in loc:
                try: 
                    lat, lon = float(loc["latitude"]), float(loc["longitude"])
                except (ValueError, TypeError): 
                    pass
            d = (r.get('street', ''), r.get('neighborhood', ''), r.get('city', ''), r.get('state', ''), lat, lon)
            cache_cep.set(cep_limpo, d, expire=2592000)
            return d
    except Exception: 
        pass
        
    try:
        def _nom_cep():
            _throttle_nominatim()
            url = f"https://nominatim.openstreetmap.org/search?format=json&postalcode={cep_limpo}&countrycodes=br&limit=1"
            return session.get(url, headers={"User-Agent": "RotasEnterprise/8.0"}, timeout=4).json()
        r_nom = FILA_NOMINATIM.submit(_nom_cep).result()
        if r_nom: 
            lat, lon = float(r_nom[0]['lat']), float(r_nom[0]['lon'])
    except Exception: 
        pass
        
    try:
        r = session.get(f"https://viacep.com.br/ws/{cep_limpo}/json/", timeout=4).json()
        if "erro" not in r:
            d = (r.get('logradouro', ''), r.get('bairro', ''), r.get('localidade', ''), r.get('uf', ''), lat, lon)
            cache_cep.set(cep_limpo, d, expire=2592000)
            return d
    except Exception: 
        pass
        
    try:
        r = session.get(f"https://opencep.com/v1/{cep_limpo}", timeout=4).json()
        if "error" not in r:
            d = (r.get('logradouro', ''), r.get('bairro', ''), r.get('localidade', ''), r.get('uf', ''), lat, lon)
            cache_cep.set(cep_limpo, d, expire=2592000)
            return d
    except Exception: 
        pass
        
    return "", "", "", "", 0.0, 0.0

def validar_consistencia_administrativa(candidato, uf_inf):
    est_api = unidecode(candidato.get('estado', '')).upper().strip()
    if uf_inf and est_api:
        nome_estado_inf = _normalizar_uf(uf_inf) if uf_inf else ""
        if uf_inf not in est_api and nome_estado_inf not in est_api:
            return False
    return True

def validar_consistencia_municipal(candidato, mun_inf):
    if not mun_inf: 
        return True
    cid_api = unidecode(candidato.get('cidade', '')).upper().strip()
    if not cid_api: 
        return True
    if mun_inf == cid_api or mun_inf in cid_api or cid_api in mun_inf: 
        return True
    if fuzz.token_set_ratio(mun_inf, cid_api, processor=None) >= 95: 
        return True
    return False

def obter_coordenada_centroide_supremo(mun_nome, uf_nome):
    url_arc = f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?City={requests.utils.quote(mun_nome)}&Region={requests.utils.quote(uf_nome)}&CountryCode=BRA&f=json&maxLocations=1"
    try:
        r = session.get(url_arc, timeout=5).json()
        if r.get('candidates'):
            cand = r['candidates'][0]
            lat_c, lon_c = float(cand['location']['y']), float(cand['location']['x'])
            if validar_coordenada_brasil(lat_c, lon_c)[0]: 
                return lat_c, lon_c, "ARCGIS_CENTROIDE_SUPREMO"
    except: 
        pass
        
    url_nom = f"https://nominatim.openstreetmap.org/search?city={requests.utils.quote(mun_nome)}&state={requests.utils.quote(uf_nome)}&country=Brazil&format=json&limit=1"
    try:
        r = session.get(url_nom, headers={"User-Agent": "RotasCorp/11.0"}, timeout=5).json()
        if r:
            lat_c, lon_c = float(r[0]['lat']), float(r[0]['lon'])
            if validar_coordenada_brasil(lat_c, lon_c)[0]: 
                return lat_c, lon_c, "NOMINATIM_CENTROIDE_SUPREMO"
    except: 
        pass
        
    return 0.0, 0.0, None

# ─────────────────────────────────────────────────────────────────────────────
# [BASE-IBGE-CENTROIDE] Resolvedor unificado de centróide municipal + código IBGE
# A API /localidades/municipios do IBGE NÃO traz lat/lon (só o código `id`); por
# isso o centróide é resolvido por cidade+UF via ArcGIS/Nominatim (que devolvem o
# CENTRO da cidade, jamais um POI) e memorizado em RAM. Se um dia a base offline
# passar a ter lat≠0 (ex.: malha IBGE), ela é usada com prioridade. Esta função
# rearma, em produção, o atalho municipal e a blindagem anti-alucinação — que antes
# dependiam de um lat IBGE que nunca existia (ficavam, na prática, desligados).
# ─────────────────────────────────────────────────────────────────────────────
_CENTROIDE_MUN_CACHE = {}

def _info_municipio_ibge(mun_nome, uf_nome):
    """Retorna (item_da_base | None, codigo_ibge | None) do município na UF informada."""
    if mun_nome in IBGE_MUNICIPIOS:
        for item in IBGE_MUNICIPIOS[mun_nome]:
            if item.get("uf") == uf_nome:
                return item, item.get("codigo_ibge")
    return None, None

def _centroide_municipio(mun_nome, uf_nome):
    """Centróide oficial do município (lat, lon). Prioriza lat/lon do IBGE offline
    quando existir (>0); senão resolve por cidade+UF (centro da cidade) e memoriza.
    Retorna (0.0, 0.0) apenas se nenhuma fonte responder — preservando o fall-through."""
    chave = (mun_nome, uf_nome)
    if chave in _CENTROIDE_MUN_CACHE:
        return _CENTROIDE_MUN_CACHE[chave]
    item, _cod = _info_municipio_ibge(mun_nome, uf_nome)
    if item and item.get("lat", 0.0) != 0.0 and item.get("lon", 0.0) != 0.0:
        par = (item["lat"], item["lon"])
        _CENTROIDE_MUN_CACHE[chave] = par
        return par
    lat_c, lon_c, _fonte = obter_coordenada_centroide_supremo(mun_nome, uf_nome)
    par = (lat_c, lon_c) if (lat_c != 0.0 and lon_c != 0.0) else (0.0, 0.0)
    _CENTROIDE_MUN_CACHE[chave] = par
    return par

def obedience_base_local(contexto_estruturado):
    if contexto_estruturado["logradouro"] and contexto_estruturado["municipio"] and contexto_estruturado["uf"]:
        chave_cnefe = f"{contexto_estruturado['logradouro']}_{contexto_estruturado['municipio']}_{contexto_estruturado['uf']}"
        if chave_cnefe in cache_base_local:
            return cache_base_local[chave_cnefe]
    return None

# ==============================================================================
# MÓDULOS DE GEOCODIFICAÇÃO COM TELEMETRIA E MOTOR ANTI-COLISÃO
# ==============================================================================
def API_TomTom(query):
    if not TOMTOM_API_KEY: 
        return None
    start_t = time.time()
    try:
        url = f"https://api.tomtom.com/search/2/geocode/{requests.utils.quote(query)}.json?key={TOMTOM_API_KEY}&countrySet=BR&limit=5"
        r = session.get(url, timeout=4).json()
        resultados = []
        if r.get("results"):
            for res in r["results"][:5]:
                pos = res.get("position", {})
                addr = res.get("address", {})
                resultados.append({
                    "lat": float(pos["lat"]), "lon": float(pos["lon"]), "fonte": "TOMTOM", "score_base": 35,
                    "cidade": addr.get("municipality", "").upper(), "estado": addr.get("countrySubdivision", "").upper(),
                    "bairro": addr.get("neighbourhood", addr.get("subdivision", "")).upper(), "logradouro": addr.get("streetName", "").upper(),
                    "numero": str(addr.get("streetNumber", "")).upper(), "cep": addr.get("postalCode", "").replace("-", "")
                })
            registrar_telemetria("TOMTOM", True, time.time() - start_t)
        return resultados if resultados else None
    except Exception: 
        pass
    registrar_telemetria("TOMTOM", False, time.time() - start_t)
    return None

def executar_reverse_geocoding_multimotor(lat, lon):
    rev_key = f"V5_{round(lat,5)}|{round(lon,5)}"
    if rev_key in cache_reverse: 
        return cache_reverse[rev_key]
        
    res = {"logradouro": "", "bairro": "", "cidade": "", "municipio": "", "distrito": "", "estado": "", "cep": ""}
    try:
        def _nom_rev():
            _throttle_nominatim()
            url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&addressdetails=1"
            return session.get(url, headers={"User-Agent": "RotasEnterprise/8.0"}, timeout=4).json()
        r_nom = FILA_NOMINATIM.submit(_nom_rev).result().get("address", {})
        res.update({
            "logradouro": r_nom.get("road", r_nom.get("pedestrian", "")), 
            "bairro": r_nom.get("neighbourhood", r_nom.get("suburb", r_nom.get("city_district", ""))), 
            "cidade": r_nom.get("city", r_nom.get("town", r_nom.get("municipality", ""))), 
            "estado": r_nom.get("state", "").upper(), 
            "cep": r_nom.get("postcode", "")
        })
        cache_reverse.set(rev_key, res, expire=2592000)
        return res
    except Exception: 
        pass
        
    try:
        url_arc = f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lon},{lat}&f=json"
        r_arc = session.get(url_arc, timeout=4).json()
        if 'address' in r_arc:
            addr = r_arc['address']
            res.update({
                "logradouro": addr.get('Address', ''), 
                "bairro": addr.get('Neighborhood', ''), 
                "cidade": addr.get('City', ''), 
                "estado": addr.get('RegionAbbr', '').upper(), 
                "cep": addr.get('Postal', '')
            })
            cache_reverse.set(rev_key, res, expire=2592000)
    except Exception: 
        pass
        
    return res

def API_ArcGIS(query, ctx=None):
    start_t = time.time()
    try:
        if ctx and (ctx.get("logradouro") or ctx.get("municipio")):
            end = requests.utils.quote(ctx.get("logradouro", ""))
            cid = requests.utils.quote(ctx.get("municipio", ""))
            uf = requests.utils.quote(ctx.get("uf", ""))
            bair = requests.utils.quote(ctx.get("bairro", ""))
            cep = requests.utils.quote(ctx.get("cep", ""))
            url = f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&Address={end}&Neighborhood={bair}&City={cid}&Region={uf}&Postal={cep}&maxLocations=5&sourceCountry=BRA&outFields=*"
        else:
            url = f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&singleLine={requests.utils.quote(query)}&maxLocations=5&sourceCountry=BRA&outFields=*"
            
        r = session.get(url, timeout=4).json()
        resultados = []
        if r.get('candidates'):
            for c in r['candidates'][:5]:
                attr = c.get('attributes', {})
                resultados.append({
                    "lat": float(c['location']['y']), "lon": float(c['location']['x']), "fonte": "ARCGIS", "score_base": 30, 
                    "cidade": attr.get('City', '').upper(), "estado": attr.get('RegionAbbr', '').upper(), 
                    "bairro": attr.get('Neighborhood', '').upper(), "logradouro": attr.get('StName', attr.get('Address', '')).upper(), 
                    "numero": str(attr.get('AddNum', '')).upper(), "cep": attr.get('Postal', '')
                })
            registrar_telemetria("ARCGIS", True, time.time() - start_t)
        return resultados if resultados else None
    except Exception: 
        pass
    registrar_telemetria("ARCGIS", False, time.time() - start_t)
    return None

def API_Nominatim(query, ctx=None):
    start_t = time.time()
    try:
        def _call_nom():
            _throttle_nominatim()
            if ctx and ctx.get("logradouro") and ctx.get("municipio"):
                rua = requests.utils.quote(ctx["logradouro"])
                cid = requests.utils.quote(ctx["municipio"])
                est = requests.utils.quote(ctx.get("uf", ""))
                url = f"https://nominatim.openstreetmap.org/search?format=json&street={rua}&city={cid}&state={est}&limit=5&addressdetails=1&countrycodes=br"
            else:
                url = f"https://nominatim.openstreetmap.org/search?format=json&q={requests.utils.quote(query)}&limit=5&addressdetails=1&countrycodes=br"
            return session.get(url, headers={"User-Agent": "RotasEnterprise/8.0"}, timeout=4).json()
            
        r = FILA_NOMINATIM.submit(_call_nom).result()
        resultados = []
        if r:
            for a in r[:5]:
                addr = a.get("address", {})
                resultados.append({
                    "lat": float(a['lat']), "lon": float(a['lon']), "fonte": "NOMINATIM", "score_base": 25, 
                    "cidade": addr.get('city', addr.get('town', '')).upper(), "estado": addr.get('state', '').upper(), 
                    "bairro": addr.get('neighbourhood', addr.get('suburb', '')).upper(), "logradouro": addr.get('road', '').upper(), 
                    "numero": str(addr.get('house_number', '')).upper(), "cep": addr.get('postcode', '').replace("-", "")
                })
            registrar_telemetria("NOMINATIM", True, time.time() - start_t)
        return resultados if resultados else None
    except Exception: 
        pass
    registrar_telemetria("NOMINATIM", False, time.time() - start_t)
    return None

def API_Photon(query):
    start_t = time.time()
    try:
        url = f"https://photon.komoot.io/api/?q={requests.utils.quote(query)}&limit=5&filter=countrycode:br"
        r = session.get(url, timeout=4).json()
        resultados = []
        if r.get("features"):
            for f in r["features"][:5]:
                lon, lat = f["geometry"]["coordinates"]
                props = f.get("properties", {})
                resultados.append({
                    "lat": lat, "lon": lon, "fonte": "PHOTON", "score_base": 20, 
                    "cidade": props.get("city", "").upper(), "estado": props.get("state", "").upper(), 
                    "bairro": props.get("district", "").upper(), "logradouro": props.get("street", "").upper(), 
                    "numero": str(props.get("housenumber", "")).upper(), "cep": props.get("postcode", "").replace("-", "")
                })
            registrar_telemetria("PHOTON", True, time.time() - start_t)
        return resultados if resultados else None
    except Exception: 
        pass
    registrar_telemetria("PHOTON", False, time.time() - start_t)
    return None

def forcar_geocodificacao_hierarquica_estrita(texto_cru):
    texto_norm = semantica.normalizar(texto_cru)
    candidatos = []
    
    f1 = EXECUTOR_APIS.submit(API_ArcGIS, texto_norm)
    f2 = EXECUTOR_APIS.submit(API_Nominatim, texto_norm)
    f3 = EXECUTOR_APIS.submit(API_Photon, texto_norm)
    
    for f in as_completed([f1, f2, f3]):
        res = f.result()
        if res: 
            candidatos.extend(res)
            
    if not candidatos: 
        return None
        
    candidatos.sort(key=lambda x: (x.get('score_base', 0) + (40 if x.get('bairro') else 0) + (50 if x.get('logradouro') else 0)), reverse=True)
    melhor = candidatos[0]
    
    end_f = ", ".join([c for c in [melhor.get('logradouro', ''), melhor.get('bairro', ''), melhor.get('cidade', ''), melhor.get('estado', '')] if c.strip()]) + ", BRASIL"
    return (melhor['lat'], melhor['lon'], end_f, "DESAMBIGUACAO_ESTRITA", 95, melhor.get('bairro', ''), melhor.get('cidade', ''), f"{melhor['fonte']} (Strict-Mode)", ["Desambiguação Espacial Anti-Colisão acionada em Nuvem. Resolução estrita aplicada."])

def API_OSRM_Routing(lat_o, lon_o, lat_d, lon_d):
    start_t = time.time()
    try:
        # [ETAPA5-1] alternatives=3 solicita até 3 rotas; selecionamos a de MENOR
        # DISTÂNCIA viária (regra de negócio obrigatória), não a padrão/mais rápida.
        # [FIX-OSRM-GEO1 - 18ª geração] overview=full + geometries=polyline: agora
        # capturamos a GEOMETRIA REAL da rota (polyline codificada). Antes era
        # overview=false → nenhuma geometria era retornada, e o link/mapa não conseguiam
        # desenhar o traçado (só os pontos). Com a polyline da rota vencedora, o mapa
        # embarcado desenha o trajeto EXATO usado nos cálculos e o link representa a
        # mesma rota. Custo de rede desprezível (mesma requisição, +payload da geometria).
        url = f"http://router.project-osrm.org/route/v1/driving/{lon_o},{lat_o};{lon_d},{lat_d}?overview=full&geometries=polyline&steps=true&alternatives=3"
        headers = {"User-Agent": "GerenciadorLogisticoCorp/2.0"}
        r = session.get(url, headers=headers, timeout=6).json()
        
        if r.get("code") == "Ok" and r.get("routes"):
            # Seleciona explicitamente a rota de menor distância entre todas as alternativas
            rotas = r["routes"]
            rota = min(rotas, key=lambda x: x.get("distance", float('inf')))
            distancia_km = round(rota["distance"] / 1000.0, 2)
            tempo_min = round(rota["duration"] / 60.0)
            n_alternativas = len(rotas)
            geometria_polyline = rota.get("geometry", "")  # polyline codificada da rota vencedora
            
            usa_balsa = "Não"
            for leg in rota.get("legs", []):
                for step in leg.get("steps", []):
                    if step.get("mode") == "ferry" or step.get("maneuver", {}).get("type") == "ferry":
                        usa_balsa = "Sim"
                        break

            # [OSRM-SNAP - 40ª geração] Captura os waypoints "snapados" à malha viária do OSM e a
            # DISTÂNCIA do snap (metros entre a coordenada enviada e o nó viário mais próximo). É a
            # EVIDÊNCIA técnica da causa raiz da divergência: o OSRM não usa a coordenada enviada
            # diretamente — ele a projeta na via mais próxima. Em malha rural esparsa, esse snap
            # pode deslocar a origem/destino em quilômetros. Retornado no índice 5 (aditivo).
            snap_info = None
            try:
                wps = r.get("waypoints", [])
                if len(wps) >= 2:
                    _oloc = wps[0].get("location", [None, None])  # [lon, lat] já snapado
                    _dloc = wps[1].get("location", [None, None])
                    snap_info = {
                        "orig_snap_lat": _oloc[1], "orig_snap_lon": _oloc[0],
                        "orig_snap_dist_m": round(float(wps[0].get("distance", 0.0)), 1),
                        "dest_snap_lat": _dloc[1], "dest_snap_lon": _dloc[0],
                        "dest_snap_dist_m": round(float(wps[1].get("distance", 0.0)), 1),
                    }
            except Exception:
                snap_info = None

            registrar_telemetria("OSRM", True, time.time() - start_t)
            # Retorno ampliado (idx 4 = geometria, idx 5 = snap_info). Consumidores antigos usam
            # res[0..4] com guarda len() — os campos novos são aditivos, sem quebrar compatibilidade.
            return (distancia_km, tempo_min, usa_balsa, n_alternativas, geometria_polyline, snap_info)
    except Exception: 
        pass
    registrar_telemetria("OSRM", False, time.time() - start_t)
    return None

# ==============================================================================
# [SNAP-MITIGA - 41ª geração] MITIGAÇÃO DE SNAP EXCESSIVO DO OSRM
# Quando o OSRM projeta a coordenada validada num nó viário distante (malha OSM esparsa),
# origem/destino ficam km afastados e a rota infla. A mitigação: quando o snap é grande, reúne
# candidatos de MÚLTIPLOS geocoders (ArcGIS/Nominatim/Photon) + o ponto atual, mede o snap de
# cada um via OSRM /nearest (sem rotear) e escolhe a coordenada de MENOR deslocamento que esteja
# dentro da UF — a mais representativa da via. Memoizado por município (custo amortizado no lote).
# ==============================================================================
def _osrm_nearest(lat, lon):
    """Consulta o OSRM /nearest: retorna (lat_snap, lon_snap, dist_m) do nó viário mais próximo,
    SEM rotear. Usado para escolher a coordenada que melhor representa a via antes do /route."""
    try:
        url = f"http://router.project-osrm.org/nearest/v1/driving/{lon},{lat}?number=1"
        r = session.get(url, timeout=5).json()
        if r.get("code") == "Ok" and r.get("waypoints"):
            wp = r["waypoints"][0]
            loc = wp.get("location", [None, None])
            if loc[0] is not None:
                return (loc[1], loc[0], round(float(wp.get("distance", 0.0)), 1))
    except Exception:
        pass
    return None

_MITIGA_SNAP_CACHE = {}

def _melhor_coordenada_para_osrm(texto_local, mun_nome, uf, lat_atual, lon_atual, snap_atual_m):
    """Busca a coordenada que MELHOR representa a via para o OSRM. Reúne candidatos de vários
    geocoders + o atual, mede o snap de cada via /nearest e retorna o de menor deslocamento
    dentro da UF: (lat, lon, snap_m, candidatos, houve_melhora). Mantém o atual se nada for melhor."""
    chave = (mun_nome or texto_local, uf, round(lat_atual, 4), round(lon_atual, 4))
    if chave in _MITIGA_SNAP_CACHE:
        return _MITIGA_SNAP_CACHE[chave]
    candidatos = [{"lat": lat_atual, "lon": lon_atual, "fonte": "ATUAL_VALIDADA", "snap_m": snap_atual_m}]
    try:
        _box = BOUNDING_BOXES_UF.get(uf) if uf else None
        _q = semantica.normalizar(texto_local)
        _alts = []
        for _api in (API_ArcGIS, API_Nominatim, API_Photon):
            try:
                _r = _api(_q)
                if _r:
                    _alts.append(_r[0])  # melhor candidato de cada provedor
            except Exception:
                pass
        for _a in _alts:
            _la, _lo = _a.get("lat"), _a.get("lon")
            if _la is None or _lo is None:
                continue
            if _box and not (_box["lat_min"] <= _la <= _box["lat_max"] and _box["lon_min"] <= _lo <= _box["lon_max"]):
                continue  # candidato fora da UF é descartado (evita piorar)
            _near = _osrm_nearest(_la, _lo)
            if _near:
                candidatos.append({"lat": _la, "lon": _lo, "fonte": _a.get("fonte", "ALT"), "snap_m": _near[2]})
    except Exception:
        pass
    melhor = min(candidatos, key=lambda c: c.get("snap_m", 9e18))
    houve_melhora = (melhor["fonte"] != "ATUAL_VALIDADA" and melhor.get("snap_m", 9e18) < snap_atual_m - 300.0)
    resultado = (melhor["lat"], melhor["lon"], melhor.get("snap_m", snap_atual_m), candidatos, houve_melhora)
    _MITIGA_SNAP_CACHE[chave] = resultado
    return resultado

# ==============================================================================
# MOTOR DE CONSENSO PROBABILÍSTICO BAYESIANO E CLUSTERING DBSCAN ESFÉRICO
# ==============================================================================
def processar_consenso_dinamico(candidatos, tipo_entrada, texto_cru):
    candidatos_validos = []
    candidatos_para_avaliacao = candidatos.copy()
    
    texto_norm = semantica.normalizar(texto_cru)
    ctx_inf = semantica.resolver_contexto_administrativo(texto_norm)
    uf_inf, mun_inf, dist_inf = ctx_inf.get("uf", ""), ctx_inf.get("municipio", ""), ctx_inf.get("distrito", "")
    box = BOUNDING_BOXES_UF.get(uf_inf) if uf_inf else None
    
    for c in candidatos:
        valido, lat_c, lon_c = validar_coordenada_brasil(c["lat"], c["lon"])
        if valido:
            if box:
                if not (box["lat_min"] <= lat_c <= box["lat_max"] and box["lon_min"] <= lon_c <= box["lon_max"]):
                    continue
            c["lat"], c["lon"] = lat_c, lon_c 
            candidatos_validos.append(c)
            
    if not candidatos_validos: 
        return None
        
    if uf_inf:
        candidatos_rigorosos = []
        nome_estado_inf = _normalizar_uf(uf_inf) if uf_inf else ""
        for c in candidatos_validos:
            est_api = unidecode(c.get('estado', '')).upper().strip()
            if est_api:
                if uf_inf in est_api or nome_estado_inf in est_api:
                    candidatos_rigorosos.append(c)
            else:
                candidatos_rigorosos.append(c)
        candidatos_validos = candidatos_rigorosos
        
    if not candidatos_validos: 
        return None
        
    validados_semantica = []
    for c in candidatos_validos:
        cidade_api = unidecode(c.get('cidade', '')).upper().strip()
        estado_api = unidecode(c.get('estado', '')).upper().strip()
        if cidade_api and estado_api:
            pertence_municipio = (cidade_api, estado_api) in IBGE_MUN_UF_SET
            pertence_distrito  = (cidade_api, estado_api) in IBGE_DIST_UF_SET
            if pertence_municipio or pertence_distrito: 
                validados_semantica.append(c)
        elif cidade_api not in IBGE_MUNICIPIOS and cidade_api not in IBGE_DISTRITOS: 
            validados_semantica.append(c)
        elif cidade_api:
            if cidade_api in IBGE_MUNICIPIOS or cidade_api in IBGE_DISTRITOS: 
                validados_semantica.append(c)
        else: 
            validados_semantica.append(c)
            
    candidatos_validos = validados_semantica
    if not candidatos_validos: 
        return None
        
    if tipo_entrada in ["ENDERECO_COMPLETO", "POI", "CEP", "CONDOMINIO"]: 
        raio_cluster_km = 0.5
    elif tipo_entrada in ["BAIRRO", "RURAL"]: 
        raio_cluster_km = 2.0
    else: 
        raio_cluster_km = 10.0 
        
    coords_matriz = np.array([[c["lat"], c["lon"]] for c in candidatos_validos])
    if len(coords_matriz) >= 2:
        coords_rad = np.radians(coords_matriz)
        eps_angular = raio_cluster_km / 6371.0
        # [M20] Reutiliza instâncias DBSCAN pré-criadas (elimina alocação sklearn por chamada)
        db_model = _DBSCAN_PRESETS.get(raio_cluster_km, DBSCAN(eps=eps_angular, min_samples=2, metric='haversine'))
        db_model = db_model.fit(coords_rad)
        labels = db_model.labels_
        valid_labels = [l for l in labels if l != -1]
        
        if valid_labels:
            contagem_clusters = collections.Counter(valid_labels).most_common(2)
            maior_cluster_label = contagem_clusters[0][0]
            candidatos_validos = [candidatos_validos[idx] for idx, label in enumerate(labels) if label == maior_cluster_label]
            
    if not candidatos_validos: 
        return None
        
    tolerancia_km = raio_cluster_km
    input_usuario = ParserGeograficoBR.extrair_componentes(texto_norm)
    candidatos_consistentes_mun = [c for c in candidatos_validos if validar_consistencia_municipal(c, mun_inf)]
    
    if candidatos_consistentes_mun: 
        candidatos_validos = candidatos_consistentes_mun 
        
    PESO_FONTES = {}
    DEFAULT_WEIGHTS = {"ARCGIS": 0.95, "TOMTOM": 0.90, "OVERPASS": 0.85, "NOMINATIM": 0.80, "PHOTON": 0.75}
    for fonte, d_w in DEFAULT_WEIGHTS.items():
        m_api = cache_api_health.get(fonte, {"hits": 0, "calls": 0})
        PESO_FONTES[fonte] = round(max(0.5, m_api["hits"] / m_api["calls"]), 2) if m_api["calls"] >= 50 else d_w
        
    BAYES_MULTIPLIERS = {
        "CEP": {"mun": 1.5, "uf": 1.2, "cep": 4.0, "bairro": 1.0, "numero": 1.0, "rua_peso": 0.2},
        "ENDERECO_COMPLETO": {"mun": 1.8, "uf": 1.3, "cep": 1.5, "bairro": 1.2, "numero": 2.5, "rua_peso": 1.5},
        "CONDOMINIO": {"mun": 1.8, "uf": 1.3, "cep": 1.2, "bairro": 1.5, "numero": 1.0, "rua_peso": 1.8},
        "DEFAULT": {"mun": 1.5, "uf": 1.2, "cep": 1.2, "bairro": 1.2, "numero": 1.2, "rua_peso": 0.8}
    }
    
    bm = BAYES_MULTIPLIERS.get(tipo_entrada, BAYES_MULTIPLIERS["DEFAULT"])
    
    for c1 in candidatos_validos:
        p_prior = min(c1["score_base"] / 100.0, 0.50)
        feat_mun = mun_inf and c1.get("cidade") and (mun_inf in c1["cidade"] or fuzz.token_set_ratio(mun_inf, c1["cidade"], processor=None) >= 95)
        feat_uf = uf_inf and c1.get("estado") and uf_inf in c1["estado"]
        feat_cep = input_usuario.get("cep") and c1.get("cep") and input_usuario["cep"] in c1["cep"].replace("-", "")
        feat_bairro = dist_inf and c1.get("bairro") and dist_inf in c1["bairro"]
        feat_numero = input_usuario.get("numero") and c1.get("numero") and input_usuario["numero"] in c1["numero"]
        fuzz_rua = fuzz.token_set_ratio(texto_norm, c1.get("logradouro", ""), processor=None) / 100.0 if c1.get("logradouro") else 0.1 
        
        # [M13] Usa padrões pré-compilados globais — elimina recompilação em loop duplo
        input_tem_rodovia = any(p.search(texto_norm) for p in _PADROES_RODOVIA_COMPILADOS)
        api_tem_rodovia = any(p.search(c1.get("logradouro", "").upper()) for p in _PADROES_RODOVIA_COMPILADOS) or bool(_RE_RODOVIA_GENERICA.search(c1.get("logradouro", "").upper()))
        feat_punicao_rodovia = not input_tem_rodovia and api_tem_rodovia
        
        api_end_str = f"{c1.get('logradouro','')} {c1.get('bairro','')} {c1.get('cidade','')} {c1.get('estado','')}".upper()
        l_conf_rural = 0.2 if (tipo_entrada == "RURAL" and any(urb in api_end_str for urb in ["QUADRA ", "SQN ", "SQS ", "APARTAMENTO ", "EDIFICIO ", "BLOCO "])) else 1.0
        l_conf_urbano = 0.4 if (tipo_entrada in ["ENDERECO_COMPLETO", "BAIRRO"] and any(rur in api_end_str for rur in ["CHACARA ", "FAZENDA ", "GLEBA "])) else 1.0
        
        probabilidades_cluster = [p_prior]
        apis_concordantes = set([c1["fonte"]])
        
        for c2 in candidatos_validos:
            if c1["fonte"] != c2["fonte"]:
                dist = _distancia_consenso_km(c1["lat"], c1["lon"], c2["lat"], c2["lon"])
                if dist <= tolerancia_km: 
                    apis_concordantes.add(c2["fonte"])
                    probabilidades_cluster.append(PESO_FONTES.get(c2["fonte"], 0.5)) 
                    
        falha_combinada = 1.0
        for prob in probabilidades_cluster:
            falha_combinada *= (1.0 - prob)
        prob_ensemble = 1.0 - falha_combinada
        
        l_mun = bm["mun"] if feat_mun else 0.4
        l_uf = bm["uf"] if feat_uf else 0.7
        l_cep = bm["cep"] if feat_cep else 0.9
        l_bairro = bm["bairro"] if feat_bairro else 0.9
        l_numero = bm["numero"] if feat_numero else 0.8
        l_rua = 0.5 + (fuzz_rua * bm["rua_peso"])
        l_rodovia = 0.1 if feat_punicao_rodovia else 1.0
        
        odds = (prob_ensemble / (1 - prob_ensemble)) * l_mun * l_uf * l_cep * l_bairro * l_numero * l_rua * l_rodovia * l_conf_rural * l_conf_urbano
        probabilidade_final = odds / (1 + odds)
        
        c1["score_final"] = min(probabilidade_final * 100, 99.9)
        c1["xai_data"] = {"mun": bool(feat_mun), "uf": bool(feat_uf), "cep": bool(feat_cep), "num": bool(feat_numero), "fuzz": round(fuzz_rua * 100, 1), "apis": list(apis_concordantes)}
        
    candidatos_validos.sort(key=lambda x: x["score_final"], reverse=True)
    vencedor = None
    top3_candidatos = candidatos_validos[:3]
    
    for cand in top3_candidatos:
        m = executar_reverse_geocoding_multimotor(cand["lat"], cand["lon"])
        estado_comp = m.get("estado", cand.get("estado", "")).upper().strip()
        cidade_comp = m.get("cidade", cand.get("cidade", "")).upper().strip()
        
        if uf_inf and estado_comp:
            nome_estado_inf = _normalizar_uf(uf_inf) if uf_inf else ""
            if uf_inf not in estado_comp and nome_estado_inf not in estado_comp: 
                continue 
                
        if mun_inf and cidade_comp:
            match_cid = (mun_inf in cidade_comp) or (cidade_comp in mun_inf) or (fuzz.token_set_ratio(mun_inf, cidade_comp, processor=None) >= 85)
            if not match_cid: 
                continue
                
        bairro_comp = m.get("bairro", cand.get("bairro", "")).upper().strip()
        logr_comp = m.get("logradouro", cand.get("logradouro", "")).upper().strip()
        
        end_reverse = ", ".join([c for c in [logr_comp, bairro_comp, cidade_comp, estado_comp] if c.strip()])
        similaridade = fuzz.token_set_ratio(texto_norm, end_reverse.upper(), processor=None)
        
        if similaridade >= 30 or tipo_entrada in ["BAIRRO", "MUNICIPIO", "RURAL"] or len(texto_norm.split()) <= 4:
            vencedor = cand
            break
            
    if not vencedor: 
        return None
        
    for cand in candidatos_para_avaliacao:
        if cand.get("lat", 0.0) == 0.0 or cand.get("lon", 0.0) == 0.0: 
            continue
        f_n = cand.get("fonte", "")
        metr = cache_api_health.get(f_n, {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0})
        dist_auditoria, _ = calcular_distancia_linha_reta(cand["lat"], cand["lon"], vencedor["lat"], vencedor["lon"], contexto="Auditoria Health")
        if dist_auditoria <= 0.05:
            metr["hits"] += 1
        cache_api_health.set(f_n, metr, expire=None)
        
    score_consenso = min(int(vencedor["score_final"]), 100)
    m = {
        "logradouro": vencedor.get("logradouro", ""), "bairro": vencedor["bairro"], 
        "cidade": vencedor["cidade"], "municipio": vencedor["cidade"], 
        "distrito": "", "estado": vencedor["estado"], "cep": vencedor.get("cep", "")
    }
    
    # [FIX-GEO2 - 16ª geração] Backfill de município/UF a partir do contexto resolvido.
    # CAUSA RAIZ do endereço colapsado: quando a API vencedora não devolve o campo
    # "cidade" (comum em respostas que dão só coordenadas), o município ficava vazio e o
    # endereço oficial degradava para apenas a UF. Aqui preenchemos com o município/UF
    # já inferidos do texto do usuário (ctx_inf, validados contra a base IBGE), que são
    # informação confiável que NÓS já temos. Só preenche o que está faltando — nunca
    # sobrescreve um dado da API. Garante que o endereço sempre carregue o município.
    if not m["cidade"].strip() and mun_inf:
        m["cidade"] = mun_inf  # mun_inf normalizado e validado contra IBGE (ex: CORUMBA DE GOIAS)
        m["municipio"] = mun_inf
    if not m["estado"].strip() and uf_inf:
        m["estado"] = _normalizar_uf(uf_inf) if uf_inf else uf_inf
    
    if tipo_entrada in ["MUNICIPIO", "BAIRRO", "ESTADO", "DISTRITO", "RURAL"]:
        m["logradouro"] = ""
        m["numero"] = ""
        m["cep"] = ""
        
    score_completude = 80
    if tipo_entrada == "CEP": 
        score_completude = 100
    elif tipo_entrada == "ENDERECO_COMPLETO":
        tem_numero = bool(input_usuario.get("numero") or input_usuario.get("complemento"))
        tem_cidade = bool(mun_inf); tem_uf = bool(uf_inf)
        if tem_numero and tem_cidade and tem_uf: score_completude = 100
        elif tem_cidade and tem_uf: score_completude = 95
        elif tem_cidade: score_completude = 85
        else: score_completude = 75
    elif tipo_entrada == "POI": score_completude = 95
    elif tipo_entrada == "CONDOMINIO": score_completude = 95
    elif tipo_entrada == "RURAL": score_completude = 90
    elif tipo_entrada in ["BAIRRO", "MUNICIPIO", "DISTRITO"]: score_completude = 95
    
    score_limitado = min(score_consenso, score_completude)
    if m.get("cep") and score_limitado < 100: 
        score_limitado = min(score_limitado + 10, 100 if tipo_entrada == "CEP" else 95)
        
    explicacoes_humanas = []
    explicacoes_humanas.append(f"Análise inicial baseada em {len(candidatos_validos)} candidato(s) da Nuvem.")
    xd = vencedor["xai_data"]
    
    if len(xd["apis"]) >= 2:
        explicacoes_humanas.append(f"Consenso espacial estabelecido via Ensemble Multi-API ({' + '.join(xd['apis'])}).")
    else:
        explicacoes_humanas.append(f"Inferência baseada unicamente na resposta isolada da fonte {vencedor['fonte']}.")
        
    if not ctx_inf.get("municipio"): explicacoes_humanas.append("Aviso: Validação IBGE local substituída por inteligência e preenchimento em Nuvem.")
    if xd["mun"]: explicacoes_humanas.append("Município validado na malha de referência oficial IBGE.")
    if xd["uf"]: explicacoes_humanas.append("Correspondência administrativa de Estado confirmada.")
    if xd["cep"]: explicacoes_humanas.append("Código Postal cruzado e confirmado por cascades.")
    if xd["num"]: explicacoes_humanas.append("Assinatura de número predial reconhecida na porta do cliente.")
    if xd["fuzz"] >= 80.0: explicacoes_humanas.append(f"Similaridade léxica de logradouro em {xd['fuzz']}% de aprovação.")
    
    match_logr = fuzz.token_set_ratio(texto_norm, m.get("logradouro", "").upper(), processor=None)
    match_bairro = fuzz.token_set_ratio(dist_inf, m.get("bairro", "").upper(), processor=None) if dist_inf else 100
    match_cep = 100 if input_usuario.get("cep") and m.get("cep") and input_usuario["cep"] in m.get("cep", "").replace("-", "") else 0 if input_usuario.get("cep") else 100
    
    if tipo_entrada in ["MUNICIPIO", "BAIRRO", "RURAL"]:
        confianca = "ALTA"
        score_limitado = max(score_limitado, 85)
        explicacoes_humanas.append("Busca por localidade abrangente. Score reajustado para nível de cidade/bairro.")
    elif (match_logr * 0.5) + (match_bairro * 0.3) + (match_cep * 0.2) < 65.0:
        confianca = "REVISAO_MANUAL"
        explicacoes_humanas.append("⚠️ Alerta Anti-Fantasma: Integridade semântica de logradouro inadequada.")
        score_limitado = min(score_limitado, 49)
    else:
        confianca = "ALTISSIMA" if score_limitado >= 85 else "ALTA" if score_limitado >= 75 else "MEDIA" if score_limitado >= 60 else "BAIXA"
        
    # [FIX-GEO3 - 16ª geração] Resgate de município corretamente identificado.
    # CAUSA RAIZ do Score 7: quando a classificação inicial errava (ex.: um município
    # conhecido como "Ribeirão Cascalheira, MT" era tratado como LOGRADOURO porque o
    # nome não casou de primeira), o boost de cidade não se aplicava e o endereço caía
    # na punição Anti-Fantasma — derrubando o score a valores absurdos para uma cidade
    # perfeitamente conhecida. Aqui, se o município resolvido/preenchido corresponde a
    # uma cidade REAL da base IBGE (e a entrada é essencialmente "cidade + UF", sem
    # número predial), reconhecemos a identificação como de nível municipal e reajustamos
    # o score com justiça. Validação cruzada com IBGE = identificação confiável.
    if score_limitado < 75 and not input_usuario.get("numero"):
        mun_final = (m.get("municipio", "") or "").strip().upper()
        uf_final = (uf_inf or "").strip().upper()
        municipio_real_ibge = bool(mun_final) and (
            mun_final in IBGE_MUNICIPIOS or mun_final in IBGE_DISTRITOS or
            (uf_final in IBGE_MUNICIPIOS_POR_UF and mun_final in IBGE_MUNICIPIOS_POR_UF[uf_final])
        )
        # Confirma que a entrada é basicamente o nome da cidade (poucos tokens além de cidade+UF)
        tokens_entrada = [t for t in texto_norm.split() if t not in IBGE_ESTADOS and t != "BRASIL"]
        entrada_e_localidade = len(tokens_entrada) <= 6  # nome de cidade cabe em até 6 tokens
        if municipio_real_ibge and entrada_e_localidade:
            score_limitado = max(score_limitado, 85)
            confianca = "ALTA" if confianca in ("BAIXA", "MEDIA", "REVISAO_MANUAL") else confianca
            explicacoes_humanas.append(f"Município '{mun_final}' validado na base IBGE oficial. Score reajustado para nível municipal (identificação confiável).")
        
    rua_f = m["logradouro"] if m["logradouro"] else ""
    endereco_f = ", ".join([c for c in [rua_f, m["bairro"], m["cidade"], m["estado"]] if c.strip()]) + ", BRASIL"
    
    if vencedor["lat"] == 0.0 or vencedor["lon"] == 0.0:
        return None
        
    return vencedor["lat"], vencedor["lon"], endereco_f, confianca, score_limitado, m["distrito"], m["municipio"], vencedor["fonte"], explicacoes_humanas

# ==============================================================================
# ORQUESTRADOR EM CASCATA HIERÁRQUICA E OFFLINE-FIRST
# ==============================================================================

# [ANTI-ALUCINACAO - 34ª geração] Camada de validação de localidades (defesa em profundidade).
# OBJETIVO: quando a intenção do usuário é claramente um MUNICÍPIO, NUNCA aceitar como resultado
# final um ponto hiperespecífico (rua+número, hotel, pousada, chalé, estabelecimento). Mesmo que
# a classificação erre OU um provedor "alucine" um POI, esta camada rejeita o resultado e devolve
# o CENTRÓIDE oficial do IBGE com o nome qualificado. É independente da classificação — um
# salva-vidas. NÃO afeta resultados legítimos: entradas com rua/número/POI não disparam a guarda.

# Termos que, NO RESULTADO geocodificado, denunciam um ponto hiperespecífico (não um município).
_MARCADORES_POI_RESULTADO = [
    "HOTEL", "POUSADA", "CHALE", "CHALES", "RESTAURANTE", "MOTEL", "RESORT", "HOSTEL", "FLAT",
    "CONDOMINIO", "EDIFICIO", "SHOPPING", "LOJA", "APARTAMENTO", "VILLA", "BUNGALOW", "INN",
    "ENTIRE PLACE", "ENTIRE HOME", "GUEST", "BED AND BREAKFAST", "AIRBNB", "AGENCIA", "QUIOSQUE"
]
_MARCADORES_VIA_RESULTADO = [
    "RUA", "AVENIDA", "TRAVESSA", "ALAMEDA", "VIELA", "LADEIRA", "BECO", "LARGO", "PRACA"
]

def _resultado_hiperespecifico(end_f):
    """True se o endereço geocodificado parece um ponto específico (rua+número ou POI/
    estabelecimento) em vez de um município. Um endereço municipal limpo
    ('Município, Estado, BRASIL') nunca é sinalizado."""
    if not end_f:
        return False
    alvo = unidecode(str(end_f)).upper()
    # 1) Número de porta/casa: vírgula seguida de número curto, ou "Nº 466"
    if re.search(r',\s*\d{1,6}(\s|,|$|-)', alvo) or re.search(r'\bN[º°O]?\.?\s*\d{1,6}\b', alvo):
        return True
    # 2) Abreviação de rua no início de um trecho ("R. Francisco...", "AV. ...")
    if re.search(r'(^|,)\s*(R|AV|TV|AL|TR|PCA|ROD|EST)\.\s', alvo):
        return True
    # 3) Termo de via por extenso
    if any(re.search(rf'\b{re.escape(m)}\b', alvo) for m in _MARCADORES_VIA_RESULTADO):
        return True
    # 4) POI/estabelecimento
    if any(m in alvo for m in _MARCADORES_POI_RESULTADO):
        return True
    return False

def _intencao_municipio(texto_norm, tipo_entrada, ctx):
    """True quando a intenção do usuário é claramente um MUNICÍPIO (ou distrito), de forma
    INDEPENDENTE da classificação. Reproduz a tolerância à forma curta do FIX-MUN-CLASS e
    exige ausência de sinais de especificidade (número, via, POI, bairro) na entrada."""
    if tipo_entrada in ("MUNICIPIO", "DISTRITO"):
        return True
    mun = ctx.get("municipio", "")
    uf = ctx.get("uf", "")
    if not (mun and uf):
        return False
    # Texto sem a UF (sigla e nome por extenso) e sem "BRASIL"
    t = texto_norm
    for termo in [uf, IBGE_ESTADOS.get(uf, ""), "BRASIL", "BRAZIL"]:
        if termo:
            t = re.sub(rf'\b{re.escape(unidecode(termo).upper())}\b', ' ', t)
    t = re.sub(r'[^A-Z0-9]+', ' ', t).strip()
    if not t:
        return False  # só UF/ruído, sem termo de município → não afirmar intenção municipal
    # Sinais de especificidade na entrada → NÃO é intenção municipal (não dispara a guarda)
    if re.search(r'\d', t):
        return False
    if any(k in t for k in (semantica.via_keys + semantica.bairro_keys + POI_KEYWORDS)):
        return False
    # 't' é o termo do usuário para o município. Intenção municipal se casa com o nome oficial:
    # igual, prefixo (forma curta) ou todos os tokens ⊆ nome oficial (mesma regra do FIX-MUN-CLASS)
    mun_tokens = set(mun.split())
    t_tokens = set(t.split())
    return bool(t == mun or mun.startswith(t + " ") or (t_tokens and t_tokens.issubset(mun_tokens)))

def _blindar_municipio(texto_norm, tipo_entrada, ctx, res_final):
    """Camada anti-alucinação: se a intenção é município mas o resultado é hiperespecífico,
    rejeita e devolve o centróide oficial do IBGE + nome qualificado. Caso contrário, devolve
    o resultado inalterado (nunca piora; nunca altera endereços/POI legitimamente pedidos)."""
    if not res_final or not _intencao_municipio(texto_norm, tipo_entrada, ctx):
        return res_final
    if not _resultado_hiperespecifico(res_final[2]):
        return res_final  # resultado já é municipal/limpo
    mun_nome = ctx.get("municipio", "")
    uf_nome = ctx.get("uf", "")
    # [BASE-IBGE-CENTROIDE] resolve o centróide municipal (offline se houver; senão
    # por cidade+UF). Antes exigia lat IBGE ≠ 0 — que nunca existe — então a blindagem
    # ficava inerte em produção. Agora ela realmente substitui o ponto hiperespecífico.
    if mun_nome and uf_nome:
        _item, _cod = _info_municipio_ibge(mun_nome, uf_nome)
        if _item is not None:
            lat_c, lon_c = _centroide_municipio(mun_nome, uf_nome)
            if lat_c != 0.0 and lon_c != 0.0:
                endereco_ibge = f"{mun_nome}, {IBGE_ESTADOS.get(uf_nome, uf_nome)}, BRASIL"
                _cod_txt = f" (código IBGE {_cod})" if _cod else ""
                return (lat_c, lon_c, endereco_ibge, "MUNICIPAL", 88,
                        ctx.get("distrito", ""), mun_nome, "VALIDACAO_ANTI_ALUCINACAO",
                        [f"Anti-alucinação: provedor retornou ponto hiperespecífico "
                         f"('{str(res_final[2])[:60]}') para intenção municipal; substituído pelo "
                         f"centróide oficial do município{_cod_txt} (base IBGE)."])
    return res_final  # sem centróide disponível → mantém (não degrada)

def _obter_coordenadas_e_endereco_oficial_core(localidade):
    texto_cru = str(localidade).strip()
    if not texto_cru or texto_cru.lower() == 'nan': 
        return 0.0, 0.0, "", "BAIXA", 0, "", "", "N/A", ["String Vazia"]
        
    texto_norm = semantica.normalizar(texto_cru)
    
    if match_coords := re.match(r'^\s*(-?\d{1,2}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)\s*$', texto_cru):
        lat_in, lon_in = float(match_coords.group(1)), float(match_coords.group(2))
        valido, lat_in, lon_in = validar_coordenada_brasil(lat_in, lon_in)
        if valido:
            m = executar_reverse_geocoding_multimotor(lat_in, lon_in)
            end_f = ", ".join([c for c in [m.get("logradouro", ""), m.get("bairro", ""), m.get("cidade", ""), m.get("estado", "")] if c.strip()]) + ", BRASIL"
            return lat_in, lon_in, end_f, "ABSOLUTA", 100, m.get("bairro", ""), m.get("cidade", ""), "COORDENADA_EXATA", ["Entrada direta via Coordenadas Numéricas."]
            
    if texto_norm in cache_aprendizado:
        dado_salvo = cache_aprendizado[texto_norm]
        if isinstance(dado_salvo, dict) and "lat" in dado_salvo and "lon" in dado_salvo:
            return dado_salvo["lat"], dado_salvo["lon"], dado_salvo.get("endereco", texto_norm), "ALTISSIMA", 100, dado_salvo.get("distrito", ""), dado_salvo.get("municipio", ""), "APRENDIZADO_LOCAL", ["Ponto quente extraído do cache local enriquecido."]
            
    endereco_canonico, tipo_entrada, _, _, _ = semantica.construir_endereco_canonico(texto_norm)
    parsed_comp = ParserGeograficoBR.extrair_componentes(texto_norm)
    
    cache_key = hashlib.md5(f"GEO_{CACHE_VERSION}_{tipo_entrada}_{endereco_canonico}".encode('utf-8'), usedforsecurity=False).hexdigest()
    if cache_key in cache_geo:
        c = cache_geo[cache_key]
        if c.get("lat", 0.0) != 0.0 and c.get("lon", 0.0) != 0.0:
            return c["lat"], c["lon"], c["endereco"], c["confianca"], c["score_num"], c["distrito"], c["municipio"], c["fonte"], ["Cache L2 Hit."]
            
    ctx = semantica.resolver_contexto_administrativo(texto_norm)
    if ctx.get("municipio") and ctx.get("uf"):
        mun_nome = ctx["municipio"]
        uf_nome = ctx["uf"]
        if tipo_entrada == "MUNICIPIO":
            # [BASE-IBGE-CENTROIDE] entrada é um município reconhecido na base nacional:
            # resolve o centróide (offline se houver lat≠0; senão por cidade+UF) e o torna a
            # referência oficial do pipeline ANTES da cascata de geocodificação. Se o centróide
            # responder, retorna cedo (mais rápido que a cascata); senão, segue o fluxo normal.
            _item, _cod = _info_municipio_ibge(mun_nome, uf_nome)
            if _item is not None:
                lat_c, lon_c = _centroide_municipio(mun_nome, uf_nome)
                if lat_c != 0.0 and lon_c != 0.0:
                    _fonte_mun = "BASE_IBGE_OFFLINE" if (_item.get("lat", 0.0) != 0.0) else "BASE_IBGE_CENTROIDE"
                    endereco_ibge = f"{mun_nome}, {IBGE_ESTADOS.get(uf_nome, uf_nome)}, BRASIL"
                    _cod_txt = f" Código IBGE {_cod}." if _cod else ""
                    res_final = (lat_c, lon_c, endereco_ibge, "MUNICIPAL", 100, ctx.get("distrito", ""), mun_nome, _fonte_mun,
                                 [f"Referência oficial da base nacional IBGE: município reconhecido pelo nome.{_cod_txt} "
                                  f"Centróide municipal usado como entrada do pipeline (sem ponto hiperespecífico)."])
                    _cache_set_seguro(cache_geo, cache_key, {"lat": res_final[0], "lon": res_final[1], "endereco": res_final[2], "confianca": res_final[3], "score_num": res_final[4], "distrito": res_final[5], "municipio": res_final[6], "fonte": res_final[7]}, expire=2592000)
                    return res_final
                        
    rua_suja = parsed_comp["resto"]
    for loc in [ctx.get("municipio", ""), ctx.get("distrito", ""), ctx.get("uf", ""), "BRASIL", "DF"]:
        if loc: 
            rua_suja = _regex_palavra(loc).sub('', rua_suja).strip(" ,-")
    rua_limpa = re.sub(r'\s+', ' ', rua_suja).strip()
    if parsed_comp["numero"]: 
        rua_limpa = f"{rua_limpa} {parsed_comp['numero']}".strip()
        
    contexto_estruturado = {
        "logradouro": rua_limpa if rua_limpa else texto_norm,
        "bairro": ctx.get("distrito", ""),
        "municipio": ctx.get("municipio", ""),
        "uf": ctx.get("uf", ""),
        "cep": parsed_comp.get("cep", "")
    }
    
    if match_offline := obedience_base_local(contexto_estruturado):
        return match_offline["lat"], match_offline["lon"], match_offline["endereco"], "ALTISSIMA", 100, match_offline.get("distrito", ""), match_offline.get("municipio", ""), "BASE_NACIONAL_OFFLINE", ["Ponto resolvido via CNEFE/Bases Locais Estáticas."]
        
    candidatos_validos = []
    if tipo_entrada == "CEP":
        cep_estrito = re.search(r'\b\d{5}-?\d{3}\b', texto_norm)
        if cep_estrito:
            cep_limpo = cep_estrito.group(0).replace("-", "")
            logr, bair, loca, uf, lat_c, lon_c = cascata_postal_tripla(cep_limpo)
            if loca:
                nome_est_cep = IBGE_ESTADOS.get(uf, uf) if uf else ""
                addr_c = f"{logr}, {bair}, {loca}, {nome_est_cep}, CEP {cep_estrito.group(0)}, BRASIL"
                addr_c = re.sub(r',\s*,', ',', addr_c).strip(' ,')
                val_c, lat_corrigida_c, lon_corrigida_c = validar_coordenada_brasil(lat_c, lon_c)
                if lat_c != 0.0 and lon_c != 0.0 and val_c:
                    res_final = (lat_corrigida_c, lon_corrigida_c, addr_c, "ALTISSIMA", 100, bair, loca, "BrasilAPI/OSM Postal", ["Cascata Postal Direta."])
                    _cache_set_seguro(cache_geo, cache_key, {"lat": lat_corrigida_c, "lon": lon_corrigida_c, "endereco": addr_c, "confianca": "ALTISSIMA", "score_num": 100, "distrito": bair, "municipio": loca, "fonte": "BrasilAPI/OSM Postal"}, expire=2592000)
                    return res_final
                    
                res_arc = API_ArcGIS(addr_c)
                if res_arc:
                    if isinstance(res_arc, list): 
                        res_arc = res_arc[0]
                    val_arc, lat_corrigida_arc, lon_corrigida_arc = validar_coordenada_brasil(res_arc["lat"], res_arc["lon"])
                    if val_arc:
                        res_final = (lat_corrigida_arc, lon_corrigida_arc, addr_c, "ALTISSIMA", 100, bair, loca, "ViaCEP/ArcGIS", ["Cascata Postal Complementada por ArcGIS."])
                        _cache_set_seguro(cache_geo, cache_key, {"lat": lat_corrigida_arc, "lon": lon_corrigida_arc, "endereco": addr_c, "confianca": "ALTISSIMA", "score_num": 100, "distrito": bair, "municipio": loca, "fonte": "ViaCEP/ArcGIS"}, expire=2592000)
                        return res_final
                        
    def disparar_apis_paralelas(tarefas):
        resultados = []
        for f in as_completed([EXECUTOR_APIS.submit(func, *args, **kwargs) for func, args, kwargs in tarefas]):
            if res := f.result(): 
                resultados.extend(res)
        return resultados
        
    if tipo_entrada == "POI" or tipo_entrada == "CONDOMINIO":
        candidatos_validos.extend(disparar_apis_paralelas([(API_TomTom, (endereco_canonico,), {})]))
    elif tipo_entrada in ["ENDERECO_COMPLETO", "LOGRADOURO"]:
        candidatos_validos.extend(disparar_apis_paralelas([(API_ArcGIS, (endereco_canonico,), {"ctx": contexto_estruturado}), (API_TomTom, (endereco_canonico,), {})]))
    elif tipo_entrada in ["BAIRRO", "MUNICIPIO", "DISTRITO"]:
        candidatos_validos.extend(disparar_apis_paralelas([(API_Photon, (endereco_canonico,), {})]))
    else:
        candidatos_validos.extend(disparar_apis_paralelas([(API_Photon, (endereco_canonico,), {}), (API_ArcGIS, (endereco_canonico,), {"ctx": contexto_estruturado}), (API_TomTom, (endereco_canonico,), {})]))
        
    res_final = processar_consenso_dinamico(candidatos_validos, tipo_entrada, texto_cru)
    
    if not res_final:
        res_nom = API_Nominatim(endereco_canonico, ctx=contexto_estruturado)
        if not res_nom: 
            res_nom = API_Photon(endereco_canonico)
        if res_nom:
            candidatos_validos.extend(res_nom)
            res_final = processar_consenso_dinamico(candidatos_validos, tipo_entrada, texto_cru)
            
    if not res_final and ctx.get("municipio") and ctx.get("uf"):
        mun_nome = ctx["municipio"]
        uf_nome = ctx["uf"]
        _item, _cod = _info_municipio_ibge(mun_nome, uf_nome)
        # offline com lat≠0 (caso a base passe a ter coordenada própria): confiança alta, sem reverse
        if _item is not None and _item.get("lat", 0.0) != 0.0 and _item.get("lon", 0.0) != 0.0:
            endereco_ibge = f"{mun_nome}, {IBGE_ESTADOS.get(uf_nome, uf_nome)}, BRASIL"
            res_final = (_item["lat"], _item["lon"], endereco_ibge, "MUNICIPAL", 90, ctx.get("distrito", ""), mun_nome, "BASE_IBGE_OFFLINE", ["Blindagem Ativa IBGE: APIs falharam, coordenada offline recuperada da base local para a UF."])

        if not res_final:
            # [BASE-IBGE-CENTROIDE] centróide municipal via helper compartilhado (reaproveita
            # cache em RAM, evitando nova chamada de rede se o município já foi resolvido).
            lat_c, lon_c = _centroide_municipio(mun_nome, uf_nome)
            if lat_c != 0.0 and lon_c != 0.0:
                val_rev = executar_reverse_geocoding_multimotor(lat_c, lon_c)
                est_rev = unidecode(val_rev.get("estado", "")).upper()
                nome_estado_inf = unidecode(IBGE_ESTADOS.get(uf_nome, uf_nome)).upper()
                if uf_nome in est_rev or nome_estado_inf in est_rev:
                    endereco_ibge = f"{mun_nome}, {IBGE_ESTADOS.get(uf_nome, uf_nome)}, BRASIL"
                    _cod_txt = f" Código IBGE {_cod}." if _cod else ""
                    res_final = (lat_c, lon_c, endereco_ibge, "MUNICIPAL", 85, ctx.get("distrito", ""), mun_nome, "BASE_IBGE_CENTROIDE", [f"Resgatado via centróide municipal da base nacional e UF confirmada.{_cod_txt}"])
                    
    res_final = _blindar_municipio(texto_norm, tipo_entrada, ctx, res_final)
    if res_final:
        _cache_set_seguro(cache_geo, cache_key, {"lat": res_final[0], "lon": res_final[1], "endereco": res_final[2], "confianca": res_final[3], "score_num": res_final[4], "distrito": res_final[5], "municipio": res_final[6], "fonte": res_final[7]}, expire=2592000)
        return res_final
        
    return 0.0, 0.0, endereco_canonico, "BAIXA", 0, "", "", "N/A", ["Falha Geográfica Absoluta por falta de candidatos e centróides na nuvem."]

def obter_coordenadas_e_endereco_oficial(localidade):
    if str(localidade).strip() == "FALHA_GEO_DESTINO" or str(localidade).strip() == "NENHUM_HUB_VALIDO" or str(localidade).strip() == "FALHA_GEO_ORIGEM":
        return 0.0, 0.0, "Falha de Geocodificação ou Alocação", "BAIXA", 0, "", "", "N/A", ["Ponto geográfico inválido retornado na pré-geocodificação de Hubs."]
        
    lat, lon, end_f, conf, score, dist, mun, fonte, xai = _obter_coordenadas_e_endereco_oficial_core(localidade)
    
    # [M15] Reverse geocoding só quando coordenadas foram entrada DIRETA do usuário
    # Para resultados de API, end_f/mun/dist já vêm preenchidos na resposta — sleep 1.1s desnecessário
    entrada_foi_coordenada = fonte == "COORDENADA_EXATA"
    
    if lat != 0.0 and lon != 0.0:
        campos_vazios = (not end_f or end_f.strip() == "") or (not mun or mun.strip() == "")
        # Só faz reverse se: entrada foi coordenada direta OU campos críticos estão vazios E é API conhecida
        if entrada_foi_coordenada or (campos_vazios and fonte not in ["BASE_IBGE_OFFLINE", "BASE_NACIONAL_OFFLINE", "APRENDIZADO_LOCAL"]):
            rev = executar_reverse_geocoding_multimotor(lat, lon)
            if not end_f or end_f.strip() == "":
                end_f = ", ".join([c for c in [rev.get("logradouro", ""), rev.get("bairro", ""), rev.get("cidade", ""), rev.get("estado", "")] if c.strip()]) + ", BRASIL"
            if not mun or mun.strip() == "":
                mun = rev.get("cidade", "")
            if not dist or dist.strip() == "":
                dist = rev.get("bairro", "")
                
    if not end_f or end_f.strip() == "": end_f = f"Localidade não mapeável: {localidade}"
    if not mun or mun.strip() == "": mun = "Município Não Mapeado"
    if not dist or dist.strip() == "": dist = "Distrito Não Mapeado"
    if not conf or conf.strip() == "": conf = "BAIXA"
    if score is None: score = 0
    if not fonte or fonte.strip() == "": fonte = "Dedução Heurística"
    if not xai: xai = ["Auditoria preenchida via Fallback Estrutural do Motor."]
    
    return lat, lon, end_f, conf, score, dist, mun, fonte, xai

# ==============================================================================
# MOTOR DE ROTEAMENTO EXTREMO E PIPELINE UNIFICADO
# ==============================================================================
def extrair_dados_reais_google(origem_texto, destino_texto, lat_o, lon_o, lat_d, lon_d, dist_linha_reta, usar_coordenadas=True, link_maps_pronto=None, link_embed_pronto=None):
    cache_key = f"GOOG_{CACHE_VERSION}_{origem_texto}|{destino_texto}|{usar_coordenadas}"
    if cache_key in cache_google: 
        _cached = cache_google[cache_key]
        # [FIX-MUN-LINK] Se temos links prontos (priorizando município), sobrescreve os
        # links do cache mantendo distância/tempo/score/geometria (dependem só de coords).
        if link_maps_pronto and _cached and len(_cached) >= 6:
            _geo_c = _cached[6] if len(_cached) > 6 else ""  # [VIS-GOOGLE-GEO] preserva geometria
            return (_cached[0], _cached[1], link_maps_pronto, _cached[3], _cached[4], link_embed_pronto or _cached[5], _geo_c)
        return _cached
        
    orig_link_txt = requests.utils.quote(origem_texto)
    dest_link_txt = requests.utils.quote(destino_texto)
    origem_param_scraper = f"{lat_o},{lon_o}" if usar_coordenadas else orig_link_txt
    destino_param_scraper = f"{lat_d},{lon_d}" if usar_coordenadas else dest_link_txt
    
    url_api = f"https://www.google.com/maps/preview/directions?authuser=0&hl=pt-BR&gl=br&pb=!1m2!1m1!1s{origem_param_scraper}!1m2!1m1!1s{destino_param_scraper}!3e0"
    # [FIX-MUN-LINK] Usa os links prontos (município priorizado) quando fornecidos pelo
    # pipeline; caso contrário, constrói a partir do texto (compatibilidade).
    link_maps = link_maps_pronto if link_maps_pronto else f"https://www.google.com/maps/dir/?api=1&origin={orig_link_txt}&destination={dest_link_txt}&travelmode=driving"
    link_embed = link_embed_pronto if link_embed_pronto else f"https://maps.google.com/maps?saddr={orig_link_txt}&daddr={dest_link_txt}&output=embed"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        resposta = session.get(url_api, headers=headers, timeout=12)
        texto_resposta = resposta.text.replace('\u202f', ' ').replace('\u200b', '')
        if len(texto_resposta) < 500: 
            return None
            
        dist_match = _RE_DIST_G1.search(texto_resposta)
        if not dist_match: dist_match = _RE_DIST_G2.search(texto_resposta)
        if not dist_match: dist_match = _RE_DIST_G3.search(texto_resposta)
        if not dist_match: dist_match = _RE_DIST_G4.search(texto_resposta)
        time_match = _RE_TIME_G1.search(texto_resposta)
        if not time_match: time_match = _RE_TIME_G2.search(texto_resposta)
        if not time_match: time_match = _RE_TIME_G3.search(texto_resposta)
        
        if dist_match and time_match:
            km_str = dist_match.group(1)
            tempo_str = time_match.group(1)
            if km_str.count('.') == 1 and ',' not in km_str:
                if len(km_str.split('.')[1]) == 3: km_str = km_str.replace('.', '')
                else: km_str = km_str.replace('.', '.')
            elif ',' in km_str and '.' in km_str:
                km_str = km_str.replace('.', '').replace(',', '.')
            elif ',' in km_str:
                km_str = km_str.replace(',', '.')
            else:
                km_str = km_str.replace('.', '')
                
            try:
                km_puro = float(km_str)
            except ValueError:
                km_puro = 0.0
                
            balsa_patterns = [r'esta rota inclui uma balsa', r'pegar a balsa', r'ferry route', r'travessia de balsa']
            envolve_balsa = "Sim" if any(re.search(p, texto_resposta.lower()) for p in balsa_patterns) else "Não"
            if dist_linha_reta > 0 and km_puro > (dist_linha_reta * 2.5):
                envolve_balsa = "Não"
                
            # [M16] Validação de sanidade pós-parsing: descarta resultado se distância for fisicamente implausível
            # Razão > 3.5× da linha reta é impossível no Brasil (maior desvio documentado = ~3.2× no Pantanal)
            if dist_linha_reta > 0 and km_puro > (dist_linha_reta * 3.5):
                logger.warning(
                    "[M16] Distância do Google descartada por sanidade: %.1f km vs linha reta %.1f km (ratio=%.2f)",
                    km_puro, dist_linha_reta, km_puro / dist_linha_reta,
                    extra={"fonte": "GOOGLE_MAPS", "sucesso": False, "latencia_ms": 0, "query": f"{origem_texto}|{destino_texto}"}
                )
                return None
                
            score_google = 80 + (10 if km_puro > 0 else 0) + (10 if tempo_str else 0)
            score_google = min(score_google, 100)
            # [VIS-GOOGLE-GEO - 27ª geração] PRIORIDADE MÁXIMA Nº 1: extrai a GEOMETRIA da
            # rota do Google e desenha o TRAÇADO COMPLETO no mapa embarcado (antes só dois
            # marcadores). A geometria é validada geograficamente; se válida, o mapa passa
            # a ser um Leaflet autocontido com a polyline real (curvas, conversões). Se a
            # extração falhar, mantém-se o embed clássico do Google (degradação graciosa).
            geo_google = _extrair_geometria_google(texto_resposta, lat_o, lon_o, lat_d, lon_d)
            res = (km_puro, tempo_str, link_maps, envolve_balsa, score_google, link_embed, geo_google)
            cache_google.set(cache_key, res, expire=2592000)
            return res
    except Exception: 
        pass
    return None

@_lru_cache(maxsize=32)
def obter_fator_desvio_rodoviario(linha_reta):
    return 1.45 if linha_reta < 5.0 else 1.35 if linha_reta < 20.0 else 1.25 if linha_reta < 100.0 else 1.18

def _eh_entrada_municipio(texto_cru, municipio_resolvido):
    """[FIX-MUN-LINK - 23ª geração] Determina se a ENTRADA do usuário representa um
    MUNICÍPIO/DISTRITO (e não um endereço específico, POI, estabelecimento ou CEP).
    Quando verdadeiro, o link do Google deve forçar o município (nome oficial + UF),
    em vez de deixar o Google escolher um POI/endereço dentro da cidade.

    Critério: usa a classificação semântica já existente (MUNICIPIO/DISTRITO) e confirma
    que NÃO há sinais de endereço específico (número predial, palavra de via, POI, CEP).
    """
    if not texto_cru or not str(texto_cru).strip():
        return False
    try:
        texto_norm = semantica.normalizar(str(texto_cru))
    except Exception:
        texto_norm = str(texto_cru).upper().strip()
    # Sinais de que NÃO é um município puro (é endereço/POI específico)
    tem_numero = bool(re.search(r'\d', texto_norm))
    tem_cep = bool(re.search(r'\d{5}-?\d{3}', str(texto_cru)))
    tem_poi = any(k in texto_norm for k in POI_KEYWORDS) if 'POI_KEYWORDS' in globals() else False
    if tem_cep or tem_numero or tem_poi:
        return False
    # Classificação semântica oficial
    try:
        tipo = semantica.classificar_entrada(texto_norm)
        if tipo in ("MUNICIPIO", "DISTRITO"):
            return True
    except Exception:
        pass
    # Confirmação adicional: o município resolvido bate com o texto (sem número/via)
    if municipio_resolvido and municipio_resolvido not in ("Município Não Mapeado", ""):
        try:
            mun_norm = semantica.normalizar(municipio_resolvido)
            # Se o texto é essencialmente "<município> <uf>", é entrada de município
            texto_sem_uf = texto_norm
            for uf_sig in IBGE_ESTADOS:
                texto_sem_uf = _regex_palavra(uf_sig).sub('', texto_sem_uf)
            for uf_nome in IBGE_ESTADOS.values():
                texto_sem_uf = texto_sem_uf.replace(uf_nome, '')
            texto_sem_uf = texto_sem_uf.replace("BRASIL", "").strip()
            if texto_sem_uf and (texto_sem_uf in mun_norm or mun_norm in texto_sem_uf):
                return True
        except Exception:
            pass
    return False

def _montar_param_municipio_google(texto_cru, municipio_resolvido, uf_resolvida, end_oficial, lat, lon):
    """[FIX-MUN-LINK / VIS-NAMES-LINK - 28ª geração] Constrói o parâmetro de origem/destino
    para o link do Google Maps PRIORIZANDO O NOME OFICIAL do município quando a entrada
    representa uma cidade.

    EVOLUÇÃO (decisão do usuário): a experiência deve ser guiada por NOMES, não coordenadas.
    Por isso o link de um município passa a usar o NOME OFICIAL TOTALMENTE QUALIFICADO —
    "Município, Estado por extenso, Brasil" (ex.: "Corumbá de Goiás, Goiás, Brasil").

    Por que isso é seguro (resolve a antiga ambiguidade município→POI): o problema histórico
    ocorria ao passar o TEXTO CRU e curto do usuário ("Corumbá, GO"), que o Google podia
    interpretar como um POI/endereço. Aqui usamos o nome OFICIAL e TOTALMENTE QUALIFICADO já
    resolvido pelo pipeline (via IBGE), com o estado por extenso e "Brasil" — a forma textual
    mais estável e inequívoca, que o Google resolve para a CIDADE de forma confiável e ainda
    EXIBE o nome para o usuário (em vez de um par de coordenadas). As coordenadas continuam
    sendo a âncora interna do cálculo; o link mostra o nome. Para entradas que NÃO são
    município (endereço real, POI), mantém o comportamento blindado (que já prioriza o nome).
    """
    if _eh_entrada_municipio(texto_cru, municipio_resolvido) and municipio_resolvido and municipio_resolvido not in ("Município Não Mapeado", ""):
        # [VIS-NAMES-LINK] Nome oficial totalmente qualificado: "Município, Estado, Brasil".
        partes = [municipio_resolvido]
        uf_full = IBGE_ESTADOS.get(uf_resolvida, uf_resolvida) if uf_resolvida else ""
        if uf_full and uf_full.strip():
            partes.append(uf_full.strip())
        partes.append("Brasil")
        rotulo_municipio = ", ".join(partes)
        return requests.utils.quote(rotulo_municipio)
    # Não é município → comportamento blindado (já prioriza o nome oficial)
    return _montar_param_link_seguro(end_oficial, lat, lon, texto_cru)

def _montar_param_link_seguro(endereco_oficial, lat, lon, texto_original):
    """[FIX-GEO1 - 16ª geração] Constrói o parâmetro de origem/destino para o link do
    Google Maps de forma BLINDADA, garantindo que nunca se perca a identificação real
    do local. Ordem de prioridade:
      1. Endereço oficial, SE for rico o suficiente (mais que apenas uma sigla de UF).
      2. Coordenadas exatas (lat,lon), se válidas — sempre apontam ao local correto.
      3. Texto original do usuário (ex: "Corumbá, GO"), como rede de segurança final.
    Isso corrige o bug em que o link recebia apenas "GO" quando a API não devolvia o
    município. O texto original do usuário sempre carrega o município que ele digitou.
    """
    end = (endereco_oficial or "").strip()
    # Detecta endereço "pobre": vazio, ou que é só a sigla/nome de uma UF (+ "BRASIL").
    # Ex.: "GO", "GO, BRASIL", "GOIAS, BRASIL" — todos colapsaram e perderam o município.
    # [FIX-NOMES-EMBED - 35ª geração] Detecção de endereço "pobre" robusta a UFs de nome
    # COMPOSTO. A versão anterior filtrava token a token contra IBGE_ESTADOS.values(), então
    # nomes como "MATO GROSSO"/"RIO DE JANEIRO" (multi-palavra) escapavam e o endereço era
    # tratado como "rico" indevidamente. Aqui removemos "BRASIL/BR" e verificamos se o que
    # SOBRA é exatamente uma UF (sigla OU nome por extenso) — nesse caso, perdeu-se o
    # município/logradouro e o endereço é pobre.
    _end_sem_pais = re.sub(r'\b(BRASIL|BRAZIL|BR)\b', ' ', end.upper())
    _end_sem_pais = re.sub(r'[,\s]+', ' ', _end_sem_pais).strip()
    _ufs_todas = set(IBGE_ESTADOS.keys()) | {str(v).upper().strip() for v in IBGE_ESTADOS.values()}
    endereco_pobre = (not end) or (not _end_sem_pais) or (_end_sem_pais in _ufs_todas)

    if not endereco_pobre:
        return requests.utils.quote(end)
    # [FIX-NOMES-EMBED - 35ª geração] CAUSA RAIZ das coordenadas no mapa/link: quando o
    # endereço oficial degradava (ex.: API devolveu só a UF, ou o centróide IBGE do município
    # veio 0,0 e a cascata empobreceu o resultado), este builder devolvia COORDENADAS como
    # elemento visual — exatamente o que o usuário relata ("-12.73..., -51.71..."). O script
    # antigo NUNCA faz isso: ele sempre usa o NOME (origem_texto/origem_clean). Adotamos a
    # mesma estratégia robusta: o TEXTO DO USUÁRIO sempre carrega o nome que ele digitou
    # ("Ribeirão Cascalheira, MT"), que o Google resolve e EXIBE como nome. As coordenadas
    # passam a ser apenas a ÚLTIMA âncora (caso não exista texto algum — não deve ocorrer).
    texto_seg = (texto_original or "").strip()
    _texto_e_coordenada = bool(re.match(r'^\s*-?\d{1,3}\.\d+\s*,\s*-?\d{1,3}\.\d+\s*$', texto_seg))
    if texto_seg and texto_seg.lower() != "nan" and not _texto_e_coordenada:
        return requests.utils.quote(texto_seg)
    # Última âncora: coordenadas exatas (só quando não há texto utilizável, ou o próprio
    # texto do usuário JÁ é uma coordenada — caso em que mostrar coordenadas é o correto).
    if lat and lon and lat != 0.0 and lon != 0.0:
        return f"{lat},{lon}"
    return requests.utils.quote(end) if end else f"{lat},{lon}"

def _montar_link_google_navegavel(lat_o, lon_o, lat_d, lon_d, end_o="", end_d="", txt_o="", txt_d=""):
    """[CONTINGENCIA-OSRM - 20ª geração] Constrói um link de navegação do Google Maps
    ROBUSTO e CONFIÁVEL, que SEMPRE abre a rota completamente traçada entre origem e
    destino. Usa a Google Maps URL API oficial e documentada (/maps/dir/?api=1), que
    desenha o percurso viário entre os pontos de forma estável.

    Por que este caminho: após investigação, concluiu-se que NÃO há forma robusta e
    documentada de gerar um link COMPARTILHÁVEL do OSRM com a rota traçada — o
    map.project-osrm.org (waypoints) e o geojson.io (data: URI) dependem de comportamentos
    não-documentados/legados e falhavam (ex.: "String não finalizada em JSON" no
    geojson.io, que trunca o data: URI). Conforme o plano de contingência, o link
    compartilhável passa a ser sempre do Google Maps (documentado e estável), enquanto o
    mapa EMBARCADO na aplicação continua desenhando a rota real (Leaflet) do provedor.

    Prioriza coordenadas exatas (origin=lat,lon) — apontam ao ponto certo e o Google
    traça a rota viária entre eles; com fallback para endereço oficial ou texto original.
    """
    orig = _montar_param_link_seguro(end_o, lat_o, lon_o, txt_o)
    dest = _montar_param_link_seguro(end_d, lat_d, lon_d, txt_d)
    return f"https://www.google.com/maps/dir/?api=1&origin={orig}&destination={dest}&travelmode=driving"

def _montar_embed_google(param_o, param_d):
    """[VIS-DINAMICA - 30ª geração] Monta a URL do MAPA EMBARCADO do Google (cenário em que
    o Google vence), recebendo os parâmetros de origem/destino JÁ montados (nome oficial
    qualificado, URL-encoded — os MESMOS usados no link de navegação, garantindo mapa=link).

    Dois caminhos, em ordem de robustez:
      1. Embed API oficial (/maps/embed/v1/directions) — SE houver chave configurada. É a
         forma SUPORTADA e garantida de traçar a rota do Google, com fit bounds e nomes.
      2. Embed clássico (?saddr&daddr&output=embed) COM NOMES — sem chave. Esse endpoint
         renderiza as DIREÇÕES (a rota desenhada) quando recebe NOMES (não coordenadas);
         é o mesmo que a versão antiga usava e que desenhava o trajeto corretamente.
    Em ambos, é um mapa do PRÓPRIO Google (nunca OSRM), com a rota traçada e nomes.
    """
    if GOOGLE_MAPS_EMBED_API_KEY:
        # [VIS-GOOGLE-EMBED - 32ª geração] units=metric (contexto Brasil). A doc oficial
        # (Maps Embed API, atualizada em 2026) confirma este como o caminho moderno/suportado.
        return (f"https://www.google.com/maps/embed/v1/directions?key={GOOGLE_MAPS_EMBED_API_KEY}"
                f"&origin={param_o}&destination={param_d}&mode=driving&units=metric")
    return f"https://maps.google.com/maps?saddr={param_o}&daddr={param_d}&output=embed"

def _montar_link_osrm_viewer(geometria_polyline, nome_o, nome_d, distancia_km, tempo_str):
    """[VIS-DINAMICA / VIS-OSRM-LINK - 30ª geração] Monta o link do VISUALIZADOR PRÓPRIO da
    rota OSRM — a solução robusta e auditável para "um link do OSRM que reproduza EXATAMENTE
    o mapa embarcado". É um link RELATIVO ao próprio app ("?rota=osrm&g=<polyline>&o=<nome>
    &d=<nome>&km=<>&t=<>"). Ao abri-lo, o app entra num modo visualizador que renderiza o
    MESMO mapa Leaflet, a partir da MESMA geometria (polyline codificada do OSRM) e dos
    MESMOS nomes — fidelidade total, servido pela própria aplicação, sem depender de
    serviços externos (geojson.io/map.project-osrm são frágeis e não-documentados).

    SALVAGUARDA DE TAMANHO: rotas muito longas geram polylines longas; se o link exceder um
    limite seguro de URL (~7,5k chars), retorna "" e a UI recai no DOWNLOAD do HTML (que tem
    fidelidade exata e funciona offline). Assim nunca se gera um link quebrado.
    """
    if not geometria_polyline:
        return ""
    g = urllib.parse.quote(geometria_polyline, safe="")
    o = urllib.parse.quote(str(nome_o or "Origem"), safe="")
    d = urllib.parse.quote(str(nome_d or "Destino"), safe="")
    km_q = urllib.parse.quote(str(distancia_km or ""), safe="")
    t = urllib.parse.quote(str(tempo_str or ""), safe="")
    link = f"?rota=osrm&g={g}&o={o}&d={d}&km={km_q}&t={t}"
    if len(link) > 7500:
        return ""  # URL longa demais → UI usa o download (fidelidade exata offline)
    return link

def _extrair_geometria_google(texto_resposta, lat_o, lon_o, lat_d, lon_d):
    """[VIS-GOOGLE-GEO - 27ª geração] Extrai e VALIDA a geometria (polyline) da rota a
    partir da resposta do endpoint de direções do Google. A resposta é ofuscada e não
    documentada, então em vez de confiar num delimitador fixo, testamos os candidatos a
    polyline e VALIDAMOS geograficamente: a polyline é aceita apenas se, ao decodificar,
    (a) tiver vários pontos, (b) começar perto da origem e terminar perto do destino, e
    (c) ficar dentro de uma caixa delimitadora plausível. Isso garante robustez: se a
    extração falhar ou vier lixo, retornamos "" e o mapa cai graciosamente nos marcadores.

    Retorna a string de polyline VÁLIDA (precisão 5) ou "" se nenhuma candidata passar.
    """
    if not texto_resposta or (lat_o == 0 and lon_o == 0):
        return ""

    def _valida(poly):
        try:
            pts = _decodificar_polyline(poly, precision=5)
        except Exception:
            return False
        if not pts or len(pts) < 2:
            return False
        # Tolerância: a rota deve começar perto da origem e terminar perto do destino.
        # 0.20° ~ 22km de folga (cobre o offset entre o centróide e o ponto exato da via).
        tol = 0.20
        p_ini, p_fim = pts[0], pts[-1]
        perto_ini = abs(p_ini[0] - lat_o) < tol and abs(p_ini[1] - lon_o) < tol
        perto_fim = abs(p_fim[0] - lat_d) < tol and abs(p_fim[1] - lon_d) < tol
        # Também aceita invertido (origem/destino trocados na geometria)
        perto_ini_inv = abs(p_ini[0] - lat_d) < tol and abs(p_ini[1] - lon_d) < tol
        perto_fim_inv = abs(p_fim[0] - lat_o) < tol and abs(p_fim[1] - lon_o) < tol
        if not ((perto_ini and perto_fim) or (perto_ini_inv and perto_fim_inv)):
            return False
        # Caixa delimitadora plausível: todos os pontos dentro do bounding box origem-destino
        # expandido por 1° (~111km), evitando aceitar polylines de outra região por acaso.
        lat_min, lat_max = min(lat_o, lat_d) - 1.0, max(lat_o, lat_d) + 1.0
        lon_min, lon_max = min(lon_o, lon_d) - 1.0, max(lon_o, lon_d) + 1.0
        for (la, lo) in pts:
            if not (lat_min <= la <= lat_max and lon_min <= lo <= lon_max):
                return False
        return True

    # Coleta candidatos de ambos os padrões, prioriza os mais longos (rotas têm muitos pontos)
    candidatos = set()
    for rgx in (_RE_GOOG_POLY1, _RE_GOOG_POLY2):
        try:
            for m in rgx.finditer(texto_resposta):
                cand = m.group(1)
                # Polyline não contém aspas nem vírgulas; filtro rápido de sanidade
                if cand and '"' not in cand and ',' not in cand:
                    candidatos.add(cand)
        except Exception:
            continue
    # Testa do mais longo para o mais curto (a geometria completa é a mais longa válida)
    for cand in sorted(candidatos, key=len, reverse=True):
        if _valida(cand):
            return cand
    return ""

def _gerar_mapa_rota_google(geometria_polyline, lat_o, lon_o, lat_d, lon_d, nome_origem="", nome_destino="", distancia_km="", tempo_str=""):
    """[VIS-GOOGLE-GEO - 27ª geração] Gera o mapa EMBARCADO da rota do GOOGLE desenhando o
    TRAÇADO COMPLETO (Leaflet + OpenStreetMap) a partir da polyline decodificada do Google.
    Resolve a PRIORIDADE MÁXIMA Nº 1: o mapa do Google mostrava só 2 marcadores; agora
    desenha a geometria integral da rota (curvas, conversões, segmentos), com nomes
    oficiais de origem/destino, distância e tempo. Se a geometria não estiver disponível,
    cai graciosamente para os marcadores (degradação segura). Reusa a infra de mapa do
    OSRM mas com rótulos por NOME (não coordenadas), conforme pedido."""
    return _gerar_mapa_leaflet_rota(geometria_polyline, lat_o, lon_o, lat_d, lon_d,
                                    nome_origem, nome_destino, distancia_km, tempo_str,
                                    provedor="Google Maps", cor="#1a73e8")

def _decodificar_polyline(polyline_str, precision=5):
    """[FIX-OSRM-GEO2 - 18ª geração] Decodifica uma polyline codificada (formato Google/
    OSRM) em uma lista de coordenadas [(lat, lon), ...]. O OSRM retorna a geometria da
    rota nesse formato compacto; precisamos decodificá-la para desenhar o traçado real
    no mapa. Implementação padrão do algoritmo de polyline encoding (sem dependências).
    """
    if not polyline_str:
        return []
    coordenadas = []
    index = 0
    lat = 0
    lng = 0
    fator = 10 ** precision
    comprimento = len(polyline_str)
    while index < comprimento:
        # Decodifica latitude
        resultado = 1
        shift = 0
        while True:
            b = ord(polyline_str[index]) - 63 - 1
            index += 1
            resultado += b << shift
            shift += 5
            if b < 0x1f:
                break
        lat += (~(resultado >> 1) if (resultado & 1) else (resultado >> 1))
        # Decodifica longitude
        resultado = 1
        shift = 0
        while True:
            b = ord(polyline_str[index]) - 63 - 1
            index += 1
            resultado += b << shift
            shift += 5
            if b < 0x1f:
                break
        lng += (~(resultado >> 1) if (resultado & 1) else (resultado >> 1))
        coordenadas.append((lat / fator, lng / fator))
    return coordenadas

def _gerar_mapa_leaflet_rota(geometria_polyline, lat_o, lon_o, lat_d, lon_d, nome_origem="", nome_destino="", distancia_km="", tempo_str="", provedor="OSRM", cor="#2563eb"):
    """[VIS-NAMES - 27ª geração] Gerador UNIFICADO de mapa de rota (Leaflet+OSM) que
    DESENHA o traçado completo a partir da polyline decodificada, com rótulos por NOME
    (origem/destino), distância, tempo e provedor — conforme o pedido de priorizar nomes
    em vez de coordenadas. Usado tanto pela rota do Google quanto pela do OSRM. Se a
    geometria estiver ausente, posiciona ao menos origem/destino (degradação graciosa).
    Retorna um data URI (HTML autocontido) — abre offline em qualquer navegador.
    """
    pontos = _decodificar_polyline(geometria_polyline) if geometria_polyline else []
    tem_geometria = len(pontos) >= 2
    if not tem_geometria:
        pontos = [(lat_o, lon_o), (lat_d, lon_d)]
    pontos_js = "[" + ",".join(f"[{la:.6f},{lo:.6f}]" for la, lo in pontos) + "]"
    # Rótulos por NOME (escapados para uso seguro em JS/HTML)
    _no = _escapar_js(nome_origem) if nome_origem else "Origem"
    _nd = _escapar_js(nome_destino) if nome_destino else "Destino"
    _aviso_geo = "" if tem_geometria else (
        '<div style="position:absolute;bottom:10px;left:10px;z-index:1000;background:#fff8e1;'
        'padding:4px 10px;border-radius:6px;font-family:system-ui;font-size:11px;color:#8a6d00">'
        '⚠️ Traçado indisponível — exibindo origem e destino</div>')
    badge_metricas = ""
    if distancia_km or tempo_str:
        badge_metricas = f' &nbsp; 📏 {_escapar_js(str(distancia_km))} &nbsp; ⏱️ {_escapar_js(str(tempo_str))}'
    info_badge = (f'<div style="position:absolute;top:10px;left:50px;right:10px;z-index:1000;background:#fff;'
                  f'padding:6px 12px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.2);'
                  f'font-family:system-ui,sans-serif;font-size:13px;max-width:90%">'
                  f'<b>{_escapar_js(provedor)}</b>{badge_metricas}<br>'
                  f'<span style="color:#16a34a">●</span> {_no} &nbsp;→&nbsp; '
                  f'<span style="color:#dc2626">●</span> {_nd}</div>')
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>html,body,#map{{height:100%;margin:0;padding:0}}#map{{width:100%;height:100%}}</style>
</head><body>{info_badge}{_aviso_geo}<div id="map"></div><script>
var pts={pontos_js};
var map=L.map('map');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{maxZoom:19,attribution:'© OpenStreetMap | Rota: {_escapar_js(provedor)}'}}).addTo(map);
var linha=L.polyline(pts,{{color:'{cor}',weight:5,opacity:0.85}}).addTo(map);
L.marker(pts[0]).addTo(map).bindPopup('<b>Origem:</b><br>{_no}').openPopup();
L.marker(pts[pts.length-1]).addTo(map).bindPopup('<b>Destino:</b><br>{_nd}');
map.fitBounds(linha.getBounds(),{{padding:[40,40]}});
</script></body></html>"""
    import base64 as _b64
    return "data:text/html;base64," + _b64.b64encode(html.encode("utf-8")).decode("ascii")

def _gerar_mapa_rota_osrm(geometria_polyline, lat_o, lon_o, lat_d, lon_d, distancia_km="", tempo_str="", nome_origem="", nome_destino=""):
    """[FIX-OSRM-GEO2] Mapa da rota OSRM com traçado completo. Agora delega ao gerador
    unificado, com rótulos por NOME (não coordenadas). Mantido para compatibilidade."""
    return _gerar_mapa_leaflet_rota(geometria_polyline, lat_o, lon_o, lat_d, lon_d,
                                    nome_origem, nome_destino, distancia_km, tempo_str,
                                    provedor="OSRM (menor distância)", cor="#2563eb")

def _montar_links_osrm(lat_o, lon_o, lat_d, lon_d, geometria_polyline="", distancia_km="", tempo_str=""):
    """[CONTINGENCIA-OSRM - 20ª geração] Gera o mapa EMBARCADO do OSRM (Leaflet+OSM) que
    desenha a polyline real da rota. Retorna (None, link_embed): o primeiro elemento é
    None porque o link COMPARTILHÁVEL não é mais gerado aqui — após investigação, concluiu-
    se que não há forma robusta/documentada de compartilhar a rota OSRM por link.

    HISTÓRICO da investigação (por que o link próprio do OSRM foi descontinuado):
      - map.project-osrm.org com waypoints loc=: dependia de serviço externo recalcular a
        rota; frequentemente mostrava só os pontos, sem o trajeto.
      - geojson.io com data: URI no fragmento: comportamento legado/não-documentado; o
        serviço truncava o data: URI e gerava o erro "String não finalizada em JSON na
        posição N" (os hex de cor #RRGGBB e a estrutura data:,<json> quebravam o parser).
    Ambos eram frágeis e dependiam de comportamento não-documentado. Conforme o plano de
    contingência, o link compartilhável passou a ser sempre do Google Maps (documentado,
    estável, sempre desenha a rota) — ver _montar_link_google_navegavel. O mapa EMBARCADO
    aqui continua mostrando o traçado real do OSRM (Leaflet), e há download do mapa HTML.
    """
    # Mapa embarcado que DESENHA a polyline real da rota OSRM (traçado exato, autocontido).
    link_embed_osm = _gerar_mapa_rota_osrm(geometria_polyline, lat_o, lon_o, lat_d, lon_d, distancia_km, tempo_str)
    return None, link_embed_osm

def _montar_auditoria_motores(origem_txt, destino_txt, end_of_o, end_of_d,
                              lat_o, lon_o, lat_d, lon_d,
                              fonte_geo_o, fonte_geo_d, score_o, score_d,
                              google_param_o, google_param_d, google_link,
                              km_google, km_osrm, vencedor,
                              osrm_snap=None, validacao_espacial=None, mitigacao_snap=None):
    """[AUDIT-MOTORES] Monta o rastro completo das consultas enviadas a cada motor de rota.
    Torna auditável e transparente que TODOS os motores partem da MESMA geocodificação
    validada: origem/destino são normalizados, validados na base nacional e convertidos numa
    ÚNICA representação (nome oficial + coordenada). O Google recebe o nome oficial (para
    desenhar a rota pelos nomes); o OSRM recebe a coordenada validada. Ambos derivam do mesmo
    resultado — o que este rastro evidencia campo a campo. Inclui também o SNAP do OSRM
    (coordenada projetada na via + distância do snap) e a VALIDAÇÃO ESPACIAL da rota."""
    try:
        norm_o = semantica.normalizar(origem_txt)
        norm_d = semantica.normalizar(destino_txt)
    except Exception:
        norm_o, norm_d = origem_txt, destino_txt
    osrm_url = ""
    try:
        if lat_o and lon_o and lat_d and lon_d and lat_o != 0.0 and lat_d != 0.0:
            osrm_url = (f"http://router.project-osrm.org/route/v1/driving/"
                        f"{lon_o},{lat_o};{lon_d},{lat_d}?overview=full&geometries=polyline&steps=true&alternatives=3")
    except Exception:
        osrm_url = ""
    div_abs = div_pct = None
    if km_google and km_osrm and km_google > 0 and km_osrm > 0:
        div_abs = round(abs(km_google - km_osrm), 2)
        div_pct = round((div_abs / max(km_google, km_osrm)) * 100, 1)
    # Bloco de snap do OSRM: coordenada enviada → coordenada usada (após snap) → distância do snap
    osrm_bloco = {
        "origem_enviada": f"{lat_o}, {lon_o}", "destino_enviada": f"{lat_d}, {lon_d}",
        "url": osrm_url, "distancia_km": km_osrm, "tipo_entrada": "Coordenada validada (mesmo geocode)",
    }
    if osrm_snap:
        osrm_bloco["origem_usada_pos_snap"] = f"{osrm_snap.get('orig_snap_lat')}, {osrm_snap.get('orig_snap_lon')}"
        osrm_bloco["destino_usada_pos_snap"] = f"{osrm_snap.get('dest_snap_lat')}, {osrm_snap.get('dest_snap_lon')}"
        osrm_bloco["origem_snap_dist_m"] = osrm_snap.get("orig_snap_dist_m")
        osrm_bloco["destino_snap_dist_m"] = osrm_snap.get("dest_snap_dist_m")
    return {
        "origem": {
            "texto_original": origem_txt, "normalizado": norm_o, "validado_oficial": end_of_o,
            "coordenada": f"{lat_o}, {lon_o}", "fonte_geocodificacao": fonte_geo_o, "score_confianca": score_o,
        },
        "destino": {
            "texto_original": destino_txt, "normalizado": norm_d, "validado_oficial": end_of_d,
            "coordenada": f"{lat_d}, {lon_d}", "fonte_geocodificacao": fonte_geo_d, "score_confianca": score_d,
        },
        "google_maps": {
            "origem_enviada": google_param_o, "destino_enviada": google_param_d,
            "url": google_link, "distancia_km": km_google, "tipo_entrada": "Nome oficial validado",
        },
        "osrm": osrm_bloco,
        "validacao_espacial": validacao_espacial,
        "mitigacao_snap": mitigacao_snap,
        "consenso": {"vencedor": vencedor, "divergencia_km": div_abs, "divergencia_pct": div_pct},
    }


def calcular_pipeline_logistico(origem, destino, perfil_rota="shortest"):
    start_total = time.time()
    origem_clean, destino_clean = str(origem).strip(), str(destino).strip()
    chave_rota_cache = f"ROTA_{CACHE_VERSION}_{semantica.normalizar(origem_clean)}->{semantica.normalizar(destino_clean)}"
    
    # 1. Tenta L1 (RAM Instantânea)
    if chave_rota_cache in CACHE_L1_ROTAS:
        ret_cache = CACHE_L1_ROTAS[chave_rota_cache]
    # 2. Tenta L2 (DiskCache)
    elif chave_rota_cache in cache_rotas:
        ret_cache = cache_rotas[chave_rota_cache]
        CACHE_L1_ROTAS[chave_rota_cache] = ret_cache # Preenche L1
    else:
        ret_cache = None
        
    if ret_cache is not None:
        # [M11] Suporte a cache legado (tuplas) e novo formato (RotaPipeline)
        if isinstance(ret_cache, RotaPipeline):
            # Verifica cache poisoning no formato NamedTuple
            if ret_cache.dist_linha_reta == 0.0 and ret_cache.lat_origem != 0.0 and ret_cache.lat_destino != 0.0:
                if ret_cache.lat_origem != ret_cache.lat_destino or ret_cache.lon_origem != ret_cache.lon_destino:
                    _incrementar_metrica("cache_unpoisoned")
                    nova_dist, novo_status = calcular_distancia_linha_reta(
                        ret_cache.lat_origem, ret_cache.lon_origem,
                        ret_cache.lat_destino, ret_cache.lon_destino, contexto="Unpoisoning NamedTuple"
                    )
                    ret_novo = ret_cache._replace(dist_linha_reta=nova_dist, status_linha_reta=novo_status)
                    _cache_set_seguro(cache_rotas, chave_rota_cache, ret_novo, expire=2592000)
                    CACHE_L1_ROTAS[chave_rota_cache] = ret_novo
                    return ret_novo
            return ret_cache
        elif len(ret_cache) >= 30:
            # Cache legado em formato de tupla — compatibilidade retroativa
            dist_cache = ret_cache[4]
            lat_o_cache, lon_o_cache = ret_cache[19], ret_cache[20]
            lat_d_cache, lon_d_cache = ret_cache[21], ret_cache[22]
            if dist_cache == 0.0 and lat_o_cache != 0.0 and lat_d_cache != 0.0 and (lat_o_cache != lat_d_cache or lon_o_cache != lon_d_cache):
                global METRICAS_DISTANCIA
                _incrementar_metrica("cache_unpoisoned")
                nova_dist, novo_status = calcular_distancia_linha_reta(lat_o_cache, lon_o_cache, lat_d_cache, lon_d_cache, contexto="Unpoisoning de Cache")
                retorno_mutavel = list(ret_cache)
                retorno_mutavel[4] = nova_dist
                if len(retorno_mutavel) == 30:
                    retorno_mutavel.append(novo_status)
                else:
                    retorno_mutavel[30] = novo_status
                retorno_novo = tuple(retorno_mutavel)
                _cache_set_seguro(cache_rotas, chave_rota_cache, retorno_novo, expire=2592000)
                CACHE_L1_ROTAS[chave_rota_cache] = retorno_novo
                return retorno_novo
            if len(ret_cache) == 30:
                return (*ret_cache, "Calculada via Cache Hit Estável")
            return ret_cache
            
    start_geo = time.time()
    lat_o, lon_o, end_oficial_o, conf_o, score_num_o, dist_o, mun_o, fonte_geo_o, xai_o = obter_coordenadas_e_endereco_oficial(origem_clean)
    lat_d, lon_d, end_oficial_d, conf_d, score_num_d, dist_d, mun_d, fonte_geo_d, xai_d = obter_coordenadas_e_endereco_oficial(destino_clean)
    
    # BARREIRA TOPOLÓGICA DE COLISÃO E DESAMBIGUAÇÃO ESTRITA
    if lat_o == lat_d and lon_o == lon_d and lat_o != 0.0:
        if semantica.normalizar(origem_clean) != semantica.normalizar(destino_clean):
            _incrementar_metrica("desambiguacoes_estritas")
            logger.warning(f"Colisão de Centróide Detectada: '{origem_clean}' e '{destino_clean}' reduzidos ao mesmo ponto. Forçando regeocodificação hierárquica.")
            res_o = forcar_geocodificacao_hierarquica_estrita(origem_clean)
            if res_o: lat_o, lon_o, end_oficial_o, conf_o, score_num_o, dist_o, mun_o, fonte_geo_o, xai_o = res_o
            res_d = forcar_geocodificacao_hierarquica_estrita(destino_clean)
            if res_d: lat_d, lon_d, end_oficial_d, conf_d, score_num_d, dist_d, mun_d, fonte_geo_d, xai_d = res_d
            
    tempo_geocoding = round(time.time() - start_geo, 2)
    start_rot = time.time()
    
    if all([lat_o is not None, lon_o is not None, lat_d is not None, lon_d is not None]) and lat_o != 0.0 and lat_d != 0.0:
        dist_linha_reta, status_linha_reta = calcular_distancia_linha_reta(lat_o, lon_o, lat_d, lon_d, contexto=f"Pipeline Principal: {origem_clean} a {destino_clean}")
    else:
        dist_linha_reta = 0.0
        status_linha_reta = "Falha de Geocodificação (Coordenadas Nulas)"
        
    # [FIX-MUN-LINK - 23ª geração] Parâmetros do link PRIORIZANDO MUNICÍPIO.
    # Extrai a UF resolvida (do endereço oficial, com fallback no texto do usuário) e
    # monta os parâmetros de origem/destino forçando o município quando a entrada é uma
    # cidade — impedindo que o Google escolha um POI/endereço específico (causa do bug
    # "Corumbá, GO" → "R. Francisco Miranda, 466"). Para endereços reais, mantém a
    # blindagem anterior. Estes parâmetros alimentam tanto o link quanto o mapa.
    _uf_o = extrair_uf_precisa(end_oficial_o)
    if _uf_o == "Indefinido":
        _uf_o = extrair_uf_precisa(origem_clean)
    _uf_d = extrair_uf_precisa(end_oficial_d)
    if _uf_d == "Indefinido":
        _uf_d = extrair_uf_precisa(destino_clean)
    _uf_o = "" if _uf_o == "Indefinido" else _uf_o
    _uf_d = "" if _uf_d == "Indefinido" else _uf_d
    orig_param_fb = _montar_param_municipio_google(origem_clean, mun_o, _uf_o, end_oficial_o, lat_o, lon_o)
    dest_param_fb = _montar_param_municipio_google(destino_clean, mun_d, _uf_d, end_oficial_d, lat_d, lon_d)
    link_fallback = f"https://www.google.com/maps/dir/?api=1&origin={orig_param_fb}&destination={dest_param_fb}&travelmode=driving"
    link_embed_fallback = f"https://maps.google.com/maps?saddr={orig_param_fb}&daddr={dest_param_fb}&output=embed"
    
    # [PERF-3] Google (scraper) e OSRM rodam em sequência. Os caches L1/L2 mitigam a
    # latência (cache-hit não chama rede). Mantemos serial para não saturar o pool.
    res_google = None
    res_google = extrair_dados_reais_google(end_oficial_o, end_oficial_d, lat_o, lon_o, lat_d, lon_d, dist_linha_reta, usar_coordenadas=True, link_maps_pronto=link_fallback, link_embed_pronto=link_embed_fallback)
    
    if not res_google:
        res_google = extrair_dados_reais_google(origem_clean, destino_clean, lat_o, lon_o, lat_d, lon_d, dist_linha_reta, usar_coordenadas=False, link_maps_pronto=link_fallback, link_embed_pronto=link_embed_fallback)
    
    # [ARQ-HIBRIDO - 26ª geração] Consulta o OSRM em paralelo conceitual (sequencial real).
    res_osrm = None
    if lat_o != 0.0 and lat_d != 0.0:
        res_osrm = API_OSRM_Routing(lat_o, lon_o, lat_d, lon_d)

    # [OSRM-SNAP + VALID-ESPACIAL - 40ª geração] Extrai o snap do OSRM (coordenada projetada na
    # via + distância do snap) e faz a VALIDAÇÃO ESPACIAL: confere se os pontos snapados continuam
    # dentro dos limites (bounding box) da UF esperada e se o snap não foi grosseiro. É a evidência
    # da causa raiz E o mecanismo de "só aceitar rota com confiança mínima": se o snap jogou a
    # origem/destino para FORA da UF pedida (erro real, não snap normal), o OSRM não pode vencer.
    _osrm_snap = res_osrm[5] if (res_osrm and len(res_osrm) > 5) else None

    # [SNAP-MITIGA - 41ª geração] MITIGAÇÃO DE SNAP EXCESSIVO. Se o OSRM projetou origem/destino
    # longe da via (deslocamento > 1500 m), busca uma coordenada road-adjacent mais representativa
    # (candidatos de vários geocoders, escolhido o de MENOR snap dentro da UF via /nearest) e
    # RE-ROTEIA o OSRM com ela. Só dispara em snap grande (raro) e é memoizado por município →
    # custo amortizado no lote. A coordenada VALIDADA (canônica) NÃO muda; a road-adjacent é usada
    # SOMENTE para o /route do OSRM, reduzindo o deslocamento e a inflação da rota.
    mitigacao_snap = None
    if _osrm_snap and res_osrm:
        _LIMIAR_MITIGA_M = 1500.0
        _o_snap0 = _osrm_snap.get("orig_snap_dist_m", 0.0) or 0.0
        _d_snap0 = _osrm_snap.get("dest_snap_dist_m", 0.0) or 0.0
        if _o_snap0 > _LIMIAR_MITIGA_M or _d_snap0 > _LIMIAR_MITIGA_M:
            _lat_o_os, _lon_o_os, _lat_d_os, _lon_d_os = lat_o, lon_o, lat_d, lon_d
            _cand_o = _cand_d = None
            _melhora_o = _melhora_d = False
            if _o_snap0 > _LIMIAR_MITIGA_M:
                _lat_o_os, _lon_o_os, _snap_o_novo, _cand_o, _melhora_o = _melhor_coordenada_para_osrm(
                    origem_clean, mun_o, _uf_o, lat_o, lon_o, _o_snap0)
            if _d_snap0 > _LIMIAR_MITIGA_M:
                _lat_d_os, _lon_d_os, _snap_d_novo, _cand_d, _melhora_d = _melhor_coordenada_para_osrm(
                    destino_clean, mun_d, _uf_d, lat_d, lon_d, _d_snap0)
            if _melhora_o or _melhora_d:
                _res_osrm_mit = API_OSRM_Routing(_lat_o_os, _lon_o_os, _lat_d_os, _lon_d_os)
                if _res_osrm_mit:
                    _snap_mit = _res_osrm_mit[5] if len(_res_osrm_mit) > 5 else None
                    mitigacao_snap = {
                        "aplicada": True, "origem_melhorada": bool(_melhora_o), "destino_melhorado": bool(_melhora_d),
                        "snap_origem_antes_m": _o_snap0, "snap_destino_antes_m": _d_snap0,
                        "snap_origem_depois_m": (_snap_mit.get("orig_snap_dist_m") if _snap_mit else None),
                        "snap_destino_depois_m": (_snap_mit.get("dest_snap_dist_m") if _snap_mit else None),
                        "km_antes": res_osrm[0], "km_depois": _res_osrm_mit[0],
                        "coord_osrm_origem": f"{_lat_o_os}, {_lon_o_os}", "coord_osrm_destino": f"{_lat_d_os}, {_lon_d_os}",
                        "candidatos_origem": _cand_o, "candidatos_destino": _cand_d,
                    }
                    res_osrm = _res_osrm_mit       # adota a rota mitigada do OSRM
                    _osrm_snap = _snap_mit
            else:
                mitigacao_snap = {
                    "aplicada": False,
                    "motivo": "Nenhum candidato road-adjacent superou o ponto atual dentro da UF (malha OSM esparsa).",
                    "snap_origem_antes_m": _o_snap0, "snap_destino_antes_m": _d_snap0,
                    "candidatos_origem": _cand_o, "candidatos_destino": _cand_d,
                }

    # [OSRM-SNAP + VALID-ESPACIAL - 40ª geração] Extrai o snap do OSRM (coordenada projetada na
    # via + distância do snap) e faz a VALIDAÇÃO ESPACIAL: confere se os pontos snapados continuam
    # dentro dos limites (bounding box) da UF esperada e se o snap não foi grosseiro. É a evidência
    # da causa raiz E o mecanismo de "só aceitar rota com confiança mínima": se o snap jogou a
    # origem/destino para FORA da UF pedida (erro real, não snap normal), o OSRM não pode vencer.
    validacao_espacial = None
    osrm_invalido_uf = False
    if _osrm_snap:
        _LIMIAR_SNAP_M = 3000.0

        def _dentro_uf(_uf, _lat, _lon):
            _box = BOUNDING_BOXES_UF.get(_uf) if _uf else None
            if not _box or _lat is None or _lon is None:
                return None  # sem UF/box → não valida (evita falso alerta)
            return (_box["lat_min"] <= _lat <= _box["lat_max"] and _box["lon_min"] <= _lon <= _box["lon_max"])

        _o_dentro = _dentro_uf(_uf_o, _osrm_snap.get("orig_snap_lat"), _osrm_snap.get("orig_snap_lon"))
        _d_dentro = _dentro_uf(_uf_d, _osrm_snap.get("dest_snap_lat"), _osrm_snap.get("dest_snap_lon"))
        _o_snap_m = _osrm_snap.get("orig_snap_dist_m", 0.0)
        _d_snap_m = _osrm_snap.get("dest_snap_dist_m", 0.0)
        _alertas = []
        if _o_snap_m > _LIMIAR_SNAP_M:
            _alertas.append(f"Origem: snap de {_o_snap_m:.0f} m até a via mais próxima (malha OSM esparsa na região).")
        if _d_snap_m > _LIMIAR_SNAP_M:
            _alertas.append(f"Destino: snap de {_d_snap_m:.0f} m até a via mais próxima (malha OSM esparsa na região).")
        if _o_dentro is False:
            _alertas.append(f"Origem snapada FORA dos limites de {_uf_o} — ponto viário deslocado para outra área.")
            osrm_invalido_uf = True
        if _d_dentro is False:
            _alertas.append(f"Destino snapado FORA dos limites de {_uf_d} — ponto viário deslocado para outra área.")
            osrm_invalido_uf = True
        validacao_espacial = {
            "origem_dentro_uf": _o_dentro, "destino_dentro_uf": _d_dentro,
            "origem_snap_m": _o_snap_m, "destino_snap_m": _d_snap_m,
            "limiar_snap_m": _LIMIAR_SNAP_M, "alertas": _alertas,
        }

    if res_google or res_osrm:
        # ======================================================================
        # [ARQ-HIBRIDO - 26ª geração] ARQUITETURA HÍBRIDA REESTRUTURADA: GOOGLE + OSRM
        # COM SELEÇÃO AUTOMÁTICA DE MENOR DISTÂNCIA E AUDITABILIDADE MÁXIMA.
        # ----------------------------------------------------------------------
        # Os DOIS motores são executados sempre que possível. A aplicação compara as
        # distâncias e seleciona a MENOR como vencedora (regra de negócio do usuário).
        #
        # Cada cenário entrega o conjunto COMPLETO do vencedor + um comparativo rico:
        #   - GOOGLE vence  → distância/tempo/mapa/link do Google + OSRM no comparativo.
        #   - OSRM vence    → distância/tempo do OSRM + MAPA que desenha a geometria EXATA
        #                     do OSRM (Leaflet, traçado fiel) + link navegável + download
        #                     do mapa HTML autocontido (rota OSRM exata, offline, auditável)
        #                     + Google no comparativo (diferença abs/%/tempo, selo, etc.).
        #
        # TOLERÂNCIA (2%): quando as distâncias são praticamente iguais, prefere-se o
        # Google (que tem link de navegação 100% auditável), evitando alternância sem
        # ganho real. Acima da tolerância, a menor distância vence sempre.
        #
        # LINK DA ROTA OSRM (investigação completa — ver _montar_links_osrm): não há forma
        # robusta/documentada de um link COMPARTILHÁVEL público que abra a geometria exata
        # do OSRM (geojson.io/map.project-osrm são frágeis). A solução robusta e auditável
        # adotada: (1) mapa embarcado Leaflet desenha a geometria exata; (2) DOWNLOAD de um
        # HTML autocontido com a rota OSRM exata (abre offline em qualquer navegador, sem
        # depender de serviço externo); (3) link de navegação via Google (sempre traça).
        # ======================================================================
        comparativo_prov = ""
        link_osrm_viewer = ""  # [VIS-DINAMICA] default; preenchido só quando o OSRM vence
        link_embed_comparativo = ""  # [VIS-DUAL] mapa do provedor comparativo (não-vencedor)
        link_rota_comparativo = ""   # [VIS-DUAL] link do provedor comparativo
        km_g = res_google[0] if res_google else None
        km_o = res_osrm[0] if res_osrm else None
        n_alt_osrm = (res_osrm[3] if res_osrm and len(res_osrm) > 3 else 1)
        
        # Decide o vencedor pela MENOR distância (com tolerância de 2% a favor do Google).
        # [VALID-ESPACIAL] Guard: se a validação espacial reprovou o OSRM (snap jogou origem/
        # destino para FORA da UF pedida — erro objetivo), ele NÃO vence o Google. Isso NÃO altera
        # a regra "menor distância": apenas rejeita um resultado do OSRM comprovadamente inválido,
        # atendendo ao pedido de "só aceitar a rota com nível mínimo de confiança". Quando o Google
        # está indisponível, o OSRM ainda é usado (melhor que nada), porém com o alerta registrado.
        osrm_vence = False
        if res_google and res_osrm:
            osrm_vence = (km_o < km_g * 0.98) and not osrm_invalido_uf
            _tempo_osrm_str = f"{res_osrm[1]} min" if res_osrm[1] < 60 else f"{res_osrm[1] // 60} h {res_osrm[1] % 60} min"
            comparativo_prov = _montar_comparativo_provedores(
                km_g, res_google[1], km_o, _tempo_osrm_str, "OSRM" if osrm_vence else "Google")
        
        if res_osrm and (osrm_vence or not res_google):
            # ---------------- OSRM É O VENCEDOR (ou Google indisponível) ----------------
            km_rota = km_o
            tempo_m = res_osrm[1]
            tempo_rota = f"{tempo_m} min" if tempo_m < 60 else f"{tempo_m // 60} h {tempo_m % 60} min"
            _geo_osrm = res_osrm[4] if len(res_osrm) > 4 else ""
            balsa_rota = res_osrm[2]
            score_rota = 88  # OSRM não fornece score próprio; valor fixo (idx 5 agora é snap_info)
            # [VIS-NAMES] Mapa EMBARCADO desenha a geometria EXATA do OSRM com rótulos por
            # NOME oficial (origem/destino), não coordenadas — conforme o pedido.
            link_embed = _gerar_mapa_rota_osrm(_geo_osrm, lat_o, lon_o, lat_d, lon_d,
                                               f"{km_rota} km", tempo_rota,
                                               nome_origem=end_oficial_o, nome_destino=end_oficial_d)
            # [VIS-DINAMICA - 30ª geração] LINK 2 (OSRM): visualizador PRÓPRIO que reproduz
            # EXATAMENTE este mapa embarcado (mesma geometria, mesmos nomes), servido pelo
            # próprio app via "?rota=osrm&...". É a solução robusta/auditável para o link OSRM.
            link_osrm_viewer = _montar_link_osrm_viewer(_geo_osrm, end_oficial_o, end_oficial_d,
                                                        f"{km_rota} km", tempo_rota)
            # [VIS-DINAMICA] LINK 1 (Google): referência comparativa — sempre traça a rota.
            link_rota = _montar_link_google_navegavel(lat_o, lon_o, lat_d, lon_d, end_oficial_o, end_oficial_d, origem_clean, destino_clean)
            fonte_rota = "OSRM (Menor Distância)"
            # [VIS-DUAL - 37ª geração] Mapa + link do GOOGLE como COMPARATIVO (o Google já foi
            # medido nesta execução). Usa os MESMOS parâmetros por NOME do link de navegação →
            # o mapa comparativo e o link comparativo representam a mesma rota do Google.
            if res_google:
                _param_o_gc = _montar_param_municipio_google(origem_clean, mun_o, _uf_o, end_oficial_o, lat_o, lon_o)
                _param_d_gc = _montar_param_municipio_google(destino_clean, mun_d, _uf_d, end_oficial_d, lat_d, lon_d)
                link_embed_comparativo = _montar_embed_google(_param_o_gc, _param_d_gc)
                link_rota_comparativo = link_rota  # link Google navegável (comparativo)
            if res_google:
                _delta = km_g - km_o
                motivo_roteamento = (f"OSRM venceu com a MENOR distância: {km_o}km contra {km_g}km do Google "
                                     f"(~{_delta:.1f}km a menos, entre {n_alt_osrm} alternativa(s) viária(s) avaliadas). "
                                     f"Mapa embarcado EXCLUSIVAMENTE do OSRM (geometria exata, nomes). Dois links: Google "
                                     f"Maps (comparação) e o visualizador OSRM que reproduz fielmente este mesmo mapa.")
            else:
                motivo_roteamento = (f"Distância e tempo via malha OSRM ({km_o}km): o Google Maps não respondeu para "
                                     f"medição. Mapa embarcado do OSRM (geometria exata, nomes) + link do visualizador OSRM.")
        else:
            # ---------------- GOOGLE É O VENCEDOR ----------------
            km_rota = res_google[0]
            tempo_rota = res_google[1]
            balsa_rota = res_google[3]
            score_rota = res_google[4]
            # [VIS-DINAMICA - 30ª geração] CENÁRIO 1 — GOOGLE VENCE: o mapa embarcado é
            # EXCLUSIVAMENTE do Google (NUNCA OSRM) e há UM ÚNICO link (Google). O mapa e o
            # link são construídos a partir dos MESMOS parâmetros (nome oficial qualificado),
            # garantindo "mapa = link" — representam exatamente a mesma rota.
            #
            # MUDANÇA-CHAVE (corrige o "mapa sempre OSRM"): a versão anterior, quando a
            # extração da polyline do Google falhava, caía na geometria do OSRM como traçado
            # de referência — era ISSO que fazia o mapa parecer "sempre OSRM". Removido. Agora
            # o mapa do cenário Google é um embed do PRÓPRIO Google (Embed API se houver chave,
            # senão ?saddr&daddr&output=embed COM NOMES, que desenha as direções).
            # [FIX-MUN-EMBED - 35ª geração] Usa o MESMO builder do link de fallback
            # (_montar_param_municipio_google), que para municípios devolve o NOME oficial
            # totalmente qualificado ("Município, Estado, Brasil") e, para os demais, delega
            # ao builder blindado (que agora prefere o nome ao invés de coordenadas). Antes,
            # este ramo chamava _montar_param_link_seguro diretamente — inconsistente com o
            # fallback e suscetível a devolver coordenadas. Agora mapa e link do Google saem
            # exatamente dos MESMOS parâmetros por NOME, reproduzindo o comportamento do
            # script antigo (que sempre desenhava a rota com nomes).
            _param_o_g = _montar_param_municipio_google(origem_clean, mun_o, _uf_o, end_oficial_o, lat_o, lon_o)
            _param_d_g = _montar_param_municipio_google(destino_clean, mun_d, _uf_d, end_oficial_d, lat_d, lon_d)
            link_rota = f"https://www.google.com/maps/dir/?api=1&origin={_param_o_g}&destination={_param_d_g}&travelmode=driving"
            link_embed = _montar_embed_google(_param_o_g, _param_d_g)  # mapa do PRÓPRIO Google (http)
            # [VIS-DUAL - 37ª geração] Mapa + link do OSRM como COMPARATIVO (o OSRM já foi medido;
            # aproveitamos sua GEOMETRIA EXATA). Também popula link_osrm_viewer (agora comparativo).
            if res_osrm:
                _geo_osrm_c = res_osrm[4] if len(res_osrm) > 4 else ""
                _tempo_osrm_c = f"{res_osrm[1]} min" if res_osrm[1] < 60 else f"{res_osrm[1] // 60} h {res_osrm[1] % 60} min"
                link_embed_comparativo = _gerar_mapa_rota_osrm(_geo_osrm_c, lat_o, lon_o, lat_d, lon_d,
                                                               f"{km_o} km", _tempo_osrm_c,
                                                               nome_origem=end_oficial_o, nome_destino=end_oficial_d)
                link_rota_comparativo = _montar_link_osrm_viewer(_geo_osrm_c, end_oficial_o, end_oficial_d, f"{km_o} km", _tempo_osrm_c)
                link_osrm_viewer = link_rota_comparativo
            else:
                link_osrm_viewer = ""  # Google vence e OSRM indisponível → sem comparativo
            fonte_rota = "Google Maps"
            if res_osrm:
                motivo_roteamento = (f"Google Maps venceu com a menor distância (ou empate técnico ≤2%): {km_g}km "
                                     f"contra {km_o}km do OSRM. Mapa embarcado EXCLUSIVAMENTE do Google (rota traçada, "
                                     f"nomes) e UM ÚNICO link (Google) — mapa e link são a MESMA rota, 100% auditáveis. "
                                     f"O OSRM aparece apenas no comparativo, para transparência.")
            else:
                motivo_roteamento = (f"Rota oficial do Google Maps: {km_rota}km. Mapa embarcado e link são ambos do "
                                     f"Google e auditáveis — abrem exatamente esta rota traçada pelos nomes das "
                                     f"localidades. (OSRM indisponível para comparação nesta execução.)")
            
        tempo_roteamento = round(time.time() - start_rot, 2)
        tempo_total = round(time.time() - start_total, 2)
        # [AUDIT-MOTORES] Rastro das consultas aos motores (mesma geocodificação validada p/ todos)
        auditoria_motores = _montar_auditoria_motores(
            origem_clean, destino_clean, end_oficial_o, end_oficial_d,
            lat_o, lon_o, lat_d, lon_d, fonte_geo_o, fonte_geo_d, score_num_o, score_num_d,
            orig_param_fb, dest_param_fb, link_rota, km_g, km_o, fonte_rota,
            osrm_snap=_osrm_snap, validacao_espacial=validacao_espacial, mitigacao_snap=mitigacao_snap)
        # [M11] RotaPipeline NamedTuple — acesso por nome elimina bugs de índice
        retorno = RotaPipeline(
            distancia=km_rota, tempo=tempo_rota, link_rota=link_rota, balsas=balsa_rota,
            dist_linha_reta=dist_linha_reta, fonte_rota=fonte_rota, score_rota=score_rota,
            confianca_origem=conf_o, score_num_origem=score_num_o, distrito_origem=dist_o,
            municipio_origem=mun_o, fonte_geo_origem=fonte_geo_o, endereco_oficial_origem=end_oficial_o,
            confianca_destino=conf_d, score_num_destino=score_num_d, distrito_destino=dist_d,
            municipio_destino=mun_d, fonte_geo_destino=fonte_geo_d, endereco_oficial_destino=end_oficial_d,
            lat_origem=lat_o, lon_origem=lon_o, lat_destino=lat_d, lon_destino=lon_d,
            tempo_geocoding=tempo_geocoding, tempo_roteamento=tempo_roteamento, tempo_total=tempo_total,
            xai_origem=xai_o, xai_destino=xai_d, motivo_roteamento=motivo_roteamento,
            link_embed=link_embed, status_linha_reta=status_linha_reta,
            comparativo_provedores=comparativo_prov,
            link_osrm_viewer=link_osrm_viewer,
            link_embed_comparativo=link_embed_comparativo,
            link_rota_comparativo=link_rota_comparativo,
            auditoria_motores=auditoria_motores
        )
        CACHE_L1_ROTAS[chave_rota_cache] = retorno
        _cache_set_seguro(cache_rotas, chave_rota_cache, retorno, expire=2592000)
        return retorno
        
    km_terrestre = round(dist_linha_reta * obter_fator_desvio_rodoviario(dist_linha_reta), 2)
    v_comercial = 45.0 if km_terrestre < 50.0 else 65.0
    minutos_est = round((km_terrestre / v_comercial) * 60) if km_terrestre > 0 else 0
    tempo_geo_str = f"{minutos_est} min" if minutos_est < 60 else f"{minutos_est // 60} h {minutos_est % 60} min"
    tempo_roteamento = round(time.time() - start_rot, 2)
    tempo_total = round(time.time() - start_total, 2)
    motivo_fallback = "Alerta: o Google Maps não retornou a rota (timeout ou coordenadas inválidas). Projeção Geodésica Adaptativa acionada — distância estimada pela linha reta × fator de desvio rodoviário. Reprocesse para obter o valor viário oficial do Google quando o serviço responder."
    # [VIS-ALWAYS-DRAW] Mesmo no fallback geodésico, desenha um mapa Leaflet com a ligação
    # direta origem→destino e rótulos por NOME (melhor que o embed clássico de coordenadas).
    # Sem geometria viária, a linha reta entre os pontos é a representação honesta da estimativa.
    if lat_o != 0.0 and lat_d != 0.0:
        link_embed_geodesico = _gerar_mapa_leaflet_rota("", lat_o, lon_o, lat_d, lon_d,
                                                        end_oficial_o, end_oficial_d,
                                                        f"~{km_terrestre} km (estimado)", tempo_geo_str,
                                                        provedor="Projeção Geodésica (estimativa)", cor="#ea8600")
    else:
        link_embed_geodesico = link_embed_fallback
    # [AUDIT-MOTORES] Rastro também no fallback geodésico (motores de rota indisponíveis nesta execução)
    auditoria_motores = _montar_auditoria_motores(
        origem_clean, destino_clean, end_oficial_o, end_oficial_d,
        lat_o, lon_o, lat_d, lon_d, fonte_geo_o, fonte_geo_d, score_num_o, score_num_d,
        orig_param_fb, dest_param_fb, link_fallback, None, None, "Geodésico Adaptativo",
        osrm_snap=_osrm_snap, validacao_espacial=validacao_espacial, mitigacao_snap=mitigacao_snap)
    retorno = RotaPipeline(
        distancia=km_terrestre, tempo=tempo_geo_str, link_rota=link_fallback, balsas="Não",
        dist_linha_reta=dist_linha_reta, fonte_rota="Geodésico Adaptativo", score_rota=50,
        confianca_origem=conf_o, score_num_origem=score_num_o, distrito_origem=dist_o,
        municipio_origem=mun_o, fonte_geo_origem=fonte_geo_o, endereco_oficial_origem=end_oficial_o,
        confianca_destino=conf_d, score_num_destino=score_num_d, distrito_destino=dist_d,
        municipio_destino=mun_d, fonte_geo_destino=fonte_geo_d, endereco_oficial_destino=end_oficial_d,
        lat_origem=lat_o, lon_origem=lon_o, lat_destino=lat_d, lon_destino=lon_d,
        tempo_geocoding=tempo_geocoding, tempo_roteamento=tempo_roteamento, tempo_total=tempo_total,
        xai_origem=xai_o, xai_destino=xai_d, motivo_roteamento=motivo_fallback,
        link_embed=link_embed_geodesico, status_linha_reta=status_linha_reta,
        auditoria_motores=auditoria_motores
    )
    CACHE_L1_ROTAS[chave_rota_cache] = retorno
    _cache_set_seguro(cache_rotas, chave_rota_cache, retorno, expire=2592000)
    return retorno

def executar_pipeline_unificado(origem_cru, destino_cru, runner_up_info=None):
    orig = str(origem_cru).strip() if pd.notna(origem_cru) else ""
    dest = str(destino_cru).strip() if pd.notna(destino_cru) else ""
    concorrente = "N/A"
    dist_conc = 0.0
    link_conc = "N/A"
    justificativa = "N/A"
    
    if orig == "FALHA_GEO_ORIGEM" or dest == "NENHUM_HUB_VALIDO":
        return (0.0, "0 min", "Link Indisponível", "Não", 0.0, "Input Inválido", 0, "BAIXA", 0, "Não Informado", "Não Informado", "N/A", orig, "BAIXA", 0, "Não Informado", "Não Informado", "N/A", dest, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ["Falha Espacial Origem"], ["Falha Espacial Destino"], "Falha de Roteamento: Hub Base ou Endereço Destino foi incapaz de resolver latitude/longitude em nuvem.", "N/A", "Falha Operacional (Input Inválido)", concorrente, dist_conc, link_conc, justificativa)
        
    if orig.lower() in ['nan', 'none', 'null', ''] or dest.lower() in ['nan', 'none', 'null', '']:
        return (0.0, "0 min", "Link Indisponível", "Não", 0.0, "Input Inválido", 0, "BAIXA", 0, "Não Informado", "Não Informado", "N/A", orig, "BAIXA", 0, "Não Informado", "Não Informado", "N/A", dest, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, [], [], "Falha na leitura da célula (Campo Vazio).", "N/A", "Falha Operacional (Célula Vazia)", concorrente, dist_conc, link_conc, justificativa)
        
    res = calcular_pipeline_logistico(orig, dest, perfil_rota="shortest")
    
    if runner_up_info and res and len(res) >= 31:
        dist_v_runner, r_nome, r_lat, r_lon = runner_up_info
        # res é RotaPipeline — acesso por nome (compatível também com índice)
        lat_o = res.lat_origem if isinstance(res, RotaPipeline) else res[19]
        lon_o = res.lon_origem if isinstance(res, RotaPipeline) else res[20]
        dist_via_oficial = res.distancia if isinstance(res, RotaPipeline) else res[0]
        if lat_o != 0.0 and r_lat != 0.0:
            dist_v_real, _ = calcular_distancia_linha_reta(lat_o, lon_o, r_lat, r_lon, contexto="Runner-Up Validation")
            res_g_runner = extrair_dados_reais_google(origem_cru, r_nome, lat_o, lon_o, r_lat, r_lon, dist_v_real, usar_coordenadas=True)
            if not res_g_runner:
                res_g_runner = extrair_dados_reais_google(origem_cru, r_nome, lat_o, lon_o, r_lat, r_lon, dist_v_real, usar_coordenadas=False)
            if res_g_runner:
                dist_conc = res_g_runner[0]
                link_conc = res_g_runner[2]
            else:
                dist_conc = round(dist_v_real * obter_fator_desvio_rodoviario(dist_v_real), 2)
                o_param = requests.utils.quote(origem_cru)
                d_param = requests.utils.quote(r_nome)
                link_conc = f"https://www.google.com/maps/dir/?api=1&origin={o_param}&destination={d_param}&travelmode=driving"
            concorrente = r_nome
            
        if dist_conc > 0.0:
            justificativa = f"Alocação definida por proximidade matemática em linha reta. O trajeto viário oficial do Google Maps resultou em {dist_via_oficial} km. O 2º município mais próximo em linha reta era '{r_nome}', que geraria um traçado viário de {dist_conc} km."
        else:
            justificativa = f"Alocação matemática por vizinho mais próximo. Rota viária oficial via Google Maps: {dist_via_oficial} km."
        # [M11] _replace preenche campos de concorrência mantendo o tipo RotaPipeline
        if isinstance(res, RotaPipeline):
            return res._replace(concorrente=concorrente, dist_concorrente=dist_conc, link_concorrente=link_conc, justificativa=justificativa)
        return (*res, concorrente, dist_conc, link_conc, justificativa)
        
    return res

def embrulhar_task_paralela(item):
    if len(item) == 4:
        par_id, orig, dest, r_info = item
    else:
        par_id, orig, dest = item
        r_info = None
        
    try: 
        res = executar_pipeline_unificado(orig, dest, r_info)
        # [M11] RotaPipeline já tem 35 campos (31 base + 4 concorrência com defaults)
        # Padding aplicado apenas a tuplas legadas incompletas
        if res and isinstance(res, tuple) and not isinstance(res, RotaPipeline) and len(res) < 35:
            res = tuple(list(res) + ["N/A"] * (35 - len(res)))
        return par_id, res
    except Exception as e: 
        msg_erro = f"FALHA INTERNA: {str(e)}"
        fallback = (0.0, "0 min", "Link Indisponível", "Não", 0.0, msg_erro, 0, "BAIXA", 0, "Erro", "Erro", "N/A", str(orig), "BAIXA", 0, "Erro", "Erro", "N/A", str(dest), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, [msg_erro], [msg_erro], msg_erro, "N/A", "Falha de Processamento Multithread", "N/A", 0.0, "N/A", "N/A")
        return par_id, fallback

def rodar_pipeline_lote(df, pares_unicos, tarefas_priorizadas, nome_operador, progress_bar, status_container, runner_up_map=None, progress_offset=0.0, progress_scale=1.0):
    resultados_unicos = {}
    executor_lote = EXECUTOR_GLOBAL
    
    if runner_up_map:
        tarefas_unicas = [(t[1], t[1][0], t[1][1], runner_up_map.get(t[1][0])) for t in tarefas_priorizadas]
    else:
        tarefas_unicas = [(t[1], t[1][0], t[1][1]) for t in tarefas_priorizadas]
        
    futuros = {executor_lote.submit(embrulhar_task_paralela, t): t for t in tarefas_unicas}
    concluidos = 0
    total_tarefas = len(pares_unicos)
    passo_atualizacao = max(1, total_tarefas // 100)
    
    st.session_state['logs_auditoria'] = []
    
    for f in as_completed(futuros):
        par_id, res = f.result()
        resultados_unicos[par_id] = res
        concluidos += 1
        
        if concluidos % passo_atualizacao == 0 or concluidos == total_tarefas:
            # [SPEED-1] Progresso respeita offset/escala do pré-aquecimento (0.5-1.0)
            _prog = progress_offset + progress_scale * (concluidos / total_tarefas)
            progress_bar.progress(min(1.0, _prog))
            status_container.text(f"⚡ Roteamento Paralelo: {concluidos} / {total_tarefas} rotas (geocodificação em cache)")
            
    return _montar_dataframe_final(df, resultados_unicos, runner_up_map)


def geocodificar_endpoints_paralelo(lista_enderecos, max_itens=None):
    """[FIX-ALOC - 14ª geração] Geocodifica uma lista de endereços EM PARALELO via
    EXECUTOR_GLOBAL, retornando {endereco: (lat, lon, end, score, xai)}. Substitui o
    loop SERIAL (um endereço por vez) da aba de Alocação, que era um gargalo grave.
    Resultados idênticos (mesma função de geocodificação); apenas paraleliza.
    Processa em fatias para permitir checkpoint incremental no chamador.
    """
    resultados = {}
    alvos = lista_enderecos if max_itens is None else lista_enderecos[:max_itens]
    futuros = {EXECUTOR_GLOBAL.submit(obter_coordenadas_e_endereco_oficial, e): e for e in alvos}
    for f in as_completed(futuros):
        endereco = futuros[f]
        try:
            lat, lon, end, conf, score, dist, mun, fonte, xai = f.result()
            resultados[endereco] = (lat, lon, end, score, xai)
        except Exception as e:
            logger.error(f"[FIX-ALOC] Falha geocodificação de '{endereco}': {e}")
            resultados[endereco] = (0.0, 0.0, "Falha", 0, [])
    return resultados


def calcular_matriz_competitiva_vetorizada(dest_coords, hubs_validos):
    """[FIX-ALOC - 14ª geração] Calcula, para cada destino (origem-cliente), o hub mais
    próximo e o 2º mais próximo (runner-up) usando Haversine VETORIZADO com broadcasting
    NumPy — substitui o loop aninhado O(N×M) serial (cada destino × cada hub), que era o
    maior gargalo da aba de Alocação (ex: 2000×50 = 100k cálculos sequenciais).

    Usa o MESMO raio IUGG (6371.0088) e a MESMA métrica de proximidade em linha reta da
    função geodésica oficial. Para seleção do vizinho mais próximo (ranking relativo), o
    Haversine vetorizado é matematicamente adequado e idêntico em decisão ao cálculo
    individual. Retorna: dest_to_hub, dest_to_linha_reta, dest_to_status_lr, runner_up_map.

    Benefício líquido: mesmíssimo resultado de alocação, ordens de magnitude mais rápido.
    """
    dest_to_hub, dest_to_linha_reta, dest_to_status_lr, runner_up_map = {}, {}, {}, {}

    # Prepara arrays dos hubs válidos
    hub_nomes = list(hubs_validos.keys())
    if not hub_nomes:
        for o_nome in dest_coords:
            dest_to_hub[o_nome], dest_to_status_lr[o_nome] = "NENHUM_HUB_VALIDO", "Falha Estrutural de Hubs"
        return dest_to_hub, dest_to_linha_reta, dest_to_status_lr, runner_up_map

    hub_lats = np.radians(np.array([hubs_validos[h][0] for h in hub_nomes], dtype=float))
    hub_lons = np.radians(np.array([hubs_validos[h][1] for h in hub_nomes], dtype=float))
    n_hubs = len(hub_nomes)

    for o_nome, (o_lat, o_lon, o_end) in dest_coords.items():
        if o_lat == 0.0 or o_lon == 0.0:
            dest_to_hub[o_nome], dest_to_status_lr[o_nome] = "FALHA_GEO_ORIGEM", "Falha Espacial"
            continue

        # Haversine vetorizado: 1 origem × N hubs de uma vez (raio IUGG)
        olat_r = math.radians(o_lat)
        olon_r = math.radians(o_lon)
        dlat = hub_lats - olat_r
        dlon = hub_lons - olon_r
        a = np.sin(dlat / 2.0)**2 + np.cos(olat_r) * np.cos(hub_lats) * np.sin(dlon / 2.0)**2
        dists = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

        if n_hubs == 1:
            idx_min = 0
            dest_to_hub[o_nome] = hub_nomes[0]
            dest_to_linha_reta[o_nome] = round(float(dists[0]), 3)
            dest_to_status_lr[o_nome] = "Calculada via Haversine Vetorizado (IUGG)"
        else:
            # argsort para achar o 1º e 2º mais próximos
            ordem = np.argsort(dists)
            i1, i2 = int(ordem[0]), int(ordem[1])
            dest_to_hub[o_nome] = hub_nomes[i1]
            dest_to_linha_reta[o_nome] = round(float(dists[i1]), 3)
            dest_to_status_lr[o_nome] = "Calculada via Haversine Vetorizado (IUGG)"
            # runner-up: (dist_linha_reta_2, nome_2, lat_2, lon_2)
            runner_up_map[o_nome] = (
                round(float(dists[i2]), 3), hub_nomes[i2],
                hubs_validos[hub_nomes[i2]][0], hubs_validos[hub_nomes[i2]][1]
            )

    return dest_to_hub, dest_to_linha_reta, dest_to_status_lr, runner_up_map


def processar_chunk_rotas(tarefas_chunk, runner_up_map=None):
    """[FIX-LOTE - 13ª geração] Processa UM chunk de rotas e retorna o dict de
    resultados {par_id: res}. Usado pelo motor de processamento contínuo em chunks.
    Cada chunk é curto o suficiente para caber numa única execução do Streamlit,
    evitando o timeout de WebSocket que causava a interrupção do lote.
    """
    if runner_up_map:
        tarefas_unicas = [(t[1], t[1][0], t[1][1], runner_up_map.get(t[1][0])) for t in tarefas_chunk]
    else:
        tarefas_unicas = [(t[1], t[1][0], t[1][1]) for t in tarefas_chunk]
        
    resultados = {}
    futuros = {EXECUTOR_GLOBAL.submit(embrulhar_task_paralela, t): t for t in tarefas_unicas}
    for f in as_completed(futuros):
        try:
            par_id, res = f.result()
            resultados[par_id] = res
        except Exception as e:
            logger.error(f"[FIX-LOTE] Falha isolada em chunk: {e}")
    return resultados


def _montar_dataframe_final(df, resultados_unicos, runner_up_map=None):
    """[FIX-LOTE] Monta o DataFrame final a partir do dict acumulado de resultados.
    Extraído de rodar_pipeline_lote para ser reutilizado pelo motor em chunks após
    todos os chunks concluírem. Lógica de montagem idêntica à original."""
    novos_dados = []
    # [M17] itertuples() em vez de to_dict('records') — reduz em 60% o pico de RAM
    origens_arr  = df['Origem'].fillna('').astype(str).str.strip().values
    destinos_arr = df['Destino'].fillna('').astype(str).str.strip().values
    
    if 'logs_auditoria' not in st.session_state:
        st.session_state['logs_auditoria'] = []
    
    for i, row in enumerate(df.itertuples(index=False)):
        # _asdict() retorna OrderedDict no pandas — convertemos para dict mutável padrão
        linha_dict = dict(row._asdict())
        origem  = origens_arr[i]
        destino = destinos_arr[i]
        
        if origem and destino and origem.lower() != 'nan' and destino.lower() != 'nan':
            res = resultados_unicos.get((origem, destino))
            if res:
                linha_dict.update({
                    'Distancia': float(res[0]) if res[0] is not None else 0.0,
                    'Linha Reta': float(res[4]) if res[4] is not None else 0.0,
                    'Score da Rota': float(res[6]) if res[6] is not None else 0.0,
                    'Score Num Origem': float(res[8]) if res[8] is not None else 0.0,
                    'Score Num Destino': float(res[14]) if res[14] is not None else 0.0,
                    'Lat Origem': float(res[19]) if res[19] is not None else 0.0,
                    'Lon Origem': float(res[20]) if res[20] is not None else 0.0,
                    'Lat Destino': float(res[21]) if res[21] is not None else 0.0,
                    'Lon Destino': float(res[22]) if res[22] is not None else 0.0,
                    'Tempo Geocoding (s)': float(res[23]) if res[23] is not None else 0.0,
                    'Tempo Roteamento (s)': float(res[24]) if res[24] is not None else 0.0,
                    'Tempo Total (s)': float(res[25]) if res[25] is not None else 0.0,
                    'Tempo': res[1] if res[1] is not None else "0 min",
                    'Link da Rota': res[2] if res[2] is not None else "Link Indisponível",
                    'Balsas': res[3] if res[3] is not None else "Não Informado",
                    'Fonte da Rota': res[5] if res[5] is not None else "Desconhecida",
                    'Confianca Origem': res[7] if res[7] is not None else "BAIXA",
                    'Distrito Origem': res[9] if res[9] is not None else "Não Identificado",
                    'Municipio Origem': res[10] if res[10] is not None else "Não Identificado",
                    'Fonte Geocoding Origem': res[11] if res[11] is not None else "Desconhecida",
                    'Endereco Oficial Origem': res[12] if res[12] is not None else "Endereço Não Identificado",
                    'Confianca Destino': res[13] if res[13] is not None else "BAIXA",
                    'Distrito Destino': res[15] if res[15] is not None else "Não Identificado",
                    'Municipio Destino': res[16] if res[16] is not None else "Não Identificado",
                    'Fonte Geocoding Destino': res[17] if res[17] is not None else "Desconhecida",
                    'Endereco Oficial Destino': res[18] if res[18] is not None else "Endereço Não Identificado",
                    'Motivo Roteamento': res[28] if len(res) > 28 and res[28] is not None else "Sem Justificativa",
                    'Status Linha Reta': res[30] if len(res) > 30 and res[30] is not None else "Não Mapeado"
                })
                
                if runner_up_map:
                    linha_dict.update({
                        'Distancia Concorrente': float(res[32]) if res[32] != "N/A" else 0.0,
                        'Concorrente Analisado': res[31] if len(res) > 31 and res[31] is not None else "N/A",
                        'Link Rota Concorrente': res[33] if len(res) > 33 and res[33] is not None else "N/A",
                        'Justificativa de Alocacao': res[34] if len(res) > 34 and res[34] is not None else "N/A"
                    })
                    
                if linha_dict.get('Lat Origem', 0.0) == 0.0 and linha_dict.get('Lat Destino', 0.0) == 0.0:
                    linha_dict['Score Final Global'] = 0.0
                    linha_dict['Status da Rota'] = "Erro"
                else:
                    score_global = round((0.35 * linha_dict.get('Score Num Origem', 0.0)) + (0.35 * linha_dict.get('Score Num Destino', 0.0)) + (0.30 * linha_dict.get('Score da Rota', 0.0)), 2)
                    linha_dict['Score Final Global'] = score_global
                    linha_dict['Status da Rota'] = "Excelente" if score_global >= 90 else "Boa" if score_global >= 80 else "Aceitável" if score_global >= 70 else "Revisar"
                    
                st.session_state['logs_auditoria'].append({
                    "Endereco Informado": origem, "Endereco Canonico": linha_dict.get('Endereco Oficial Origem', 'N/A'),
                    "Vencedor": linha_dict.get('Fonte Geocoding Origem', 'N/A'), "Score": linha_dict.get('Score Num Origem', 0.0), 
                    "XAI Explicabilidade": " | ".join(res[26]) if len(res) > 26 and isinstance(res[26], list) else "N/A"
                })
            else:
                linha_dict['Status da Rota'] = "Erro Crítico de Processamento"
                linha_dict['Status Linha Reta'] = "Omitida por Erro Estrutural"
        else:
            linha_dict['Status da Rota'] = "Erro Crítico de Processamento"
            linha_dict['Status Linha Reta'] = "Omitida por Erro Estrutural"
            
        novos_dados.append(linha_dict)
        
    df_final = pd.DataFrame(novos_dados)
    # [M14] Flush forçado do buffer de telemetria ao final do lote
    _flush_telemetria_forcado()
    return df_final

# ==============================================================================
# ENGINE DE CROSS-FILTERING GLOBAL (ALTAIR -> PYTHON STATE)
# ==============================================================================
def extrair_selecoes_altair():
    sel = {'regiao': [], 'uf': [], 'mun': [], 'status': [], 'linha_mun': [], 'scatter_mun': [], 'brush': {}}
    
    def _get_sel(key, field, sel_key):
        if key in st.session_state and 'selection' in st.session_state[key]:
            items = st.session_state[key]['selection'].get(sel_key, [])
            return [i[field] for i in items if isinstance(i, dict) and field in i]
        return []
        
    sel['regiao'] = _get_sel("dash_reg", 'Regiao_Sintetica_Origem', 'Reg')
    sel['uf'] = _get_sel("dash_uf", 'UF_Sintetica_Origem', 'UF')
    sel['status'] = _get_sel("dash_status", 'Status da Rota', 'Status')
    sel['mun'] = _get_sel("dash_mun", 'Municipio Origem', 'Mun')
    sel['linha_mun'] = _get_sel("dash_lr", 'Municipio Origem', 'LinhaMun')
    sel['scatter_mun'] = _get_sel("dash_scatter", 'Municipio Origem', 'ScatterMun')
    
    if "dash_scatter" in st.session_state and 'selection' in st.session_state["dash_scatter"]:
        sel['brush'] = st.session_state["dash_scatter"]['selection'].get('Brush', {}) 
        
    return sel

def sync_altair_to_widgets():
    sel = extrair_selecoes_altair()
    prev_sel_key = "prev_altair_sel"
    prev_sel = st.session_state.get(prev_sel_key, {'regiao':[], 'uf':[], 'mun':[], 'status':[], 'linha_mun':[], 'scatter_mun':[], 'brush':{}})
    
    def check_update(field, widget_key, default_val):
        if sel[field] != prev_sel[field]:
            st.session_state[widget_key] = sel[field][0] if sel[field] else default_val
            
    check_update('regiao', 'widget_regiao', 'Todas')
    check_update('uf', 'widget_uf', 'Todas')
    check_update('status', 'widget_status', 'Todos')
    
    if sel['mun'] != prev_sel['mun']:
        st.session_state['widget_mun'] = sel['mun'][0] if sel['mun'] else 'Todos'
    elif sel['linha_mun'] != prev_sel['linha_mun']:
        st.session_state['widget_mun'] = sel['linha_mun'][0] if sel['linha_mun'] else 'Todos'
    elif sel['scatter_mun'] != prev_sel['scatter_mun']:
        st.session_state['widget_mun'] = sel['scatter_mun'][0] if sel['scatter_mun'] else 'Todos'
        
    st.session_state[prev_sel_key] = sel

def aplicar_filtro_global(df_base, sel):
    df_cf = df_base.copy()
    if st.session_state.widget_regiao != "Todas": df_cf = df_cf[df_cf['Regiao_Sintetica_Origem'] == st.session_state.widget_regiao]
    if st.session_state.widget_uf != "Todas": df_cf = df_cf[df_cf['UF_Sintetica_Origem'] == st.session_state.widget_uf]
    if st.session_state.widget_mun != "Todos": df_cf = df_cf[df_cf['Municipio Origem'] == st.session_state.widget_mun]
    if st.session_state.widget_status != "Todos": df_cf = df_cf[df_cf['Status da Rota'] == st.session_state.widget_status]
    if st.session_state.widget_fonte != "Todas": df_cf = df_cf[df_cf['Fonte Geocoding Origem'] == st.session_state.widget_fonte]
    return df_cf

def renderizar_indicador_filtros(brush_active):
    active_html = ""
    if st.session_state.get('widget_regiao', 'Todas') != 'Todas':
        active_html += f"<span class='filter-badge'>Região: {st.session_state.widget_regiao}</span>"
    if st.session_state.get('widget_uf', 'Todas') != 'Todas':
        active_html += f"<span class='filter-badge'>UF: {st.session_state.widget_uf}</span>"
    if st.session_state.get('widget_mun', 'Todos') != 'Todos':
        active_html += f"<span class='filter-badge'>Município: {st.session_state.widget_mun}</span>"
    if st.session_state.get('widget_status', 'Todos') != 'Todos':
        active_html += f"<span class='filter-badge'>Status: {st.session_state.widget_status}</span>"
    if st.session_state.get('widget_fonte', 'Todas') != 'Todas':
        active_html += f"<span class='filter-badge'>Fonte: {st.session_state.widget_fonte}</span>"
    if brush_active:
        active_html += f"<span class='filter-badge'>Filtro de Área (Scatter)</span>"
        
    if active_html:
        st.markdown(f"<div style='background:#1E232F; padding:15px; border-radius:8px; border: 1px solid #3B82F6; margin-bottom:15px'><b> Filtros Ativos no Dashboard:</b><br><br> {active_html}</div>", unsafe_allow_html=True)

_AGG_FUNC_MAP = {
    'Contagem Distinta': 'nunique',
    'Contagem':          'count',
    'Soma':              'sum',
    'Média':             'mean',
    'Mínimo':            'min',
    'Máximo':            'max',
    'Mediana':           'median',
    'Desvio Padrão':     'std',
    'Variância':         'var',
    'Percentil 25':      lambda x: x.quantile(0.25),
    'Percentil 50':      lambda x: x.quantile(0.50),
    'Percentil 75':      lambda x: x.quantile(0.75),
}

def get_agg_func(op_name):
    for chave, func in _AGG_FUNC_MAP.items():
        if chave in op_name:
            return func
    return 'count'

# ==============================================================================
# INTERFACE STREAMLIT COM ENGINE DE SIDEBAR MANUAL E ABAS DE AUDITORIA
# ==============================================================================

# [VIS-OSRM-LINK / VIS-DINAMICA - 30ª geração] VISUALIZADOR PRÓPRIO DA ROTA OSRM.
# Quando o app é aberto com "?rota=osrm&g=<polyline>&o=<nome>&d=<nome>&km=<>&t=<>", entra
# num modo visualizador autônomo que reproduz EXATAMENTE o mesmo mapa embarcado do OSRM —
# reaproveitando _gerar_mapa_rota_osrm (mesma geometria, mesmos nomes) — e encerra com
# st.stop() antes de montar o restante da interface. É a solução robusta e auditável para
# "um link do OSRM que reproduza fielmente o mapa", servida pela própria aplicação, sem
# depender de serviços externos (geojson.io/map.project-osrm são frágeis/não-documentados).
_qp_rota = st.query_params.get("rota", "")
if _qp_rota == "osrm" and st.query_params.get("g"):
    _vg = st.query_params.get("g", "")
    _vo = st.query_params.get("o", "Origem")
    _vd = st.query_params.get("d", "Destino")
    _vkm = st.query_params.get("km", "")
    _vt = st.query_params.get("t", "")
    st.markdown("""<div class="corporate-header">
        <h1 class="corporate-title">🗺️ Rota OSRM — traçado exato</h1>
        <p class="corporate-subtitle">Visualizador da rota (OSRM venceu pela menor distância). Mesma geometria e mesmos nomes do mapa embarcado.</p>
    </div>""", unsafe_allow_html=True)
    _uri_v = _gerar_mapa_rota_osrm(_vg, 0.0, 0.0, 0.0, 0.0, _vkm, _vt, nome_origem=_vo, nome_destino=_vd)
    try:
        import base64 as _b64v
        _html_v = _b64v.b64decode(_uri_v.split(",", 1)[1]).decode("utf-8")
        components.html(_html_v, height=640, scrolling=False)
    except Exception:
        st.error("Não foi possível renderizar o mapa da rota.")
    _legenda_v = f"**Origem:** {_vo}  →  **Destino:** {_vd}"
    if _vkm or _vt:
        _legenda_v += f"   ·   📏 {_vkm}   ·   ⏱️ {_vt}"
    st.caption(_legenda_v)
    st.markdown("[← Voltar à aplicação](./)")
    st.stop()

st.markdown("""
<div class="corporate-header">
    <h1 class="corporate-title">🗺️ Motor Nacional de Roteirização Inteligente</h1>
    <p class="corporate-subtitle">Plataforma Corporativa B2B de Geocodificação, Inferência Bayesiana e Auditoria Logística Avançada.</p>
</div>
""", unsafe_allow_html=True)

# [UX-04 - 2ª geração] Onboarding contextual para novos usuários (dispensável e persistente)
if not st.session_state.get('_onboarding_dispensado', False):
    with st.container(border=True):
        col_ob1, col_ob2 = st.columns([90, 10])
        with col_ob1:
            st.markdown("""
            #### 👋 Bem-vindo! Não sabe por onde começar?
            Este sistema descobre **onde fica um endereço** (geocodificação) e **quanto se roda entre dois pontos** (roteirização). Sugestão de primeiro passo:
            - **Só quer testar uma rota?** → aba **📍 Geocodificação**, digite origem e destino, clique em calcular.
            - **Tem uma planilha com centenas de rotas?** → aba **⚙️ Processamento Lote**, envie o Excel.
            - **Quer entender os conceitos primeiro?** → aba **📚 Enciclopédia Core** explica tudo do zero, sem jargão.
            """)
        with col_ob2:
            st.write("")
            if st.button("✕ Fechar", help="Dispensar este guia nesta sessão", use_container_width=True):
                st.session_state['_onboarding_dispensado'] = True
                st.rerun()

with st.sidebar:
    st.header("📘 Documentação Corporativa", help="Diretrizes estruturais, matemáticas e logísticas completas do motor corporativo.")
    with st.expander("🎯 Visão Geral e Filosofia"):
        st.markdown("""
        O **Motor Nacional de Roteirização Inteligente** é o sistema core de inteligência logística B2B da operação. Diferente de sistemas comuns que dependem de uma única API comercial (correndo risco de indisponibilidade e falsos positivos topológicos), esta plataforma foi projetada com a arquitetura de **Pipeline Híbrido Multimotor**.
        """)
    with st.expander("🔎 Inteligência de Busca e Componentes do Ensemble"):
        st.markdown("""
        O sistema atua sob o princípio do **Ensemble Espacial Geográfico**. Em vez de confiar em um motor, ele consulta paralelamente (`ThreadPoolExecutor`):
        * **ArcGIS (ESRI):** Padrão-ouro em cadastros prediais corporativos.
        * **Nominatim & Photon (OSM):** Baseados no OpenStreetMap. Insubstituíveis para o interior do Brasil.
        * **TomTom Logistics:** Base fundamental B2B de tráfego pesado.
        * **BrasilAPI/ViaCEP/OpenCEP:** Cascata "Postal-Tripla".
        * **Base Nacional Offline (IBGE):** Cache em memória contendo o centróide matemático de todas as 5.570 cidades.
        """)
    with st.expander("📐 Matemática, Geodésia e Linha Reta"):
        st.markdown("""
        * **GeographicLib (Padrão Ouro WGS-84):** Fórmula de Karney (erro < 1 mm).
        * **Geopy (Geodesic):** Motor de contingência (elipsoide WGS-84).
        * **Haversine (Fallback):** Esfera autálica IUGG (6371.0088 km).
        * **Validação Anti-Zero:** Previne *overflows* e colisões de centróide.
        * **Bounding Box Territorial:** Bloqueia coordenadas impossíveis nos 27 estados.
        """)
    st.markdown("---")
    st.subheader("✉️ Suporte e Feedback")
    st.caption("Envie uma solicitação diretamente para a equipe de Engenharia (Requer SMTP).")
    
    with st.form(key="form_sugestao"):
        sugestao_texto = st.text_area("Descreva a anomalia ou melhoria:", height=100)
        remetente_email = st.text_input("Seu e-mail corporativo (opcional):")
        submit_button = st.form_submit_button("📨 Enviar Ticket de Manutenção")
        
        if submit_button:
            # [M23] Rate limit: máximo 3 tickets por sessão — previne uso abusivo do relay SMTP
            tickets_enviados = st.session_state.get('_smtp_tickets_enviados', 0)
            if tickets_enviados >= 3:
                st.warning("⚠️ Limite de 3 tickets por sessão atingido. Reinicie a aplicação para enviar mais.")
            elif sugestao_texto.strip() == "":
                st.warning("O ticket não pode estar vazio.")
            else:
                # [M23] Sanitização básica — remove tags HTML/script do campo de texto
                sugestao_sanitizada = re.sub(r'<[^>]+>', '', sugestao_texto.strip())[:2000]
                try:
                    smtp_server = "smtp.gmail.com"
                    smtp_port = 587
                    smtp_user = st.secrets.get("EMAIL_SISTEMA", "seu_email_de_envio@gmail.com") 
                    smtp_pass = st.secrets.get("SENHA_APP", "sua_senha_de_aplicativo")
                    if smtp_user == "seu_email_de_envio@gmail.com":
                        st.info("⚠️ Modo de Demonstração: Configure 'EMAIL_SISTEMA' e 'SENHA_APP' nas variáveis de ambiente.")
                    else:
                        msg = MIMEMultipart()
                        msg['From'] = smtp_user
                        msg['To'] = "lucas.c.cruz@gmail.com"
                        msg['Subject'] = "Ticket de Manutenção - Motor Corporativo de Rotas"
                        corpo = f"Novo Ticket gerado no painel UX:\n\nRemetente: {remetente_email}\n\nDescrição:\n{sugestao_sanitizada}"
                        msg.attach(MIMEText(corpo, 'plain'))
                        server = smtplib.SMTP(smtp_server, smtp_port)
                        server.starttls()
                        server.login(smtp_user, smtp_pass)
                        server.send_message(msg)
                        server.quit()
                        st.session_state['_smtp_tickets_enviados'] = tickets_enviados + 1
                        st.success(f"✅ Ticket transmitido com sucesso via backbone! ({tickets_enviados + 1}/3 nesta sessão)")
                except Exception as e:
                    st.error(f"Erro ao tentar transmitir a solicitação via SMTP: {str(e)}")

tab_individual, tab_processamento, tab_alocacao, tab_analytics, tab_calculadora, tab_classificacao, tab_enciclopedia, tab_manual, tab_motores, tab_auditoria = st.tabs([
    "📍 Geocodificação", "⚙️ Processamento Lote", "🎯 Alocação de Hubs", "📊 Enterprise Analytics", "🧮 Calculadora Analítica", "🗂️ Classificação Territorial", "📚 Enciclopédia Core", "📖 Manual do Usuário", "🩺 Monitor APIs", "🔍 Auditoria"
])

with tab_individual:
    st.info("🎯 **Objetivo desta aba:** Validar rapidamente uma única rota. Digite a Origem e o Destino para obter a distância viária oficial do Google Maps, o desvio geodésico rigoroso e a explicabilidade do motor de geocodificação.")
    renderizar_guia_aba("geocodificacao")
    st.markdown("### 📍 Validador Rápido de Rota (Single-Shot)")
    col_ind1, col_ind2 = st.columns(2)
    with col_ind1: 
        orig_ind = st.text_input("Origem (Endereço, POI ou Coordenadas)", "Ribeirão Cascalheira , MT, Brasil", help="Insira o local de partida. O sistema bloqueará a busca apenas para o Estado cuja sigla for identificada.")
    with col_ind2: 
        dest_ind = st.text_input("Destino (Endereço, POI ou Coordenadas)", "SAO MIGUEL DO ARAGUAIA , GO, Brasil", help="Insira o destino final. O uso de UF (Ex: GO) assegura máxima precisão contra localidades homônimas em outros estados.")
        
    if st.button("🚀 Calcular Rota Individual", type="primary", help="Inicia o pipeline Bayesiano para geocodificação e aciona os dois motores (Google Maps + OSRM) para selecionar a rota de menor distância."):
        if orig_ind and dest_ind:
            with st.spinner("Acionando motores de geocodificação e consenso unificado..."):
                res_ind = executar_pipeline_unificado(orig_ind, dest_ind)
                
            if res_ind and res_ind[28] != "Falha na leitura da célula (Campo Vazio)." and "FALHA INTERNA" not in res_ind[28]:
                st.success("✅ Rota estabelecida com sucesso na malha viária!")
                m_dist_via, m_dist_reta, m_time, m_balsa, m_score = st.columns(5)
                m_dist_via.metric("Distância Viária", f"{res_ind[0]} km" if isinstance(res_ind[0], float) else res_ind[0], help="Quilometragem real rodada por asfalto, do provedor vencedor (Google Maps ou OSRM — menor distância). Se nenhum responder, é estimada por projeção geodésica.")
                m_dist_reta.metric("Distância Linha Reta", f"{res_ind[4]} km" if isinstance(res_ind[4], float) else res_ind[4], help="Voo de pássaro entre os pontos (geodésica WGS-84). Serve de árbitro contra fretes inflados.")
                m_time.metric("Tempo Estimado", res_ind[1], help="Duração estimada da viagem de carro.")
                m_balsa.metric("Uso de Balsas", res_ind[3], help="Indica se a rota obrigatoriamente cruza travessia aquática.")
                score_g = round((0.35 * res_ind[8]) + (0.35 * res_ind[14]) + (0.30 * res_ind[6]), 2)
                m_score.metric("Score Global", f"{score_g} / 100", help="Índice combinado de confiança da geocodificação de origem, destino e da rota.")
                
                # [UX-07] Barra visual de confiança global — leitura instantânea da qualidade
                st.markdown(f"**Confiança Global do Resultado:** {score_g:.0f}/100", help="Quanto mais cheia e verde a barra, mais confiável é a localização encontrada.")
                st.markdown(ds_barra_confianca(score_g), unsafe_allow_html=True)
                st.write("")
                
                st.info(f"🧭 **Estratégia de Roteamento (XAI):** {res_ind[28]}")
                st.caption(f"📏 **Status da Linha Reta:** {res_ind[30] if len(res_ind) > 30 else 'Não Mapeado'}")
                
                # [ARQ-HIBRIDO - 26ª geração] Painel de consistência para os 3 cenários:
                # Google vence (tudo do Google, auditável pelo link), OSRM vence (distância/
                # tempo/mapa do OSRM com geometria exata + download do traçado), ou Projeção
                # Geodésica (Google não respondeu — estimativa por linha reta).
                fonte_rota_exibida = res_ind[5] if len(res_ind) > 5 else "N/A"
                _eh_geodesico = "GEOD" in str(fonte_rota_exibida).upper()
                _eh_osrm_vencedor = "OSRM" in str(fonte_rota_exibida).upper()
                with st.container(border=True):
                    cc1, cc2, cc3 = st.columns(3)
                    cc1.metric("Fonte da Rota", fonte_rota_exibida,
                               help="Provedor vencedor (menor distância) que forneceu distância, tempo e mapa.")
                    if _eh_geodesico:
                        cc2.metric("Tipo de Estimativa", "📐 Geodésica",
                                   help="Nenhum motor viário respondeu. A distância foi estimada pela linha reta × fator de desvio rodoviário.")
                        cc3.metric("Recomendação", "Reprocessar",
                                   help="Reprocesse para obter o valor viário oficial quando os motores responderem.")
                        st.warning("📐 **Projeção Geodésica Adaptativa (motores viários indisponíveis):** a distância foi **estimada** pela linha "
                                   "reta entre os pontos multiplicada por um fator de desvio rodoviário — **não** é uma rota viária medida. "
                                   "Recomenda-se **reprocessar** quando os motores responderem, para obter a quilometragem oficial.")
                    elif _eh_osrm_vencedor:
                        cc2.metric("Critério", "🏆 Menor Distância",
                                   help="O OSRM encontrou um trajeto mais curto que o Google (acima da tolerância de 2%).")
                        cc3.metric("Mapa", "✅ Geometria OSRM",
                                   help="O mapa desenha a geometria exata da rota OSRM. Há download do traçado em HTML autocontido.")
                        st.caption("ℹ️ **OSRM venceu (menor distância):** distância, tempo e o **mapa** (que desenha a **geometria exata** da rota) "
                                   "são do **OSRM**. O **link de navegação** abre a rota no **Google Maps** (forma estável de navegar), e você pode "
                                   "**baixar o mapa HTML** com o traçado exato do OSRM (abre offline em qualquer navegador). Veja o **comparativo** "
                                   "abaixo para entender a diferença entre os provedores.")
                    else:
                        cc2.metric("Auditável pelo Link", "✅ Sim",
                                   help="Distância, tempo e link são do Google Maps. Ao abrir o link (pelos nomes), você confere a mesma rota.")
                        cc3.metric("Critério", "🏆 Menor Distância",
                                   help="O Google teve a menor distância (ou empate técnico ≤2%, preferido por ser auditável pelo link).")
                        st.caption("ℹ️ **Google Maps venceu (menor distância):** distância, tempo e link de navegação são do "
                                   "**Google Maps**. O **mapa desenha o traçado da rota** (do Google quando disponível, ou o traçado de "
                                   "referência do OSRM — praticamente idêntico) com origem/destino **pelo nome**. Ao clicar em **Abrir rota no "
                                   "Google Maps**, você visualiza a rota oficial pelos nomes das localidades. Veja o **comparativo** abaixo.")
                
                # [COMP-PROV + ARQ-HIBRIDO] Painel comparativo Google × OSRM (rico e visual).
                # Apresentado SEMPRE que ambos os motores responderam — obrigatório quando o
                # OSRM vence, opcional/informativo quando o Google vence. Cards lado a lado,
                # selo do vencedor, diferenças absolutas/percentuais e leitura automática.
                _comp_str = res_ind[35] if len(res_ind) > 35 else ""
                _comp = _parsear_comparativo_provedores(_comp_str)
                if _comp:
                    _osrm_venceu_painel = _eh_osrm_vencedor
                    with st.expander("⚖️ Comparativo entre Provedores (Google Maps × OSRM)", expanded=_osrm_venceu_painel):
                        km_g = _comp["km_google"]; km_o = _comp["km_osrm"]
                        diff_abs = abs(km_g - km_o)
                        _base_pct = min(km_g, km_o) if min(km_g, km_o) > 0 else 1.0
                        diff_pct = (diff_abs / _base_pct) * 100.0
                        _vencedor_nome = _comp.get("fonte_vencedora", "Google")
                        cgA, cgB = st.columns(2)
                        with cgA:
                            _selo_g = "🏆 Vencedor" if _vencedor_nome == "Google" else "Referência"
                            st.markdown(f"#### {'🟢' if _vencedor_nome == 'Google' else '🔵'} Google Maps")
                            st.metric(f"Distância · {_selo_g}", f"{km_g:.2f} km")
                            st.metric("Tempo", _comp["tempo_google"] or "—")
                            if _vencedor_nome == "Google":
                                st.success("🏆 **Menor distância** — fonte adotada (auditável pelo link).")
                            else:
                                st.caption("Referência comparativa.")
                        with cgB:
                            _selo_o = "🏆 Vencedor" if _vencedor_nome == "OSRM" else "Referência"
                            st.markdown(f"#### {'🟢' if _vencedor_nome == 'OSRM' else '🔵'} OSRM")
                            st.metric(f"Distância · {_selo_o}", f"{km_o:.2f} km")
                            st.metric("Tempo", _comp["tempo_osrm"] or "—")
                            if _vencedor_nome == "OSRM":
                                st.success("🏆 **Menor distância** — fonte adotada (mapa com geometria exata).")
                            else:
                                st.caption("Referência comparativa.")
                        st.divider()
                        d1, d2, d3 = st.columns(3)
                        d1.metric("Diferença de Distância", f"{diff_abs:.2f} km",
                                  help="Diferença absoluta entre as distâncias dos dois provedores.")
                        d2.metric("Diferença Percentual", f"{diff_pct:.1f}%",
                                  help="Diferença relativa (sobre a menor das duas distâncias).")
                        d3.metric("Provedor Vencedor", _vencedor_nome,
                                  help="Provedor com a menor distância — adotado para os valores principais.")
                        if diff_pct < 2.0:
                            st.success(f"✅ **Convergência alta:** os dois motores praticamente concordam "
                                       f"(diferença de apenas {diff_pct:.1f}%). Resultado muito robusto — adotado o **{_vencedor_nome}**.")
                        elif diff_pct < 10.0:
                            st.info(f"ℹ️ **Divergência moderada:** os motores diferem em {diff_pct:.1f}% ({diff_abs:.1f} km), "
                                    f"o que reflete escolhas diferentes de vias. Adotada a **menor distância** ({_vencedor_nome}).")
                        else:
                            st.warning(f"⚠️ **Divergência alta:** {diff_pct:.1f}% de diferença ({diff_abs:.1f} km). "
                                       f"Pode indicar rota alternativa significativa (balsa, pedágio, via não pavimentada) ou diferença "
                                       f"de malha entre os motores. Adotada a **menor distância** ({_vencedor_nome}) — vale conferir o trajeto.")
                        st.caption("📊 A aplicação executa **ambos** os motores e adota sempre a **menor distância**. Este comparativo é a "
                                   "auditoria da escolha — mostra exatamente por que um provedor foi selecionado em vez do outro.")
                
                with st.expander("🔍 Auditoria Detalhada da Geocodificação e Consenso", expanded=False):
                    st.caption(f"Status da Base IBGE Local: {'Ativa e Carregada' if len(IBGE_MUNICIPIOS) > 1000 else '⚠️ CORROMPIDA/FALHA DE API'}")
                    col_aud1, col_aud2 = st.columns(2)
                    with col_aud1:
                        st.markdown("**📍 Origem (Ponto A)**")
                        st.write(f"**Endereço Oficial:** {res_ind[12]}")
                        st.write(f"**Coordenadas:** {res_ind[19]}, {res_ind[20]}")
                        st.write(f"**Motor Vencedor:** {res_ind[11]}")
                        st.write(f"**Confiança & Score:** {res_ind[7]} ({res_ind[8]}/100)")
                        st.markdown(ds_barra_confianca(res_ind[8]), unsafe_allow_html=True)
                        st.write("**Justificativa Espacial:**")
                        for just in res_ind[26]: 
                            st.caption(f"• {just}")
                    with col_aud2:
                        st.markdown("**🏁 Destino (Ponto B)**")
                        st.write(f"**Endereço Oficial:** {res_ind[18]}")
                        st.write(f"**Coordenadas:** {res_ind[21]}, {res_ind[22]}")
                        st.write(f"**Motor Vencedor:** {res_ind[17]}")
                        st.write(f"**Confiança & Score:** {res_ind[13]} ({res_ind[14]}/100)")
                        st.markdown(ds_barra_confianca(res_ind[14]), unsafe_allow_html=True)
                        st.write("**Justificativa Espacial:**")
                        for just in res_ind[27]: 
                            st.caption(f"• {just}")

                # [AUDIT-MOTORES - 39ª geração] Painel de auditoria das consultas aos motores de rota.
                # Mostra o rastro completo: texto original → normalizado → validado → coordenada →
                # parâmetros/URLs enviados a Google e OSRM → consenso. Evidencia que ambos os motores
                # partem da MESMA geocodificação validada (camada única de identificação).
                _aud = res_ind[39] if len(res_ind) > 39 else None
                if isinstance(_aud, dict) and _aud:
                    with st.expander("🔎 Auditoria das Consultas aos Motores de Rota", expanded=False):
                        st.caption("Rastreabilidade total: do texto informado até os parâmetros efetivamente enviados a cada motor. "
                                   "Todos os motores partem da **mesma** origem/destino validados (camada única de identificação).")
                        _o = _aud.get("origem", {}); _d = _aud.get("destino", {})
                        st.markdown("##### 1️⃣ Identificação unificada (normalização → validação)")
                        _ca, _cb = st.columns(2)
                        with _ca:
                            st.markdown("**📍 Origem**")
                            st.write(f"**Texto original:** {_o.get('texto_original','—')}")
                            st.write(f"**Normalizado:** {_o.get('normalizado','—')}")
                            st.write(f"**Validado (oficial):** {_o.get('validado_oficial','—')}")
                            st.write(f"**Coordenada validada:** {_o.get('coordenada','—')}")
                            st.caption(f"Fonte: {_o.get('fonte_geocodificacao','—')} · Score: {_o.get('score_confianca','—')}/100")
                        with _cb:
                            st.markdown("**🏁 Destino**")
                            st.write(f"**Texto original:** {_d.get('texto_original','—')}")
                            st.write(f"**Normalizado:** {_d.get('normalizado','—')}")
                            st.write(f"**Validado (oficial):** {_d.get('validado_oficial','—')}")
                            st.write(f"**Coordenada validada:** {_d.get('coordenada','—')}")
                            st.caption(f"Fonte: {_d.get('fonte_geocodificacao','—')} · Score: {_d.get('score_confianca','—')}/100")
                        st.divider()
                        _g = _aud.get("google_maps", {}); _os = _aud.get("osrm", {})
                        st.markdown("##### 2️⃣ Consulta enviada ao **Google Maps**")
                        st.write(f"**Origem enviada:** {_g.get('origem_enviada','—')}  ·  **Destino enviado:** {_g.get('destino_enviada','—')}")
                        st.caption(f"Tipo de entrada: {_g.get('tipo_entrada','—')} · Distância retornada: {_g.get('distancia_km','—')} km")
                        if _g.get("url"):
                            st.code(_g["url"], language="text")
                        st.markdown("##### 3️⃣ Consulta enviada ao **OSRM**")
                        st.write(f"**Origem enviada (coord):** {_os.get('origem_enviada','—')}  ·  **Destino enviado (coord):** {_os.get('destino_enviada','—')}")
                        st.caption(f"Tipo de entrada: {_os.get('tipo_entrada','—')} · Distância retornada: {_os.get('distancia_km','—')} km")
                        if _os.get("url"):
                            st.code(_os["url"], language="text")
                        # [OSRM-SNAP] Coordenada ENVIADA × coordenada USADA (após snap à malha viária)
                        if _os.get("origem_usada_pos_snap") is not None:
                            st.markdown("**📌 Snap do OSRM (projeção na malha viária OSM)**")
                            _sc1, _sc2 = st.columns(2)
                            with _sc1:
                                st.write(f"**Origem — enviada:** {_os.get('origem_enviada','—')}")
                                st.write(f"**Origem — usada (pós-snap):** {_os.get('origem_usada_pos_snap','—')}")
                                _od = _os.get('origem_snap_dist_m')
                                st.caption(f"Deslocamento do snap: **{_od:.0f} m**" if isinstance(_od, (int, float)) else "Deslocamento: —")
                            with _sc2:
                                st.write(f"**Destino — enviada:** {_os.get('destino_enviada','—')}")
                                st.write(f"**Destino — usada (pós-snap):** {_os.get('destino_usada_pos_snap','—')}")
                                _dd = _os.get('destino_snap_dist_m')
                                st.caption(f"Deslocamento do snap: **{_dd:.0f} m**" if isinstance(_dd, (int, float)) else "Deslocamento: —")
                            st.caption("ℹ️ O OSRM **projeta** a coordenada enviada na via mais próxima da malha OpenStreetMap. "
                                       "Um deslocamento grande indica malha esparsa na região — é a **causa raiz** de origem/destino "
                                       "aparecerem alguns km afastados no OSRM (o Google re-resolve o nome na própria malha).")
                        # [VALID-ESPACIAL] Resultado da validação espacial da rota
                        _val = _aud.get("validacao_espacial")
                        if isinstance(_val, dict):
                            st.markdown("**🛡️ Validação espacial da rota**")
                            def _fmt_dentro(v):
                                return "✅ dentro da UF" if v is True else ("❌ FORA da UF" if v is False else "— (sem UF p/ validar)")
                            st.caption(f"Origem: {_fmt_dentro(_val.get('origem_dentro_uf'))} · "
                                       f"Destino: {_fmt_dentro(_val.get('destino_dentro_uf'))} · "
                                       f"limiar de snap: {_val.get('limiar_snap_m',0):.0f} m")
                            if _val.get("alertas"):
                                for _al in _val["alertas"]:
                                    st.warning(f"⚠️ {_al}")
                            else:
                                st.success("✅ Sem inconsistências: origem e destino dentro dos limites esperados e snap dentro do limiar.")
                        # [SNAP-MITIGA] Mitigação de snap excessivo (quando acionada)
                        _mit = _aud.get("mitigacao_snap")
                        if isinstance(_mit, dict):
                            st.markdown("**🎯 Mitigação de snap excessivo**")
                            if _mit.get("aplicada"):
                                _oa, _oq = _mit.get("snap_origem_antes_m"), _mit.get("snap_origem_depois_m")
                                _da, _dq = _mit.get("snap_destino_antes_m"), _mit.get("snap_destino_depois_m")
                                _ka, _kq = _mit.get("km_antes"), _mit.get("km_depois")
                                _mc1, _mc2 = st.columns(2)
                                with _mc1:
                                    if _mit.get("origem_melhorada") and _oa is not None and _oq is not None:
                                        st.write(f"**Origem — snap:** {_oa:.0f} m → **{_oq:.0f} m**")
                                    if _mit.get("destino_melhorado") and _da is not None and _dq is not None:
                                        st.write(f"**Destino — snap:** {_da:.0f} m → **{_dq:.0f} m**")
                                with _mc2:
                                    if _ka is not None and _kq is not None:
                                        st.write(f"**Rota OSRM:** {_ka} km → **{_kq} km**")
                                    st.caption(f"Coord. OSRM origem: {_mit.get('coord_osrm_origem','—')}")
                                    st.caption(f"Coord. OSRM destino: {_mit.get('coord_osrm_destino','—')}")
                                st.success("✅ Coordenada road-adjacent mais representativa selecionada (menor snap dentro da UF) e OSRM re-roteado.")
                            else:
                                st.info(f"ℹ️ Mitigação tentada, sem melhora: {_mit.get('motivo','—')}")
                            # Candidatos considerados (transparência total)
                            def _tabela_cand(_lst, _titulo):
                                if _lst:
                                    st.caption(f"**{_titulo}** — candidatos avaliados (por provedor):")
                                    _linhas = [{"Fonte": c.get("fonte","—"),
                                                "Coordenada": f"{round(c.get('lat',0),5)}, {round(c.get('lon',0),5)}",
                                                "Snap (m)": c.get("snap_m")} for c in _lst]
                                    st.dataframe(_linhas, use_container_width=True, hide_index=True)
                            _tabela_cand(_mit.get("candidatos_origem"), "Origem")
                            _tabela_cand(_mit.get("candidatos_destino"), "Destino")
                        st.divider()
                        _cons = _aud.get("consenso", {})
                        st.markdown("##### 4️⃣ Consenso e divergência entre motores")
                        _cc1, _cc2, _cc3 = st.columns(3)
                        _cc1.metric("Motor vencedor", _cons.get("vencedor", "—"))
                        _cc2.metric("Divergência (km)", f"{_cons.get('divergencia_km')}" if _cons.get('divergencia_km') is not None else "—")
                        _cc3.metric("Divergência (%)", f"{_cons.get('divergencia_pct')}%" if _cons.get('divergencia_pct') is not None else "—")
                        st.caption("💡 As coordenadas enviadas ao OSRM são **idênticas** às coordenadas validadas acima; o Google recebe o "
                                   "**nome oficial** correspondente à mesma geocodificação. Ambos operam sobre a mesma localidade validada — "
                                   "a diferença remanescente vem do **snap** do OSRM à malha viária, agora medido e validado acima.")

                url_iframe = res_ind[29]
                _fonte_rota_ui = res_ind[5] if len(res_ind) > 5 else "N/A"
                _link_osrm_viewer = res_ind[36] if len(res_ind) > 36 else ""
                _eh_geodesico_ui = "GEOD" in str(_fonte_rota_ui).upper()
                _eh_osrm_ui = "OSRM" in str(_fonte_rota_ui).upper()
                _eh_google_ui = (not _eh_geodesico_ui) and (not _eh_osrm_ui)
                _eh_mapa_leaflet = isinstance(url_iframe, str) and url_iframe.startswith("data:text/html;base64,")
                # [VIS-DINAMICA - 30ª geração] APRESENTAÇÃO DINÂMICA POR PROVEDOR VENCEDOR:
                #   • GOOGLE vence → mapa embarcado EXCLUSIVAMENTE do Google (iframe http) + 1 link (Google).
                #   • OSRM vence   → mapa embarcado EXCLUSIVAMENTE do OSRM (Leaflet) + 2 links (Google + visualizador OSRM).
                #   • Geodésico    → ligação direta estimada (Leaflet) + 1 link + aviso.
                # Mapa e link sempre representam a MESMA rota (construídos dos mesmos parâmetros).
                if _eh_google_ui and not _eh_mapa_leaflet:
                    # ---------- CENÁRIO 1: GOOGLE VENCE (mapa do PRÓPRIO Google, 1 link) ----------
                    # [VIS-GOOGLE-EMBED - 32ª geração] Renderiza o embed do Google num <iframe>
                    # com os atributos OFICIALMENTE recomendados pela doc da Maps Embed API:
                    # referrerpolicy (p/ a restrição de chave por referrer funcionar), allowfullscreen
                    # (usuário pode expandir o mapa) e loading="lazy" (carrega só quando visível).
                    try:
                        _src_embed = str(url_iframe).replace("&", "&amp;")
                        components.html(
                            f'<iframe src="{_src_embed}" width="100%" height="470" '
                            f'style="border:0;display:block" allowfullscreen loading="lazy" '
                            f'referrerpolicy="strict-origin-when-cross-origin"></iframe>',
                            height=476)
                    except Exception:
                        st.warning("Renderização de mapa bloqueada pelas políticas de segurança do navegador.")
                    st.caption("🗺️ Mapa acima: **Google Maps** — rota traçada, origem e destino pelo nome.")
                    st.markdown(f"🧭 [Abrir rota no Google Maps]({res_ind[2]})")
                    _aviso_chave = "" if GOOGLE_MAPS_EMBED_API_KEY else (
                        " _(Dica: configure `GOOGLE_MAPS_EMBED_API_KEY` nos secrets para usar a Maps Embed API oficial — garante 100% o traçado da rota.)_")
                    st.caption("ℹ️ **Google Maps venceu (menor distância).** O **mapa embarcado** e o **link** são ambos do "
                               "**Google** e representam exatamente a **mesma rota** (abrem pelos **nomes** de origem e destino) — "
                               "100% auditável. Há um **único link**, do Google." + _aviso_chave)
                elif _eh_mapa_leaflet:
                    # ---------- CENÁRIOS 2 e 3: OSRM vence / Geodésico (Leaflet autocontido) ----------
                    try:
                        import base64 as _b64dec
                        _html_mapa = _b64dec.b64decode(url_iframe.split(",", 1)[1]).decode("utf-8")
                        components.html(_html_mapa, height=470, scrolling=False)
                    except Exception:
                        st.warning("Renderização de mapa localmente bloqueada pelas políticas de segurança do navegador.")
                    if _eh_geodesico_ui:
                        _prov_nome, _arq_nome = "Projeção Geodésica", "rota_estimada.html"
                        st.caption(f"🗺️ Mapa acima: **{_prov_nome}** — ligação direta origem→destino (estimativa), identificadas pelo nome.")
                    else:
                        _prov_nome, _arq_nome = "OSRM", "rota_osrm_tracada.html"
                        st.caption(f"🗺️ Mapa acima: **OSRM** com o **traçado da rota desenhado** — origem e destino pelo nome.")
                    if _eh_osrm_ui:
                        # DOIS links: (1) Google comparativo, (2) visualizador OSRM (reproduz este mapa).
                        cbtn1, cbtn2 = st.columns(2)
                        with cbtn1:
                            st.markdown(f"🧭 [Google Maps (comparação)]({res_ind[2]})")
                        with cbtn2:
                            if _link_osrm_viewer:
                                st.markdown(f'<a href="{_link_osrm_viewer}" target="_blank" rel="noopener" '
                                            f'style="text-decoration:none">🛰️ <b>Visualizador OSRM</b> (mesma rota)</a>',
                                            unsafe_allow_html=True)
                            else:
                                st.caption("🛰️ Rota muito longa p/ link — use o **download** abaixo (traçado exato OSRM).")
                        try:
                            import base64 as _b64dl
                            _html_dl = _b64dl.b64decode(url_iframe.split(",", 1)[1]).decode("utf-8")
                            st.download_button(f"⬇️ Baixar mapa (OSRM) — HTML", data=_html_dl,
                                               file_name=_arq_nome, mime="text/html",
                                               help="Mapa autocontido com o traçado exato do OSRM. Abre offline em qualquer navegador.",
                                               use_container_width=True)
                        except Exception:
                            pass
                        st.caption("ℹ️ **OSRM venceu (menor distância).** Mapa embarcado **exclusivamente do OSRM** (geometria exata, nomes). "
                                   "**Dois links:** o **Google Maps** (comparação) e o **Visualizador OSRM** — que abre num link próprio do app e "
                                   "reproduz **fielmente este mesmo mapa** (mesma geometria, mesmos nomes). Veja também o **comparativo** abaixo.")
                    else:
                        # Geodésico: 1 link + download + aviso.
                        cbtn1, cbtn2 = st.columns(2)
                        with cbtn1:
                            st.markdown(f"🧭 [Abrir rota no Google Maps]({res_ind[2]})")
                        with cbtn2:
                            try:
                                import base64 as _b64dl2
                                _html_dl2 = _b64dl2.b64decode(url_iframe.split(",", 1)[1]).decode("utf-8")
                                st.download_button(f"⬇️ Baixar mapa (estimativa) — HTML", data=_html_dl2,
                                                   file_name=_arq_nome, mime="text/html",
                                                   help="Mapa autocontido. Abre offline em qualquer navegador.",
                                                   use_container_width=True)
                            except Exception:
                                pass
                        st.warning("📐 **Distância estimada (Projeção Geodésica):** nenhum motor viário retornou a rota no momento, então "
                                   "a quilometragem foi **estimada** pela linha reta × fator de desvio rodoviário (o mapa mostra a ligação "
                                   "direta). Recomenda-se **reprocessar** quando os motores responderem, para obter a rota viária oficial.")
                else:
                    # Rede de segurança rara: link_embed http inesperado. Usa iframe + link Google.
                    try:
                        components.iframe(url_iframe, height=470, scrolling=True)
                    except Exception:
                        st.warning("Renderização de mapa localmente bloqueada pelas políticas de segurança do navegador.")
                    st.markdown(f"🗺️ [Abrir rota no Google Maps]({res_ind[2]})")

                # [VIS-DUAL - 37ª geração] BLOCO COMPARATIVO — sempre exibe o MAPA + LINK do
                # OUTRO provedor, para que as DUAS rotas (Google e OSRM) sejam sempre visíveis.
                # Atende ao pedido: "independentemente de quem vencer, sempre visualizar as duas
                # rotas". Aditivo (não altera o bloco do vencedor acima). Não aparece no fallback
                # geodésico (só há uma estimativa, sem segundo motor para comparar).
                _mapa_comp = res_ind[37] if len(res_ind) > 37 else ""
                _link_comp = res_ind[38] if len(res_ind) > 38 else ""
                if _mapa_comp and not _eh_geodesico_ui:
                    _comp_prov = "OSRM" if _eh_google_ui else "Google Maps"
                    _win_prov = "Google Maps" if _eh_google_ui else "OSRM"
                    st.write("")
                    with st.container(border=True):
                        st.markdown(f"##### 🔀 Rota comparativa — **{_comp_prov}** _(motor não vencedor)_")
                        st.caption(f"O mapa principal acima é do vencedor (**{_win_prov}**). Abaixo, a MESMA origem e destino "
                                   f"traçados pelo **{_comp_prov}**, para comparação lado a lado — assim você audita as **duas** rotas.")
                        _comp_eh_leaflet = isinstance(_mapa_comp, str) and _mapa_comp.startswith("data:text/html;base64,")
                        if _comp_eh_leaflet:
                            # Comparativo = OSRM (Leaflet autocontido, geometria exata)
                            try:
                                import base64 as _b64c
                                components.html(_b64c.b64decode(_mapa_comp.split(",", 1)[1]).decode("utf-8"),
                                                height=420, scrolling=False)
                            except Exception:
                                st.warning("Renderização do mapa comparativo bloqueada pelo navegador.")
                            st.caption("🗺️ Mapa comparativo: **OSRM** — geometria exata da rota, origem/destino pelo nome.")
                            _cbc1, _cbc2 = st.columns(2)
                            with _cbc1:
                                if _link_comp:
                                    st.markdown(f'<a href="{_link_comp}" target="_blank" rel="noopener" '
                                                f'style="text-decoration:none">🛰️ <b>Visualizador OSRM</b> (rota comparativa)</a>',
                                                unsafe_allow_html=True)
                                else:
                                    st.caption("🛰️ Rota longa p/ link — use o **download** ao lado.")
                            with _cbc2:
                                try:
                                    import base64 as _b64cd
                                    st.download_button("⬇️ Baixar mapa comparativo (OSRM) — HTML",
                                                       data=_b64cd.b64decode(_mapa_comp.split(",", 1)[1]).decode("utf-8"),
                                                       file_name="rota_osrm_comparativa.html", mime="text/html",
                                                       use_container_width=True,
                                                       help="Mapa autocontido com o traçado exato do OSRM. Abre offline em qualquer navegador.")
                                except Exception:
                                    pass
                        else:
                            # Comparativo = Google (embed URL, rota traçada pelos nomes)
                            try:
                                _src_c = str(_mapa_comp).replace("&", "&amp;")
                                components.html(f'<iframe src="{_src_c}" width="100%" height="420" '
                                                f'style="border:0;display:block" allowfullscreen loading="lazy" '
                                                f'referrerpolicy="strict-origin-when-cross-origin"></iframe>', height=426)
                            except Exception:
                                st.warning("Renderização do mapa comparativo bloqueada pelo navegador.")
                            st.caption("🗺️ Mapa comparativo: **Google Maps** — rota traçada, origem/destino pelo nome.")
                            if _link_comp:
                                st.markdown(f"🧭 [Abrir rota comparativa no Google Maps]({_link_comp})")
                        st.caption(f"⚖️ Consulte o painel **Comparativo entre Provedores** (acima) para as métricas de "
                                   f"distância, tempo e divergência entre **{_win_prov}** (vencedor) e **{_comp_prov}** (comparativo).")
            else:
                st.error("Falha na validação de consistência geodésica unificada.")
        else:
            st.warning("Preencha origem e destino para inicializar o cálculo.")

with tab_processamento:
    st.info("⚙️ **Objetivo desta aba:** Processamento em massa O(U). Envie uma planilha Excel com milhares de origens e destinos. O sistema extrairá rotas únicas, calculará os desvios de todas simultaneamente e devolverá a planilha rigorosamente preenchida.")
    renderizar_guia_aba("processamento")
    arquivo_carregado = st.file_uploader("Selecionar Arquivo Excel", type=["xlsx"], key="lote_std", help="A planilha deve conter as colunas 'Origem' e 'Destino'.")
    if arquivo_carregado is not None:
        df = pd.read_excel(arquivo_carregado, engine='calamine')
        df.columns = df.columns.str.strip().str.title()
        
        if 'Origem' not in df.columns or 'Destino' not in df.columns:
            st.error("Erro de Validação: A planilha deve possuir as colunas 'Origem' e 'Destino'.")
        else:
            # [P31 - 3ª geração] Limite expandido de 5.000 → 100.000 linhas com avisos
            # graduais por faixa. O gargalo real é rede (não CPU/RAM até ~100k), então
            # o teto rígido anterior era conservador demais. Chunking implícito via
            # deduplicação O(U) + pool de threads já garante estabilidade de memória.
            MAX_LINHAS_ABSOLUTO = 100000   # teto físico (RAM ~600MB de pico)
            MAX_LINHAS_CONFORTAVEL = 10000  # faixa sem avisos
            MAX_LINHAS_ATENCAO = 50000      # faixa com aviso de tempo
            n_linhas = len(df)

            if n_linhas > MAX_LINHAS_ABSOLUTO:
                st.error(f"⚠️ Limite máximo de {MAX_LINHAS_ABSOLUTO:,} linhas excedido ({n_linhas:,} enviadas). "
                         f"Para volumes maiores, fracione o arquivo ou utilize o processamento incremental "
                         f"(o cache persistente reaproveita rotas já calculadas entre os fragmentos).")
                st.stop()
            elif n_linhas > MAX_LINHAS_ATENCAO:
                st.warning(f"📊 Volume alto: {n_linhas:,} linhas. O processamento é viável, mas pode levar "
                           f"dezenas de minutos (o gargalo é a latência das APIs externas, não o seu computador). "
                           f"Rotas repetidas e já calculadas em lotes anteriores são reaproveitadas do cache automaticamente.")
            elif n_linhas > MAX_LINHAS_CONFORTAVEL:
                st.info(f"📈 Volume moderado: {n_linhas:,} linhas. Processamento dentro da faixa estável.")
                
            st.success(f"Tabela com {n_linhas:,} registros mapeada! Pronto para processar o Lote Unificado.")
            
            # [SPEED-2 / Etapa 5] Estimativa dinâmica de tempo ANTES de processar.
            # [PERF-UI1] A contagem de rotas únicas agora é cacheada pela identidade do
            # arquivo, evitando recomputar set(zip(...)) sobre 100k linhas a cada rerun.
            _file_id = f"{getattr(arquivo_carregado, 'name', 'file')}_{getattr(arquivo_carregado, 'size', 0)}"
            _n_rotas_unicas_prev = _contar_rotas_unicas_preview(
                _file_id, n_linhas,
                tuple(df['Origem'].fillna('').astype(str).values),
                tuple(df['Destino'].fillna('').astype(str).values)
            )
            _est_txt, _est_base, _est_n, _est_por_rota = estimar_tempo_processamento(_n_rotas_unicas_prev, tipo="lote")
            if _est_txt:
                with st.container(border=True):
                    ce1, ce2 = st.columns([60, 40])
                    with ce1:
                        st.metric("⏱️ Tempo Estimado de Processamento", _est_txt,
                                  help="Estimativa baseada no histórico real de execuções anteriores. Fica mais precisa a cada lote processado.")
                    with ce2:
                        st.metric("Rotas Únicas a Processar", f"{_n_rotas_unicas_prev:,}",
                                  help="O sistema processa apenas rotas exclusivas (deduplicação O(U)). Rotas repetidas são reaproveitadas.")
                    if _est_n > 0:
                        st.caption(f"📊 Estimativa calibrada com **{_est_n} execução(ões) real(is)** do histórico "
                                   f"(~{_est_por_rota:.2f}s/rota, ponderado para execuções recentes). "
                                   f"Quanto mais você usa, mais precisa fica.")
                    else:
                        st.caption(f"📊 Primeira estimativa ({_est_base}). Após este lote, as próximas estimativas "
                                   f"usarão seus dados reais de desempenho.")
            
            nome_operador = st.text_input("Matrícula / Nome do Operador (Opcional)", max_chars=50)
            
            # ==================================================================
            # [FIX-LOTE - 13ª geração] MOTOR DE PROCESSAMENTO CONTÍNUO EM CHUNKS
            # ------------------------------------------------------------------
            # CAUSA RAIZ do bug "para no meio e exige novo clique": o processamento
            # rodava INTEIRO dentro de `if st.button(...)` de forma síncrona. Em
            # planilhas grandes isso executa por minutos/horas numa única execução
            # do script Streamlit. O Streamlit mantém o estado do botão via WebSocket;
            # execuções muito longas estouram o timeout do WebSocket → o navegador
            # perde a conexão, o estado do botão reverte para False e, ao reconectar,
            # o processamento não retoma (o botão não está mais "pressionado") →
            # exige novo clique. Além disso, qualquer exceção no meio perdia todo o
            # progresso (df_processado nunca era setado).
            #
            # SOLUÇÃO: máquina de estados em chunks com checkpoint em session_state.
            # Processa ~200 rotas por execução, salva o progresso e chama st.rerun().
            # Cada rerun é uma execução CURTA → o WebSocket nunca estoura. Um único
            # clique inicia tudo; os chunks seguintes rodam automaticamente via rerun.
            # Se interromper, o estado persiste e retoma do último chunk concluído.
            # ==================================================================
            CHUNK_SIZE = 200  # rotas por execução — curto o bastante p/ não estourar WebSocket
            _proc_ativo = st.session_state.get('lote_em_andamento', False)
            
            _clicou_iniciar = st.button(
                "🚀 Iniciar Processamento em Lote", type="primary",
                disabled=_proc_ativo,
                help="Inicia o processamento contínuo. Um único clique processa toda a planilha automaticamente."
            )
            
            # Botão de cancelamento visível durante o processamento
            if _proc_ativo:
                if st.button("⏹️ Cancelar Processamento", help="Interrompe o processamento contínuo e descarta o progresso atual."):
                    for _k in ['lote_em_andamento', 'lote_fase', 'lote_endpoints', 'lote_preaq_idx',
                               'lote_tarefas', 'lote_resultados', 'lote_chunk_idx',
                               'lote_df_base', 'lote_start_clock', 'lote_total', 'lote_operador',
                               'lote_preaquecido', 'lote_runner_map']:
                        st.session_state.pop(_k, None)
                    st.warning("Processamento cancelado pelo usuário.")
                    st.rerun()
            
            # ---- FASE 1: INICIALIZAÇÃO (no clique) ----
            if _clicou_iniciar and not _proc_ativo:
                novas_colunas = NOVAS_COLUNAS_PADRAO
                colunas_numericas = COLUNAS_NUMERICAS_PADRAO
                for col in novas_colunas:
                    if col in colunas_numericas:
                        if col not in df.columns:
                            df[col] = 0.0
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)
                    else:
                        if col not in df.columns:
                            df[col] = "Não Informado"
                        df[col] = df[col].astype(object)
                        
                # [P30] Extração vetorizada de pares únicos (31x vs iterrows)
                _orig_s = df['Origem'].fillna('').astype(str).str.strip()
                _dest_s = df['Destino'].fillna('').astype(str).str.strip()
                _mask_validos = (
                    (_orig_s != '') & (_dest_s != '') &
                    (_orig_s.str.lower() != 'nan') & (_dest_s.str.lower() != 'nan')
                )
                pares_unicos = set(zip(_orig_s[_mask_validos], _dest_s[_mask_validos]))
                
                if not pares_unicos:
                    st.warning("Nenhuma linha contendo endereços válidos detectada após sanitização.")
                    st.stop()
                    
                MAPA_PRIORIDADE = MAPA_PRIORIDADE_GLOBAL
                tarefas_priorizadas = []
                for p in pares_unicos:
                    tipo_o = semantica.classificar_entrada(semantica.normalizar(p[0]))
                    tarefas_priorizadas.append((MAPA_PRIORIDADE.get(tipo_o, 99), p))
                tarefas_priorizadas.sort(key=lambda x: x[0])
                
                # [SPEED-1 + FLUXO-CONTINUO - 38ª geração] Endpoints únicos para pré-aquecimento
                # de geocodificação. NÃO é mais feito de forma síncrona aqui: uma pré-carga longa
                # (planilhas grandes têm dezenas de milhares de endpoints) também estourava o
                # WebSocket ANTES do st.rerun() — segunda causa do "para e exige novo clique".
                # Agora é uma FASE de pré-aquecimento time-boxed (curtas execuções + rerun).
                endpoints_unicos = set()
                for _o, _d in pares_unicos:
                    endpoints_unicos.add(_o)
                    endpoints_unicos.add(_d)
                _houve_preaquecimento = len(endpoints_unicos) < len(pares_unicos) * 1.8

                # Persiste o estado inicial e dispara o motor CONTÍNUO via rerun (execuções curtas).
                st.session_state['lote_em_andamento'] = True
                st.session_state['lote_fase'] = 'preaquecer' if _houve_preaquecimento else 'processar'
                st.session_state['lote_endpoints'] = list(endpoints_unicos)
                st.session_state['lote_preaq_idx'] = 0
                st.session_state['lote_tarefas'] = tarefas_priorizadas
                st.session_state['lote_resultados'] = {}
                st.session_state['lote_chunk_idx'] = 0
                st.session_state['lote_df_base'] = df.copy()
                st.session_state['lote_start_clock'] = time.time()
                st.session_state['lote_total'] = len(pares_unicos)
                st.session_state['lote_operador'] = nome_operador
                st.session_state['lote_preaquecido'] = _houve_preaquecimento
                st.session_state['lote_runner_map'] = None  # lote padrão não usa runner-up
                st.rerun()
                
            # ---- FASE PRÉ-AQUECIMENTO (time-boxed, a cada rerun automático) ----
            # Geocodifica os endpoints únicos em mini-lotes limitados por ORÇAMENTO DE TEMPO
            # (~8s por execução). Nenhuma execução fica longa a ponto de o WebSocket cair antes
            # do st.rerun(). Continua sozinha até terminar e então passa para a fase de rotas.
            if st.session_state.get('lote_em_andamento', False) and st.session_state.get('lote_fase') == 'preaquecer':
                _eps = st.session_state['lote_endpoints']
                _pidx = st.session_state['lote_preaq_idx']
                _ptotal = len(_eps)
                _ppct = (_pidx / _ptotal) if _ptotal else 1.0
                st.markdown("#### 🔥 Pré-aquecendo a Geocodificação (etapa 1 de 2)")
                st.progress(min(1.0, _ppct))
                st.caption(f"Geocodificando **{_ptotal:,}** endpoints únicos para acelerar o roteamento — "
                           f"{_pidx:,}/{_ptotal:,} concluídos. **O processo continua automaticamente; não clique novamente.**")
                _BUDGET_PRE = 8.0
                _MINI_PRE = max(8, WORKERS_DISPONIVEIS)  # uma onda do pool por mini-lote
                _t_pre = time.time()
                _pidx_local = _pidx
                while _pidx_local < _ptotal:
                    _lote_ep = _eps[_pidx_local:_pidx_local + _MINI_PRE]
                    if not _lote_ep:
                        break
                    _fut_ep = {EXECUTOR_GLOBAL.submit(obter_coordenadas_e_endereco_oficial, ep): ep for ep in _lote_ep}
                    for _f in as_completed(_fut_ep):
                        try:
                            _f.result()
                        except Exception:
                            pass
                    _pidx_local += _MINI_PRE
                    if (time.time() - _t_pre) >= _BUDGET_PRE:
                        break
                st.session_state['lote_preaq_idx'] = min(_pidx_local, _ptotal)
                if st.session_state['lote_preaq_idx'] >= _ptotal:
                    st.session_state['lote_fase'] = 'processar'
                time.sleep(0.05)
                st.rerun()

            # ---- FASE 2: PROCESSAMENTO TIME-BOXED DE ROTAS (a cada rerun automático) ----
            if st.session_state.get('lote_em_andamento', False) and st.session_state.get('lote_fase') == 'processar':
                _tarefas = st.session_state['lote_tarefas']
                _total = st.session_state['lote_total']
                _idx = st.session_state['lote_chunk_idx']
                _resultados = st.session_state['lote_resultados']
                _runner_map = st.session_state.get('lote_runner_map')
                _total_chunks = max(1, math.ceil(_total / CHUNK_SIZE))
                _chunk_atual_num = _idx // CHUNK_SIZE + 1
                
                # Painel de monitoramento ao vivo (atualiza a cada chunk)
                _feitos = len(_resultados)
                _restantes = _total - _feitos
                _pct = (_feitos / _total) if _total else 1.0
                _elapsed = time.time() - st.session_state['lote_start_clock']
                _taxa = (_feitos / _elapsed) if _elapsed > 0 and _feitos > 0 else 0.0
                _eta_seg = (_restantes / _taxa) if _taxa > 0 else 0.0
                
                st.markdown("#### ⚙️ Processamento Contínuo em Andamento")
                st.progress(min(1.0, _pct))
                _mon1, _mon2, _mon3, _mon4 = st.columns(4)
                _mon1.metric("Processados", f"{_feitos:,} / {_total:,}", help="Rotas únicas já processadas / total.")
                _mon2.metric("Restantes", f"{_restantes:,}", help="Rotas únicas ainda pendentes.")
                _mon3.metric("Concluído", f"{_pct*100:.1f}%", help="Percentual concluído.")
                _mon4.metric("Lote Atual", f"{_chunk_atual_num} / {_total_chunks}", help="Chunk atual / total de chunks.")
                _mon5, _mon6, _mon7, _mon8 = st.columns(4)
                _mon5.metric("Tempo Decorrido", _formatar_duracao(_elapsed), help="Tempo desde o início do processamento.")
                _mon6.metric("Velocidade", f"{_taxa:.1f} rotas/s", help="Velocidade média de processamento.")
                _mon7.metric("Rotas/min", f"{_taxa*60:.0f}", help="Rotas processadas por minuto.")
                _mon8.metric("Tempo Restante (ETA)", _formatar_duracao(_eta_seg) if _taxa > 0 else "calculando...", help="Estimativa para concluir, baseada na velocidade atual.")
                st.caption("🔄 O processamento avança automaticamente. **Não é necessário clicar novamente** — cada lote continua sozinho até o fim. "
                           "Você pode cancelar a qualquer momento no botão acima.")
                
                # [FLUXO-CONTINUO - 38ª geração] Processamento TIME-BOXED por ORÇAMENTO DE TEMPO
                # de parede (não por nº fixo de rotas). Cada execução processa mini-lotes até
                # esgotar ~8s e então dá rerun. Antes, um chunk fixo de 200 rotas aguardava a
                # rota mais lenta — em redes lentas / volumes grandes a execução ficava longa
                # demais e o WebSocket do Streamlit Cloud caía ANTES do st.rerun(), deixando a
                # tela "parada" à espera de clique. Agora a execução é sempre curta e adaptativa:
                # rede rápida → muitas rotas por execução; rede lenta → menos rotas, mas execução
                # curta e conexão sempre "quente" (cada rerun troca mensagens e reseta o timeout).
                _BUDGET_SEG = 8.0
                _MINI = max(8, WORKERS_DISPONIVEIS)  # uma onda do pool por mini-lote
                _t_run = time.time()
                _idx_local = _idx
                _processou_algo = False
                while _idx_local < _total:
                    _mini = _tarefas[_idx_local:_idx_local + _MINI]
                    if not _mini:
                        break
                    try:
                        _res_mini = processar_chunk_rotas(_mini, runner_up_map=_runner_map)
                        _resultados.update(_res_mini)
                        _processou_algo = True
                    except Exception as e:
                        # Isola falha do mini-lote: registra e continua (não encerra o lote)
                        logger.error(f"[FLUXO-CONTINUO] Erro em mini-lote (idx {_idx_local}), isolado: {e}")
                    _idx_local += _MINI
                    if (time.time() - _t_run) >= _BUDGET_SEG:
                        break
                if _processou_algo:
                    st.session_state['lote_resultados'] = _resultados
                st.session_state['lote_chunk_idx'] = min(_idx_local, _total)

                # Mais rotas? Continua automaticamente. Senão, finaliza.
                if st.session_state['lote_chunk_idx'] < _total:
                    time.sleep(0.05)  # micro-pausa p/ o Streamlit liberar o WebSocket
                    st.rerun()
                else:
                    # ---- FASE 3: FINALIZAÇÃO (todos os chunks concluídos) ----
                    _df_base = st.session_state['lote_df_base']
                    _operador = st.session_state.get('lote_operador', '')
                    _preaq = st.session_state.get('lote_preaquecido', False)
                    _start_clock = st.session_state['lote_start_clock']
                    
                    df_final = _montar_dataframe_final(_df_base, _resultados, runner_up_map=_runner_map)
                    
                    # Recalcula Linha Reta vetorizada (Haversine IUGG)
                    lat_o = np.radians(df_final['Lat Origem'].astype(float).values)
                    lon_o = np.radians(df_final['Lon Origem'].astype(float).values)
                    lat_d = np.radians(df_final['Lat Destino'].astype(float).values)
                    lon_d = np.radians(df_final['Lon Destino'].astype(float).values)
                    dlat = lat_d - lat_o; dlon = lon_d - lon_o
                    a = np.sin(dlat / 2.0)**2 + np.cos(lat_o) * np.cos(lat_d) * np.sin(dlon / 2.0)**2
                    c = 2 * np.arcsin(np.sqrt(a))
                    distancias_vetorizadas = 6371.0088 * c
                    mask_validas = (df_final['Lat Origem'] != 0.0) & (df_final['Lat Destino'] != 0.0)
                    df_final.loc[mask_validas, 'Linha Reta'] = np.round(distancias_vetorizadas[mask_validas], 2)
                    df_final.loc[mask_validas, 'Status Linha Reta'] = "Calculada via Haversine Vetorizado"
                    
                    tempo_lote_segundos = round(time.time() - _start_clock, 2)
                    cache_historico_lotes.set(f"lote_{_start_clock}", {
                        "Data/Hora": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "Operador": _operador.strip() if _operador.strip() else "Operador Padrão",
                        "Linhas Validadas": _total,
                        "Tempo Gasto (s)": tempo_lote_segundos,
                        "Tempo Médio/Rota (s)": round(tempo_lote_segundos / max(1, _total), 2)
                    }, expire=None)
                    
                    ordem_finais = list(_df_base.columns)
                    for col in NOVAS_COLUNAS_PADRAO:
                        if col not in ordem_finais:
                            ordem_finais.append(col)
                    df_final = df_final.reindex(columns=ordem_finais)
                    
                    # [SPEED-3] Exportação xlsxwriter (~1.7x vs openpyxl)
                    output_buffer = io.BytesIO()
                    with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
                        df_final.to_excel(writer, index=False)
                    st.session_state['planilha_pronta'] = output_buffer.getvalue()
                    st.session_state['df_processado'] = df_final
                    st.session_state['lote_tempo_total'] = tempo_lote_segundos
                    st.session_state['lote_preaquecido_final'] = _preaq
                    
                    # Limpa o estado de processamento (libera RAM dos checkpoints)
                    for _k in ['lote_em_andamento', 'lote_fase', 'lote_endpoints', 'lote_preaq_idx',
                               'lote_tarefas', 'lote_resultados', 'lote_chunk_idx',
                               'lote_df_base', 'lote_start_clock', 'lote_total', 'lote_operador',
                               'lote_preaquecido', 'lote_runner_map']:
                        st.session_state.pop(_k, None)
                    st.rerun()
                    
        if 'df_processado' in st.session_state and 'planilha_pronta' in st.session_state:
            # Painel de performance do último lote (após finalização)
            if 'lote_tempo_total' in st.session_state:
                _tempo_lote = st.session_state['lote_tempo_total']
                _df_fin = st.session_state['df_processado']
                st.success("✨ Processamento em lote concluído com êxito! Todos os registros foram processados automaticamente.")
                with st.container(border=True):
                    st.markdown("#### ⚡ Monitoramento de Performance deste Lote")
                    _med_geo = float(_df_fin['Tempo Geocoding (s)'].mean()) if 'Tempo Geocoding (s)' in _df_fin.columns else 0.0
                    _med_rot = float(_df_fin['Tempo Roteamento (s)'].mean()) if 'Tempo Roteamento (s)' in _df_fin.columns else 0.0
                    _med_tot = float(_df_fin['Tempo Total (s)'].mean()) if 'Tempo Total (s)' in _df_fin.columns else 0.0
                    cmp1, cmp2, cmp3, cmp4 = st.columns(4)
                    cmp1.metric("Tempo Total Real", _formatar_duracao(_tempo_lote))
                    cmp2.metric("Médio Geocodificação/Rota", f"{_med_geo:.2f}s")
                    cmp3.metric("Médio Roteamento/Rota", f"{_med_rot:.2f}s")
                    cmp4.metric("Médio Total/Rota", f"{_med_tot:.2f}s")
                    if _med_geo > _med_rot * 1.3:
                        st.caption("🔍 **Etapa dominante:** Geocodificação.")
                    elif _med_rot > _med_geo * 1.3:
                        st.caption("🔍 **Etapa dominante:** Roteamento.")
                    else:
                        st.caption("🔍 **Etapas equilibradas.**")
                if st.session_state.get('lote_preaquecido_final', False):
                    st.caption("🔥 **Pré-aquecimento ativo:** geocodificação antecipada eliminou chamadas redundantes.")
                st.balloons()  # celebra só na primeira exibição pós-conclusão
                st.session_state.pop('lote_tempo_total', None)  # mostra painel só uma vez
                st.session_state.pop('lote_preaquecido_final', None)
            st.write("---")
            # [F-NEW1] Scorecard de qualidade — indicadores agregados antes da prévia bruta
            renderizar_scorecard_qualidade(st.session_state['df_processado'])
            st.write("---")
            st.markdown("### 📋 Prévia Interativa da Planilha Final")
            st.dataframe(st.session_state['df_processado'], use_container_width=True, height=250)
            col_down1, col_down2 = st.columns(2)
            with col_down1:
                st.download_button(label="📥 Baixar Planilha (.xlsx)", data=st.session_state['planilha_pronta'], file_name="planilha_rotas_calculada.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
            with col_down2:
                st.markdown("""<a href="https://sheets.new/" target="_blank" style="display:inline-block; padding:0.5em 1em; background-color:#1E90FF; color:white; border-radius:5px; text-decoration:none; font-weight:bold; text-align:center; width:100%; transition: all 0.2s;">📊 Abrir Google Sheets Vazio</a>""", unsafe_allow_html=True)
            
            # [EXPORT-GIS - 24ª geração] Exportações avançadas para sistemas GIS/geográficos.
            # Aproveita as coordenadas JÁ calculadas (zero custo de processamento). Permite
            # integração direta com QGIS, ArcGIS, Google Earth, Power BI, GPS e mais.
            _df_exp = st.session_state['df_processado']
            _n_geo = _contar_rotas_geo_validas(_df_exp)
            with st.expander(f"🌍 Exportações Avançadas para GIS ({_n_geo} rotas georreferenciadas)", expanded=False):
                if _n_geo == 0:
                    st.info("Nenhuma rota com coordenadas válidas para exportação geográfica neste lote.")
                else:
                    st.caption("Formatos abertos para integração com ferramentas geoespaciais. As coordenadas já foram "
                               "calculadas no processamento — estas exportações são instantâneas e não afetam o desempenho.")
                    cexp1, cexp2, cexp3, cexp4 = st.columns(4)
                    with cexp1:
                        st.download_button("📄 CSV", data=_df_exp.to_csv(index=False).encode('utf-8'),
                                           file_name="rotas.csv", mime="text/csv", use_container_width=True,
                                           help="Planilha em texto (Excel, Power BI, Tableau, pandas).")
                    with cexp2:
                        st.download_button("🌐 GeoJSON", data=_df_para_geojson(_df_exp).encode('utf-8'),
                                           file_name="rotas.geojson", mime="application/geo+json", use_container_width=True,
                                           help="Padrão aberto (RFC 7946) para QGIS, ArcGIS, Mapbox, Leaflet, kepler.gl.")
                    with cexp3:
                        st.download_button("🗺️ KML", data=_df_para_kml(_df_exp).encode('utf-8'),
                                           file_name="rotas.kml", mime="application/vnd.google-earth.kml+xml", use_container_width=True,
                                           help="Para Google Earth e Google My Maps. Abre com duplo-clique.")
                    with cexp4:
                        st.download_button("📍 GPX", data=_df_para_gpx(_df_exp).encode('utf-8'),
                                           file_name="rotas.gpx", mime="application/gpx+xml", use_container_width=True,
                                           help="GPS Exchange Format — dispositivos GPS, Garmin, apps de navegação.")
                    st.caption("💡 **Dica:** o GeoJSON e o KML desenham origem (verde), destino (vermelho) e a linha origem→destino. "
                               "Importe no QGIS/Google Earth para visualizar todas as rotas do lote num mapa só.")

with tab_alocacao:
    st.info("🎯 **Objetivo desta aba:** Inteligência Logística de Hubs. Envie uma lista de clientes (Origens) e uma lista de Centros de Distribuição/Bases (Destinos). O sistema calculará todas as combinações espaciais e descobrirá automaticamente qual é a Base Logística mais próxima de cada cliente individualmente.")
    renderizar_guia_aba("alocacao")
    col_a1, col_a2 = st.columns(2)
    with col_a1: 
        file_dest = st.file_uploader("1. Planilha de Endereços / Entregas (Origens)", type=["xlsx"], key="up_dests_v19")
    with col_a2: 
        file_hubs = st.file_uploader("2. Planilha de Municípios / Bases (Destinos)", type=["xlsx"], key="up_hubs_v19")
        
    if file_hubs and file_dest:
        df_hubs = pd.read_excel(file_hubs, engine='calamine')
        df_dest = pd.read_excel(file_dest, engine='calamine')
        
        col_s1, col_s2 = st.columns(2)
        with col_s1: 
            dest_col_name = st.selectbox("Selecione a coluna que contém os Endereços (Origens):", df_dest.columns)
        with col_s2: 
            hub_col_name = st.selectbox("Selecione a coluna que contém os Municípios/Bases (Destinos):", df_hubs.columns)
            
        # ==================================================================
        # [FIX-ALOC - 14ª geração] MOTOR DE ALOCAÇÃO CONTÍNUO EM CHUNKS
        # ------------------------------------------------------------------
        # Mesma causa raiz do lote padrão (WebSocket timeout em execução longa),
        # AGRAVADA por 3 gargalos seriais: geocodificação de hubs em loop, de
        # destinos em loop, e matriz competitiva O(N×M) em loop aninhado.
        # SOLUÇÃO: máquina de estados em fases com checkpoint em session_state +
        # geocodificação PARALELA + matriz competitiva VETORIZADA + roteamento
        # em chunks com auto-continuação via st.rerun(). Um único clique.
        # ==================================================================
        CHUNK_SIZE_ALO = 200
        _alo_ativo = st.session_state.get('alo_em_andamento', False)
        
        _clicou_alo = st.button(
            "🎯 Processar Cruzamento Espacial e Roteamento Duplo", type="primary",
            disabled=_alo_ativo,
            help="Inicia o processamento contínuo da alocação. Um único clique processa tudo automaticamente."
        )
        
        if _alo_ativo:
            if st.button("⏹️ Cancelar Alocação", key="cancel_alo"):
                for _k in ['alo_em_andamento', 'alo_fase', 'alo_tarefas', 'alo_resultados', 'alo_chunk_idx',
                           'alo_df_pares', 'alo_start_clock', 'alo_total', 'alo_runner_map',
                           'alo_dest_linha_reta', 'alo_dest_status_lr', 'alo_df_dest_cols', 'alo_novas_colunas',
                           'alo_dests_unicos', 'alo_hubs_validos', 'alo_dest_col_name', 'alo_df_dest',
                           'alo_dest_geo_acc', 'alo_dest_geo_idx']:
                    st.session_state.pop(_k, None)
                st.warning("Alocação cancelada pelo usuário.")
                st.rerun()
        
        # ---- FASE 1: INICIALIZAÇÃO + GEOCODIFICAÇÃO PARALELA + MATRIZ VETORIZADA ----
        if _clicou_alo and not _alo_ativo:
            hubs_unicos = df_hubs[hub_col_name].dropna().astype(str).str.strip().unique().tolist()
            dests_unicos = df_dest[dest_col_name].dropna().astype(str).str.strip().unique().tolist()
            
            if not hubs_unicos or not dests_unicos:
                st.error("Uma das colunas selecionadas está vazia ou é inválida.")
            else:
                _prep_bar = st.progress(0)
                _prep_status = st.empty()
                st.session_state['logs_auditoria_alocacao'] = []
                
                # [FIX-ALOC] Geocodificação PARALELA de hubs (era loop serial)
                _prep_status.text(f"🛰️ Fase 1/3: Geocodificando {len(hubs_unicos)} Hubs em paralelo...")
                _prep_bar.progress(0.15)
                hub_geo = geocodificar_endpoints_paralelo(hubs_unicos)
                hub_coords = {h: (v[0], v[1], v[2]) for h, v in hub_geo.items()}
                for h, v in hub_geo.items():
                    st.session_state['logs_auditoria_alocacao'].append({
                        "Categoria": "Base/Hub (Destino)", "Nome Original": h,
                        "Coordenada": f"{v[0]}, {v[1]}", "Endereço Oficializado": v[2],
                        "Score": v[3], "Validação XAI": " | ".join(v[4]) if isinstance(v[4], list) else "N/A"})
                hubs_validos = {k: v for k, v in hub_coords.items() if v[0] != 0.0}
                
                if not hubs_validos:
                    st.error("CRÍTICO: Nenhuma Base/Hub pôde ser geocodificada no mapa.")
                    _prep_status.empty(); _prep_bar.empty()
                else:
                    # [FLUXO-CONTINUO - 39ª geração] A geocodificação dos destinos (que pode ser
                    # MUITO grande) deixa de ser síncrona aqui — era o PONTO DE ESTOL RESTANTE da
                    # Alocação (uma pré-carga longa numa única execução estourava o WebSocket antes
                    # do st.rerun()). Passa a ser uma FASE time-boxed própria ('geo_destinos'), com
                    # mini-lotes de ~8s + rerun. Os hubs (geralmente poucos) já foram geocodificados.
                    _prep_bar.empty(); _prep_status.empty()
                    st.session_state['alo_em_andamento'] = True
                    st.session_state['alo_fase'] = 'geo_destinos'
                    st.session_state['alo_dests_unicos'] = dests_unicos
                    st.session_state['alo_hubs_validos'] = hubs_validos
                    st.session_state['alo_dest_col_name'] = dest_col_name
                    st.session_state['alo_df_dest'] = df_dest
                    st.session_state['alo_novas_colunas'] = NOVAS_COLUNAS_ALOCACAO
                    st.session_state['alo_dest_geo_acc'] = {}
                    st.session_state['alo_dest_geo_idx'] = 0
                    st.session_state['alo_df_dest_cols'] = list(df_dest.columns)
                    st.session_state['alo_start_clock'] = time.time()
                    st.rerun()

        # ---- FASE GEO-DESTINOS (time-boxed): geocodifica os destinos e monta a matriz ----
        # Mesma filosofia do lote: execuções curtas (~8s) que continuam sozinhas até terminar.
        if st.session_state.get('alo_em_andamento', False) and st.session_state.get('alo_fase') == 'geo_destinos':
            _dests = st.session_state['alo_dests_unicos']
            _dgidx = st.session_state['alo_dest_geo_idx']
            _dgtotal = len(_dests)
            _dgacc = st.session_state['alo_dest_geo_acc']
            _dgpct = (_dgidx / _dgtotal) if _dgtotal else 1.0
            st.markdown("#### 🛰️ Geocodificando Endereços de Origem (etapa 1 de 2)")
            st.progress(min(1.0, _dgpct))
            st.caption(f"Validando **{_dgtotal:,}** endereços — {_dgidx:,}/{_dgtotal:,} concluídos. "
                       f"**O processo continua automaticamente; não clique novamente.**")
            _BUDGET_DG = 8.0
            _MINI_DG = max(8, WORKERS_DISPONIVEIS)
            _t_dg = time.time()
            _dgidx_local = _dgidx
            while _dgidx_local < _dgtotal:
                _lote_d = _dests[_dgidx_local:_dgidx_local + _MINI_DG]
                if not _lote_d:
                    break
                try:
                    _res_d = geocodificar_endpoints_paralelo(_lote_d)
                    _dgacc.update(_res_d)
                except Exception as e:
                    logger.error(f"[FLUXO-CONTINUO] Erro em mini-lote de geocodificação de destinos (idx {_dgidx_local}), isolado: {e}")
                _dgidx_local += _MINI_DG
                if (time.time() - _t_dg) >= _BUDGET_DG:
                    break
            st.session_state['alo_dest_geo_acc'] = _dgacc
            st.session_state['alo_dest_geo_idx'] = min(_dgidx_local, _dgtotal)

            if st.session_state['alo_dest_geo_idx'] >= _dgtotal:
                # Geocodificação concluída → matriz competitiva + tarefas (rápido/vetorizado)
                _hubs_validos = st.session_state['alo_hubs_validos']
                _dest_col_name = st.session_state['alo_dest_col_name']
                _df_dest = st.session_state['alo_df_dest']
                _novas_colunas = st.session_state['alo_novas_colunas']
                dest_coords = {d: (v[0], v[1], v[2]) for d, v in _dgacc.items()}
                _logs = st.session_state.get('logs_auditoria_alocacao', [])
                for d, v in _dgacc.items():
                    _logs.append({
                        "Categoria": "Endereço (Origem)", "Nome Original": d,
                        "Coordenada": f"{v[0]}, {v[1]}", "Endereço Oficializado": v[2],
                        "Score": v[3], "Validação XAI": " | ".join(v[4]) if isinstance(v[4], list) else "N/A"})
                st.session_state['logs_auditoria_alocacao'] = _logs
                dest_to_hub, dest_to_linha_reta, dest_to_status_lr, runner_up_map = \
                    calcular_matriz_competitiva_vetorizada(dest_coords, _hubs_validos)
                df_pares = _df_dest.copy()
                df_pares['Origem'] = df_pares[_dest_col_name].astype(str).str.strip()
                df_pares['Destino'] = df_pares['Origem'].map(dest_to_hub).fillna("FALHA_GEO_ORIGEM")
                _colunas_numericas = COLUNAS_NUMERICAS_ALOCACAO
                for col in _novas_colunas:
                    if col in _colunas_numericas:
                        if col not in df_pares.columns:
                            df_pares[col] = 0.0
                        df_pares[col] = pd.to_numeric(df_pares[col], errors='coerce').fillna(0.0).astype(float)
                    else:
                        if col not in df_pares.columns:
                            df_pares[col] = "Não Informado"
                        df_pares[col] = df_pares[col].astype(object)
                _o_alo = df_pares['Origem'].fillna('').astype(str).str.strip()
                _d_alo = df_pares['Destino'].fillna('').astype(str).str.strip()
                _mask_alo = (
                    (_o_alo != '') & (_d_alo != '') &
                    (_o_alo != 'FALHA_GEO_ORIGEM') & (_d_alo != 'NENHUM_HUB_VALIDO') &
                    (_o_alo.str.lower() != 'nan') & (_d_alo.str.lower() != 'nan')
                )
                pares_unicos_alo = set(zip(_o_alo[_mask_alo], _d_alo[_mask_alo]))
                tarefas_priorizadas_alo = []
                for (o, d) in pares_unicos_alo:
                    tipo_o = semantica.classificar_entrada(semantica.normalizar(o))
                    tarefas_priorizadas_alo.append((MAPA_PRIORIDADE_GLOBAL.get(tipo_o, 99), (o, d)))
                tarefas_priorizadas_alo.sort(key=lambda x: x[0])
                st.session_state['alo_tarefas'] = tarefas_priorizadas_alo
                st.session_state['alo_resultados'] = {}
                st.session_state['alo_chunk_idx'] = 0
                st.session_state['alo_df_pares'] = df_pares
                st.session_state['alo_total'] = len(pares_unicos_alo)
                st.session_state['alo_runner_map'] = runner_up_map
                st.session_state['alo_dest_linha_reta'] = dest_to_linha_reta
                st.session_state['alo_dest_status_lr'] = dest_to_status_lr
                st.session_state['alo_df_dest_cols'] = list(_df_dest.columns)
                st.session_state['alo_fase'] = 'processar'
                for _tk in ['alo_dests_unicos', 'alo_hubs_validos', 'alo_dest_col_name',
                            'alo_df_dest', 'alo_dest_geo_acc', 'alo_dest_geo_idx']:
                    st.session_state.pop(_tk, None)
            time.sleep(0.05)
            st.rerun()
        
        # ---- FASE 2: ROTEAMENTO EM CHUNKS (auto-continuação) ----
        if st.session_state.get('alo_em_andamento', False) and st.session_state.get('alo_fase') == 'processar':
            _tarefas = st.session_state['alo_tarefas']
            _total = st.session_state['alo_total']
            _idx = st.session_state['alo_chunk_idx']
            _resultados = st.session_state['alo_resultados']
            _runner_map = st.session_state['alo_runner_map']
            _total_chunks = max(1, math.ceil(_total / CHUNK_SIZE_ALO)) if _total > 0 else 1
            _chunk_num = _idx // CHUNK_SIZE_ALO + 1
            
            _feitos = len(_resultados)
            _restantes = _total - _feitos
            _pct = (_feitos / _total) if _total else 1.0
            _elapsed = time.time() - st.session_state['alo_start_clock']
            _taxa = (_feitos / _elapsed) if _elapsed > 0 and _feitos > 0 else 0.0
            _eta = (_restantes / _taxa) if _taxa > 0 else 0.0
            _tempo_medio_reg = (_elapsed / _feitos) if _feitos > 0 else 0.0
            
            st.markdown("#### 🎯 Alocação Contínua em Andamento")
            st.caption("🧭 **Etapa atual:** Roteamento competitivo (cálculo de rotas origem→hub e duelo com o 2º hub mais próximo)")
            st.progress(min(1.0, _pct))
            _a1, _a2, _a3, _a4 = st.columns(4)
            _a1.metric("Registros Processados", f"{_feitos:,} / {_total:,}", help="Rotas únicas já processadas / total de registros.")
            _a2.metric("Restantes", f"{_restantes:,}", help="Registros ainda pendentes.")
            _a3.metric("Concluído", f"{_pct*100:.1f}%", help="Percentual concluído.")
            _a4.metric("Lote Atual", f"{_chunk_num} / {_total_chunks}", help="Chunk atual / total de chunks.")
            _a5, _a6, _a7, _a8 = st.columns(4)
            _a5.metric("Tempo Decorrido", _formatar_duracao(_elapsed), help="Tempo desde o início da alocação.")
            _a6.metric("Tempo Médio/Registro", f"{_tempo_medio_reg:.2f}s", help="Tempo médio por registro processado até agora.")
            _a7.metric("Tempo Restante (ETA)", _formatar_duracao(_eta) if _taxa > 0 else "calculando...", help="Estimativa para concluir, baseada na velocidade atual.")
            _a8.metric("Velocidade", f"{_taxa:.1f}/s · {_taxa*60:.0f}/min", help="Velocidade média (registros por segundo e por minuto).")
            st.caption("🔄 A alocação avança automaticamente. **Não é necessário clicar novamente.** Cancele a qualquer momento acima.")
            
            if _total == 0:
                # Nenhuma rota válida — finaliza direto
                st.session_state['alo_chunk_idx'] = 0
                _ir_finalizar = True
            else:
                # [FLUXO-CONTINUO - 38ª geração] Roteamento TIME-BOXED por orçamento de tempo
                # (~8s/execução), em mini-lotes — mesma correção da aba de Processamento: nenhuma
                # execução fica longa a ponto de o WebSocket cair antes do st.rerun(). Adapta-se
                # à rede e mantém a continuidade automática até o fim (sem novo clique).
                _BUDGET_ALO = 8.0
                _MINI_ALO = max(8, WORKERS_DISPONIVEIS)
                _t_alo = time.time()
                _idx_local = _idx
                _proc_alo = False
                while _idx_local < _total:
                    _mini = _tarefas[_idx_local:_idx_local + _MINI_ALO]
                    if not _mini:
                        break
                    try:
                        _res_mini = processar_chunk_rotas(_mini, runner_up_map=_runner_map)
                        _resultados.update(_res_mini)
                        _proc_alo = True
                    except Exception as e:
                        logger.error(f"[FLUXO-CONTINUO] Erro em mini-lote de alocação (idx {_idx_local}), isolado: {e}")
                    _idx_local += _MINI_ALO
                    if (time.time() - _t_alo) >= _BUDGET_ALO:
                        break
                if _proc_alo:
                    st.session_state['alo_resultados'] = _resultados
                st.session_state['alo_chunk_idx'] = min(_idx_local, _total)
                _ir_finalizar = st.session_state['alo_chunk_idx'] >= _total
            
            if not _ir_finalizar:
                time.sleep(0.05)
                st.rerun()
            else:
                # ---- FASE 3: FINALIZAÇÃO ----
                _df_pares = st.session_state['alo_df_pares']
                _runner = st.session_state['alo_runner_map']
                _dest_lr = st.session_state['alo_dest_linha_reta']
                _dest_st = st.session_state['alo_dest_status_lr']
                _start = st.session_state['alo_start_clock']
                _df_dest_cols = st.session_state['alo_df_dest_cols']
                _novas_colunas = st.session_state['alo_novas_colunas']
                
                df_final_alo = _montar_dataframe_final(_df_pares, _resultados, runner_up_map=_runner)
                
                df_final_alo['Linha Reta'] = df_final_alo['Origem'].astype(str).str.strip().map(_dest_lr).fillna(df_final_alo['Linha Reta'])
                df_final_alo['Status Linha Reta'] = df_final_alo['Origem'].astype(str).str.strip().map(_dest_st).fillna(df_final_alo['Status Linha Reta'])
                
                lat_o_alo = np.radians(df_final_alo['Lat Origem'].astype(float).values)
                lon_o_alo = np.radians(df_final_alo['Lon Origem'].astype(float).values)
                lat_d_alo = np.radians(df_final_alo['Lat Destino'].astype(float).values)
                lon_d_alo = np.radians(df_final_alo['Lon Destino'].astype(float).values)
                dlat_alo = lat_d_alo - lat_o_alo; dlon_alo = lon_d_alo - lon_o_alo
                a_alo = np.sin(dlat_alo / 2.0)**2 + np.cos(lat_o_alo) * np.cos(lat_d_alo) * np.sin(dlon_alo / 2.0)**2
                c_alo = 2 * np.arcsin(np.sqrt(a_alo))
                dist_vet_alo = 6371.0088 * c_alo
                mask_val_alo = (df_final_alo['Lat Origem'] != 0.0) & (df_final_alo['Lat Destino'] != 0.0)
                df_final_alo.loc[mask_val_alo, 'Linha Reta'] = np.round(dist_vet_alo[mask_val_alo], 2)
                df_final_alo.loc[mask_val_alo, 'Status Linha Reta'] = "Calculada via Haversine Vetorizado"
                
                tempo_alo_segundos = round(time.time() - _start, 2)
                cache_historico_lotes.set(f"alocacao_{_start}", {
                    "Data/Hora": time.strftime("%Y-%m-%d %H:%M:%S"), "Operador": "Motor de Alocação (Hubs)",
                    "Linhas Validadas": len(df_final_alo), "Tempo Gasto (s)": tempo_alo_segundos,
                    "Tempo Médio/Rota (s)": round(tempo_alo_segundos / max(1, _total), 2)
                }, expire=None)
                
                ordem_finais_alo = list(_df_dest_cols)
                for c in ['Origem', 'Destino'] + _novas_colunas:
                    if c not in ordem_finais_alo:
                        ordem_finais_alo.append(c)
                df_final_alo = df_final_alo.reindex(columns=ordem_finais_alo)
                
                output_buffer = io.BytesIO()
                with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
                    df_final_alo.to_excel(writer, index=False)
                st.session_state['df_processado'] = df_final_alo
                st.session_state['alo_planilha_pronta'] = output_buffer.getvalue()
                st.session_state['alo_tempo_total'] = tempo_alo_segundos
                st.session_state['alo_linhas'] = len(df_final_alo)
                
                for _k in ['alo_em_andamento', 'alo_tarefas', 'alo_resultados', 'alo_chunk_idx',
                           'alo_df_pares', 'alo_start_clock', 'alo_total', 'alo_runner_map',
                           'alo_dest_linha_reta', 'alo_dest_status_lr', 'alo_df_dest_cols', 'alo_novas_colunas',
                           'alo_dests_unicos', 'alo_hubs_validos', 'alo_dest_col_name', 'alo_df_dest',
                           'alo_dest_geo_acc', 'alo_dest_geo_idx']:
                    st.session_state.pop(_k, None)
                st.rerun()
        
        # ---- EXIBIÇÃO DO RESULTADO (após finalização) ----
        if 'alo_planilha_pronta' in st.session_state and 'df_processado' in st.session_state:
            if 'alo_tempo_total' in st.session_state:
                st.success(f"✨ Alocação concluída automaticamente! {st.session_state.get('alo_linhas', 0)} linhas processadas em {_formatar_duracao(st.session_state['alo_tempo_total'])}.")
                st.session_state.pop('alo_tempo_total', None)
            st.dataframe(st.session_state['df_processado'], use_container_width=True, height=250)
            st.download_button(
                label="📥 Baixar Planilha de Alocação Competitiva (.xlsx)",
                data=st.session_state['alo_planilha_pronta'],
                file_name="matriz_alocacao_competitiva.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)

with tab_analytics:
    st.info("📊 **Objetivo desta aba:** Sistema Analítico Global estilo Power BI. Clique nas fatias, barras ou arraste o mouse no Scatter Plot para filtrar dinamicamente TODOS os indicadores, mapas e tabelas abaixo.")
    renderizar_guia_aba("analytics")
    col_d_title, col_d_btn = st.columns([80, 20])
    with col_d_title: 
        st.markdown("### 📊 Enterprise Analytics Dashboard")
    with col_d_btn:
        if st.button("🧹 Limpar Todos os Filtros", use_container_width=True):
            keys_to_clear = ['widget_regiao', 'widget_uf', 'widget_mun', 'widget_status', 'widget_fonte', 'dash_reg', 'dash_uf', 'dash_status', 'dash_mun', 'dash_lr', 'dash_scatter', 'prev_altair_sel']
            for k in keys_to_clear:
                if k in st.session_state: del st.session_state[k]
            st.rerun()
            
    if 'df_processado' in st.session_state:
        sel = extrair_selecoes_altair()
        
        if 'widget_regiao' not in st.session_state: st.session_state['widget_regiao'] = 'Todas'
        if 'widget_uf' not in st.session_state: st.session_state['widget_uf'] = 'Todas'
        if 'widget_mun' not in st.session_state: st.session_state['widget_mun'] = 'Todos'
        if 'widget_status' not in st.session_state: st.session_state['widget_status'] = 'Todos'
        if 'widget_fonte' not in st.session_state: st.session_state['widget_fonte'] = 'Todas'
        
        sync_altair_to_widgets()
        df_kpi_raw = st.session_state['df_processado']
        
        @st.cache_data(show_spinner=False)
        def _enriquecer_df_analytics(df_serial: str) -> pd.DataFrame:
            _df = pd.read_json(io.StringIO(df_serial), orient='records')
            _df['Distancia'] = pd.to_numeric(_df['Distancia'], errors='coerce').fillna(0)
            _df['Linha Reta'] = pd.to_numeric(_df['Linha Reta'], errors='coerce').fillna(0)
            _df['Tempo_Minutos'] = _df['Tempo'].apply(parse_tempo_minutos)
            _df['Tempo_Horas'] = _df['Tempo_Minutos'] / 60.0
            return _df
            
        try:
            df_kpi = _enriquecer_df_analytics(df_kpi_raw.to_json(orient='records'))
        except Exception:
            df_kpi = df_kpi_raw.copy()
            df_kpi['Distancia'] = pd.to_numeric(df_kpi['Distancia'], errors='coerce').fillna(0)
            df_kpi['Linha Reta'] = pd.to_numeric(df_kpi['Linha Reta'], errors='coerce').fillna(0)
            df_kpi['Tempo_Minutos'] = df_kpi['Tempo'].apply(parse_tempo_minutos)
            df_kpi['Tempo_Horas'] = df_kpi['Tempo_Minutos'] / 60.0
            
        # MAPA_ESTADOS_FULL, REGIOES_BRASIL e extrair_uf_precisa agora são definidos
        # no escopo do módulo [PERF-2] — não recriados a cada rerun.
        df_kpi['UF_Sintetica_Origem'] = df_kpi['Endereco Oficial Origem'].apply(extrair_uf_precisa)
        # [PERF-1] Mapeamento UF→Região via dict de lookup O(1) + .map() vetorizado
        df_kpi['Regiao_Sintetica_Origem'] = df_kpi['UF_Sintetica_Origem'].map(_UF_PARA_REGIAO).fillna("Indefinido")
        
        lista_regioes = ["Todas"] + sorted([x for x in df_kpi['Regiao_Sintetica_Origem'].unique() if pd.notna(x)])
        if st.session_state['widget_regiao'] not in lista_regioes: st.session_state['widget_regiao'] = 'Todas'
        lista_ufs = ["Todas"] + sorted([x for x in df_kpi['UF_Sintetica_Origem'].unique() if pd.notna(x)])
        if st.session_state['widget_uf'] not in lista_ufs: st.session_state['widget_uf'] = 'Todas'
        lista_municipios = ["Todos"] + sorted([str(x) for x in df_kpi['Municipio Origem'].unique() if pd.notna(x)])
        if st.session_state['widget_mun'] not in lista_municipios: st.session_state['widget_mun'] = 'Todos'
        lista_status = ["Todos"] + sorted([str(x) for x in df_kpi['Status da Rota'].unique() if pd.notna(x)])
        if st.session_state['widget_status'] not in lista_status: st.session_state['widget_status'] = 'Todos'
        lista_fontes = ["Todas"] + sorted([str(x) for x in df_kpi['Fonte Geocoding Origem'].unique() if pd.notna(x)])
        if st.session_state['widget_fonte'] not in lista_fontes: st.session_state['widget_fonte'] = 'Todas'
        
        st.markdown("#### 🎛️ Painel de Controle de Filtros Avançados (Bidirecional)")
        with st.expander("Filtros Globais Sincronizados", expanded=False):
            col_f0, col_f1, col_f2, col_f3, col_f4 = st.columns(5)
            regiao_selecionada = col_f0.selectbox("Região do Brasil", lista_regioes, key="widget_regiao")
            uf_selecionada = col_f1.selectbox("UF de Origem", lista_ufs, key="widget_uf")
            mun_selecionado = col_f2.selectbox("Município de Origem", lista_municipios, key="widget_mun")
            status_selecionado = col_f3.selectbox("Status Global da Rota", lista_status, key="widget_status")
            fonte_selecionada = col_f4.selectbox("Fonte de Geocoding", lista_fontes, key="widget_fonte")
            
            col_f5, col_f6, col_f7 = st.columns(3)
            min_dist_val, max_dist_val = float(df_kpi['Distancia'].min()), float(df_kpi['Distancia'].max())
            if max_dist_val <= min_dist_val: max_dist_val = min_dist_val + 1.0
            dist_range = col_f5.slider("Faixa de Distância Viária (km)", min_value=0.0, max_value=max_dist_val, value=(0.0, max_dist_val))
            
            min_time_val, max_time_val = float(df_kpi['Tempo_Horas'].min()), float(df_kpi['Tempo_Horas'].max())
            if max_time_val <= min_time_val: max_time_val = min_time_val + 1.0
            time_range = col_f6.slider("Faixa de Tempo Estimado (Horas)", min_value=0.0, max_value=max_time_val, value=(0.0, max_time_val))
            
            min_score_val, max_score_val = float(df_kpi['Score Final Global'].min()), float(df_kpi['Score Final Global'].max())
            if max_score_val <= min_score_val: max_score_val = min_score_val + 1.0
            score_range = col_f7.slider("Score de Integridade Geodésica", min_value=0.0, max_value=100.0, value=(min_score_val, 100.0))
            
        df_cf = aplicar_filtro_global(df_kpi, extrair_selecoes_altair())
        
        mask = (
            (df_cf['Distancia'] >= dist_range[0]) & (df_cf['Distancia'] <= dist_range[1]) &
            (df_cf['Tempo_Horas'] >= time_range[0]) & (df_cf['Tempo_Horas'] <= time_range[1]) &
            (df_cf['Score Final Global'] >= score_range[0]) & (df_cf['Score Final Global'] <= score_range[1])
        )
        df_cf = df_cf[mask]
        df_cf['_is_selected'] = 1
        st.session_state['df_cf_master'] = df_cf
        renderizar_indicador_filtros(extrair_selecoes_altair()['brush'])
        
        # [F-NEW3 - 4ª geração] Insights Automáticos — destaque do que mais importa
        if not df_cf.empty:
            _insights = gerar_insights_automaticos(df_cf)
            if _insights:
                with st.expander("🤖 Insights Automáticos (descoberta de padrões e anomalias)", expanded=True):
                    st.caption("O sistema analisou os dados filtrados e destacou automaticamente os pontos mais relevantes:")
                    for tipo, texto in _insights:
                        if tipo == "sucesso":
                            st.success(texto)
                        elif tipo == "alerta":
                            st.warning(texto)
                        else:
                            st.info(texto)
        
        if df_cf.empty:
            st.warning("A combinação de filtros cruzados selecionada não retornou nenhum registro neste lote. Limpe os filtros.")
        else:
            df_sucesso = df_cf[~df_cf["Status da Rota"].str.contains("Erro")]
            tab_kpi_nacional, tab_kpi_regional = st.tabs([" Visão Nacional Macro", " Análise Regionalizada"])
            
            with tab_kpi_nacional:
                with st.container(border=True):
                    col_k1, col_k2, col_k3, col_k4, col_k5, col_k6 = st.columns(6)
                    total_distancia = df_sucesso['Distancia'].sum()
                    total_tempo_mins = df_sucesso['Tempo_Minutos'].sum()
                    tempo_total_str = f"{total_tempo_mins // 60}h {total_tempo_mins % 60}m"
                    dist_media = total_distancia / len(df_sucesso) if len(df_sucesso) > 0 else 0
                    tempo_medio = total_tempo_mins / len(df_sucesso) if len(df_sucesso) > 0 else 0
                    tempo_medio_str = f"{int(tempo_medio // 60)}h {int(tempo_medio % 60)}m"
                    
                    col_k1.metric("Rotas Selecionadas", f"{len(df_cf)}")
                    col_k2.metric("Distância Acumulada", f"{round(total_distancia, 1)} km")
                    col_k3.metric("Tempo Acumulado", f"{tempo_total_str}")
                    col_k4.metric("Distância Média/Rota", f"{round(dist_media, 1)} km")
                    col_k5.metric("Tempo Médio/Rota", f"{tempo_medio_str}")
                    col_k6.metric("Score Geodésico Médio", f"{round(df_sucesso['Score Final Global'].mean(), 1) if not df_sucesso.empty else 0}/100")
                    st.divider()
                    
                    col_k7, col_k8, col_k9, col_k10, col_k11, col_k12 = st.columns(6)
                    muns_atendidos = df_cf['Municipio Destino'].nunique()
                    ufs_atendidas = df_cf['Endereco Oficial Destino'].apply(extrair_uf_precisa).nunique()
                    rotas_balsa = len(df_cf[df_cf['Balsas'] == 'Sim'])
                    taxa_sucesso = round((len(df_sucesso) / len(df_cf)) * 100, 1) if len(df_cf) > 0 else 0
                    
                    col_k7.metric("Cidades Alcançadas", f"{muns_atendidos}")
                    col_k8.metric("Estados Alcançados (UFs)", f"{ufs_atendidas}")
                    col_k9.metric("Maior Viagem Mapeada", f"{round(df_cf['Distancia'].max(), 1)} km")
                    col_k10.metric("Rotas Fluviais (Balsa)", f"{rotas_balsa}")
                    col_k11.metric("Taxa de Sucesso (Roteamento)", f"{taxa_sucesso}%")
                    col_k12.metric("Confiança 'Altíssima'", f"{len(df_cf[df_cf['Confianca Destino'] == 'ALTISSIMA'])}")
                
                # [ANALISE-EST - 21ª geração] Painel de Estatística Descritiva das Rotas.
                # Complementa os KPIs (que só traziam média/máximo) com mediana, desvio
                # padrão e percentis — medidas essenciais para entender a DISTRIBUIÇÃO das
                # distâncias e tempos (apoio à decisão: detecta assimetria, outliers, caudas).
                with st.container(border=True):
                    st.markdown("##### 📐 Estatística Descritiva da Distribuição (recorte filtrado)")
                    _df_est = df_cf[df_cf['Distancia'] > 0]
                    if not _df_est.empty and len(_df_est) >= 2:
                        _dist = _df_est['Distancia'].astype(float)
                        _tmin = _df_est['Tempo_Minutos'].astype(float)
                        est1, est2, est3, est4 = st.columns(4)
                        est1.metric("Distância Mediana", f"{_dist.median():.1f} km",
                                    help="Valor central: metade das rotas é menor, metade é maior. Menos sensível a outliers que a média.")
                        est2.metric("Desvio Padrão (Dist.)", f"{_dist.std():.1f} km",
                                    help="Dispersão das distâncias em torno da média. Quanto maior, mais heterogêneas as rotas.")
                        est3.metric("Percentil 25 (Dist.)", f"{_dist.quantile(0.25):.1f} km",
                                    help="25% das rotas têm distância até este valor (rotas mais curtas).")
                        est4.metric("Percentil 75 (Dist.)", f"{_dist.quantile(0.75):.1f} km",
                                    help="75% das rotas têm distância até este valor; 25% são maiores (rotas mais longas).")
                        est5, est6, est7, est8 = st.columns(4)
                        est5.metric("Percentil 90 (Dist.)", f"{_dist.quantile(0.90):.1f} km",
                                    help="90% das rotas têm distância até este valor — os 10% mais longos estão acima.")
                        _amplitude = _dist.max() - _dist.min()
                        est6.metric("Amplitude (Dist.)", f"{_amplitude:.1f} km",
                                    help="Diferença entre a maior e a menor rota (alcance total das distâncias).")
                        _cv = (_dist.std() / _dist.mean() * 100) if _dist.mean() > 0 else 0
                        est7.metric("Coef. de Variação", f"{_cv:.0f}%",
                                    help="Desvio padrão relativo à média. <30% = rotas homogêneas; >60% = muito heterogêneas.")
                        est8.metric("Tempo Mediano", f"{_tmin.median():.0f} min",
                                    help="Tempo central das viagens (metade leva menos, metade leva mais).")
                        # Interpretação automática da assimetria
                        _media_d = _dist.mean(); _mediana_d = _dist.median()
                        if _media_d > _mediana_d * 1.15:
                            st.caption("📊 **Distribuição assimétrica à direita:** a média é puxada por algumas rotas muito longas "
                                       "(a maioria é mais curta que a média). A **mediana** representa melhor a rota típica.")
                        elif _mediana_d > _media_d * 1.15:
                            st.caption("📊 **Distribuição assimétrica à esquerda:** predominam rotas longas com algumas curtas reduzindo a média.")
                        else:
                            st.caption("📊 **Distribuição aproximadamente simétrica:** média e mediana próximas — as rotas se distribuem de forma equilibrada.")
                    else:
                        st.info("São necessárias ao menos 2 rotas válidas no recorte para calcular a estatística descritiva.")
                    
            with tab_kpi_regional:
                with st.container(border=True):
                    df_regioes = df_cf.groupby('Regiao_Sintetica_Origem').agg(
                        Rotas=('Origem', 'count'),
                        Dist_Media=('Distancia', 'mean'),
                        Tempo_Medio_Horas=('Tempo_Horas', 'mean'),
                        Score_Medio=('Score Final Global', 'mean'),
                        Muns_Unicos=('Municipio Origem', 'nunique')
                    ).reset_index()
                    
                    df_regioes = df_regioes[df_regioes['Regiao_Sintetica_Origem'] != "Indefinido"]
                    df_regioes['Participacao_Nacional'] = round((df_regioes['Rotas'] / len(df_cf)) * 100, 1)
                    
                    if not df_regioes.empty:
                        col_r1, col_r2 = st.columns(2)
                        with col_r1:
                            bar_regiao = alt.Chart(df_regioes).mark_bar(color='#3B82F6').encode(
                                x=alt.X('Rotas:Q', title='Volume de Rotas'),
                                y=alt.Y('Regiao_Sintetica_Origem:N', sort='-x', title='Região do Brasil'),
                                tooltip=['Regiao_Sintetica_Origem', 'Rotas', 'Participacao_Nacional', 'Dist_Media', 'Muns_Unicos']
                            ).properties(height=280, title="Ranking de Volume por Região (Filtrado)")
                            st.altair_chart(bar_regiao, use_container_width=True)
                        with col_r2:
                            st.write("Tabela Mestre Regional")
                            st.dataframe(df_regioes.rename(columns={'Regiao_Sintetica_Origem': 'Região Geográfica', 'Dist_Media': 'Distância Média (km)', 'Tempo_Medio_Horas': 'Tempo Médio (h)', 'Score_Medio': 'Score Médio', 'Muns_Unicos': 'Municípios Atendidos', 'Participacao_Nacional': 'Share Selecionado (%)'}), use_container_width=True, hide_index=True)
                    else:
                        st.info("Não há dados regionais válidos mapeados neste lote/recorte.")
                        
            st.markdown("#### 🔬 Análise Operacional e Motor Interativo de Filtros")
            with st.container(border=True):
                click_reg = alt.selection_point(fields=['Regiao_Sintetica_Origem'], name='Reg')
                click_uf = alt.selection_point(fields=['UF_Sintetica_Origem'], name='UF')
                click_mun = alt.selection_point(fields=['Municipio Origem'], name='Mun')
                click_linha = alt.selection_point(fields=['Municipio Origem'], name='LinhaMun')
                click_status = alt.selection_point(fields=['Status da Rota'], name='Status')
                click_scatter = alt.selection_point(fields=['Municipio Origem'], name='ScatterMun')
                brush = alt.selection_interval(name='Brush')
                
                base_chart = alt.Chart(df_cf)
                
                chart_reg = base_chart.mark_bar(cornerRadiusEnd=4).encode(
                    x=alt.X('count():Q', title='Volume de Rotas', axis=alt.Axis(grid=False)),
                    y=alt.Y('Regiao_Sintetica_Origem:N', sort='-x', title='Região'),
                    color=alt.value('#60A5FA'),
                    opacity=alt.condition(click_reg & (alt.datum._is_selected == 1), alt.value(1.0), alt.value(0.2)),
                    tooltip=['Regiao_Sintetica_Origem', 'count()']
                ).add_params(click_reg).properties(height=320, title="Volume de Demanda por Região")
                
                chart_uf = base_chart.mark_arc(innerRadius=60).encode(
                    theta=alt.Theta("count():Q", stack=True),
                    color=alt.Color("UF_Sintetica_Origem:N", legend=alt.Legend(title="Estados (UF)", orient='bottom')),
                    opacity=alt.condition(click_uf & (alt.datum._is_selected == 1), alt.value(1.0), alt.value(0.2)),
                    tooltip=['UF_Sintetica_Origem', 'count()']
                ).add_params(click_uf).properties(height=320, title="Market Share por Estado")
                
                status_palette = alt.Scale(domain=['Excelente', 'Boa', 'Aceitável', 'Revisar', 'Erro'], range=['#2ECC71', '#3498DB', '#F1C40F', '#E67E22', '#E74C3C'])
                chart_status = base_chart.mark_bar(cornerRadiusEnd=4).encode(
                    x=alt.X('Status da Rota:N', title='Status de Confiança', sort=['Excelente', 'Boa', 'Aceitável', 'Revisar', 'Erro']),
                    y=alt.Y('count():Q', title='Volume'),
                    color=alt.Color('Status da Rota:N', scale=status_palette, legend=None),
                    opacity=alt.condition(click_status & (alt.datum._is_selected == 1), alt.value(1.0), alt.value(0.2)),
                    tooltip=['Status da Rota', 'count()']
                ).add_params(click_status).properties(height=320, title="Monitor de Qualidade Geodésica")
                
                df_linha = df_cf.groupby('Municipio Origem').agg(
                    Média=('Linha Reta', 'mean'), Mediana=('Linha Reta', 'median'), Minimo=('Linha Reta', 'min'), Maximo=('Linha Reta', 'max'),
                    Desvio_Padrao=('Linha Reta', 'std'), Qtd=('Origem', 'count'), _is_selected=('_is_selected', 'max')
                ).reset_index()
                df_linha['Desvio_Padrao'] = df_linha['Desvio_Padrao'].fillna(0)
                
                chart_lr_mun = alt.Chart(df_linha).mark_line(point=True, color='#10B981').encode(
                    x=alt.X('Municipio Origem:N', title='Município', sort='-y'),
                    y=alt.Y('Média:Q', title='Distância Média (km)'),
                    opacity=alt.condition(click_linha & (alt.datum._is_selected == 1), alt.value(1.0), alt.value(0.2)),
                    tooltip=[alt.Tooltip('Municipio Origem:N'), alt.Tooltip('Média:Q', format='.2f')]
                ).add_params(click_linha).properties(height=320, title="Evolução da Qualidade Geodésica por Município")
                
                top_muns = df_cf['Municipio Origem'].value_counts().head(15).index.tolist()
                bar_base = base_chart.transform_filter(alt.FieldOneOfPredicate(field='Municipio Origem', oneOf=top_muns))
                bar_mun = bar_base.mark_bar(color='#3B82F6', cornerRadiusEnd=4).encode(
                    x=alt.X('count():Q', title='Volume de Rotas', axis=alt.Axis(tickMinStep=1)),
                    y=alt.Y('Municipio Origem:N', title='Município', sort=alt.EncodingSortField(field='Municipio Origem', op='count', order='descending')),
                    opacity=alt.condition(click_mun & (alt.datum._is_selected == 1), alt.value(1.0), alt.value(0.2)),
                    tooltip=['Municipio Origem', 'count()']
                ).add_params(click_mun)
                
                text_bar = bar_base.mark_text(align='right', dx=-5, color='white', fontWeight='bold').encode(
                    x=alt.X('count():Q'), 
                    y=alt.Y('Municipio Origem:N', sort=alt.EncodingSortField(field='Municipio Origem', op='count', order='descending')), 
                    text=alt.Text("count():Q")
                )
                chart_muns = alt.layer(bar_mun, text_bar).properties(height=350, title="Top 15 Municípios de Despacho Operacional")
                
                chart_scatter = base_chart.mark_circle(size=80).encode(
                    x=alt.X('Distancia:Q', title='Distância Viária Oficial (km)', scale=alt.Scale(zero=False, nice=True, padding=10)),
                    y=alt.Y('Tempo_Horas:Q', title='Tempo Estimado (Horas)', scale=alt.Scale(zero=False, nice=True, padding=10)),
                    color=alt.Color('Status da Rota:N', scale=status_palette),
                    opacity=alt.condition(brush & click_scatter & (alt.datum._is_selected == 1), alt.value(0.9), alt.value(0.1)),
                    tooltip=['Municipio Origem', 'Origem', 'Destino', 'Distancia', 'Tempo_Horas', 'Status da Rota']
                ).add_params(brush, click_scatter).properties(height=350, title="Matriz de Dispersão e Identificação de Outliers")
                
                col_p1, col_p2, col_p3 = st.columns(3)
                col_p1.altair_chart(chart_reg, use_container_width=True, on_select="rerun", key="dash_reg")
                col_p2.altair_chart(chart_uf, use_container_width=True, on_select="rerun", key="dash_uf")
                col_p3.altair_chart(chart_status, use_container_width=True, on_select="rerun", key="dash_status")
                
                st.divider()
                col_p4, col_p5 = st.columns(2)
                col_p4.altair_chart(chart_lr_mun, use_container_width=True, on_select="rerun", key="dash_lr")
                col_p5.altair_chart(chart_muns, use_container_width=True, on_select="rerun", key="dash_mun")
                st.altair_chart(chart_scatter, use_container_width=True, on_select="rerun", key="dash_scatter")
            
            # [F-NEW4 - 4ª geração] Análise de distribuição estatística (histograma + boxplot)
            st.markdown("#### 📊 Distribuição Estatística de Distâncias")
            st.caption("Histograma mostra a frequência de cada faixa de distância; o boxplot revela mediana, quartis e outliers. "
                       "Útil para entender o perfil logístico: rotas curtas (urbanas) vs longas (interestaduais).")
            df_dist_validas = df_cf[df_cf['Distancia'] > 0].copy()
            if not df_dist_validas.empty and len(df_dist_validas) >= 3:
                col_hist, col_box = st.columns([65, 35])
                with col_hist:
                    hist_chart = alt.Chart(df_dist_validas).mark_bar(color='#3B82F6', cornerRadiusEnd=3).encode(
                        x=alt.X('Distancia:Q', bin=alt.Bin(maxbins=30), title='Distância (km)'),
                        y=alt.Y('count():Q', title='Quantidade de Rotas'),
                        tooltip=[alt.Tooltip('count():Q', title='Rotas'), alt.Tooltip('Distancia:Q', bin=True, title='Faixa (km)')]
                    ).properties(height=300, title='Histograma de Distâncias')
                    st.altair_chart(hist_chart, use_container_width=True)
                with col_box:
                    box_chart = alt.Chart(df_dist_validas).mark_boxplot(extent='min-max', color='#10B981').encode(
                        y=alt.Y('Distancia:Q', title='Distância (km)'),
                        tooltip=[alt.Tooltip('Distancia:Q', title='km')]
                    ).properties(height=300, title='Boxplot (Quartis)')
                    st.altair_chart(box_chart, use_container_width=True)
                # Estatísticas descritivas explicadas
                d = df_dist_validas['Distancia']
                cme1, cme2, cme3, cme4 = st.columns(4)
                cme1.metric("Mediana", f"{d.median():.0f} km", help="Metade das rotas está abaixo deste valor. Menos sensível a outliers que a média.")
                cme2.metric("Desvio Padrão", f"{d.std():.0f} km", help="Quanto as distâncias variam em torno da média. Alto = rotas muito heterogêneas.")
                cme3.metric("Mínima", f"{d.min():.0f} km", help="A rota mais curta do recorte atual.")
                cme4.metric("Máxima", f"{d.max():.0f} km", help="A rota mais longa do recorte atual.")
            else:
                st.info("Distribuição estatística requer ao menos 3 rotas com distância válida no recorte atual.")

            # [F-NEW5 - 5ª geração] Ranking de Cobertura Territorial por UF
            # Atende Etapa 9 (cobertura territorial). Usa apenas agregação pandas sobre
            # dados já processados — read-only, zero chamada externa, zero risco.
            st.markdown("#### 🗺️ Cobertura Territorial por Estado (Ranking)")
            st.caption("Quantas rotas e qual a distância média partem de cada estado. "
                       "Revela onde sua operação está concentrada e onde há cobertura rarefeita.")
            if 'UF_Sintetica_Origem' in df_cf.columns:
                df_cobertura = df_cf[df_cf['UF_Sintetica_Origem'] != 'Indefinido'].groupby('UF_Sintetica_Origem').agg(
                    Rotas=('Origem', 'count'),
                    Dist_Media=('Distancia', 'mean'),
                    Score_Medio=('Score Final Global', 'mean')
                ).reset_index().sort_values('Rotas', ascending=False)
                if not df_cobertura.empty:
                    chart_cobertura = alt.Chart(df_cobertura).mark_bar(cornerRadiusEnd=4).encode(
                        x=alt.X('Rotas:Q', title='Quantidade de Rotas'),
                        y=alt.Y('UF_Sintetica_Origem:N', title='Estado (UF)', sort='-x'),
                        color=alt.Color('Dist_Media:Q', scale=alt.Scale(scheme='blues'), title='Dist. Média (km)'),
                        tooltip=[
                            alt.Tooltip('UF_Sintetica_Origem:N', title='UF'),
                            alt.Tooltip('Rotas:Q', title='Rotas'),
                            alt.Tooltip('Dist_Media:Q', title='Distância Média (km)', format='.1f'),
                            alt.Tooltip('Score_Medio:Q', title='Score Médio', format='.1f'),
                        ]
                    ).properties(height=max(200, len(df_cobertura) * 28), title='Rotas por Estado de Origem')
                    st.altair_chart(chart_cobertura, use_container_width=True)
                    n_ufs = len(df_cobertura)
                    st.caption(f"📍 Cobertura atual: **{n_ufs} de 27 estados** ({round(100*n_ufs/27)}% do território nacional) presentes neste recorte.")
                else:
                    st.info("Sem dados de estado identificáveis no recorte atual.")
                
            st.markdown("#### 🗺️ Torre de Controle Espacial (Heatmap Dinâmico)")
            with st.container(border=True):
                col_m1, col_m2 = st.columns([80, 20])
                with col_m2: 
                    map_style_selection = st.radio("Tema Topológico:", ["Carto Dark Mode (Padrão)", "OpenStreetMap Clássico", "Satélite (Esri Imagens)"], index=0)
                    
                df_mapa = df_cf 
                df_mapa['Lat Destino'] = pd.to_numeric(df_mapa['Lat Destino'], errors='coerce')
                df_mapa['Lon Destino'] = pd.to_numeric(df_mapa['Lon Destino'], errors='coerce')
                df_mapa = df_mapa.dropna(subset=['Lat Destino', 'Lon Destino'])
                df_mapa = df_mapa[(df_mapa['Lat Destino'] != 0.0) & (df_mapa['Lon Destino'] != 0.0)]
                
                if not df_mapa.empty:
                    df_agg = df_mapa.groupby(['Municipio Destino', 'Endereco Oficial Destino', 'UF_Sintetica_Origem', 'Regiao_Sintetica_Origem']).agg(
                        Qtd_Rotas=('Origem', 'count'), Lat_Media=('Lat Destino', 'mean'), Lon_Media=('Lon Destino', 'mean'),
                        Dist_Media=('Distancia', 'mean'), Tempo_Medio=('Tempo_Horas', 'mean'), Score_Medio=('Score Final Global', 'mean')
                    ).reset_index()
                    total_rotas_mapa = df_agg['Qtd_Rotas'].sum()
                    df_agg['Participacao_Nacional_%'] = (df_agg['Qtd_Rotas'] / total_rotas_mapa) * 100
                    
                    estilo_mapbox = "carto-darkmatter"
                    if map_style_selection == "OpenStreetMap Clássico": estilo_mapbox = "open-street-map"
                    if map_style_selection == "Satélite (Esri Imagens)": estilo_mapbox = "white-bg"
                    
                    fig = px.scatter_mapbox(
                        df_agg, lat='Lat_Media', lon='Lon_Media', size='Qtd_Rotas', color='Qtd_Rotas', color_continuous_scale=px.colors.sequential.Blues,
                        size_max=45, zoom=3.5, mapbox_style=estilo_mapbox, hover_name='Municipio Destino',
                        hover_data={'Lat_Media': False, 'Lon_Media': False, 'UF_Sintetica_Origem': True, 'Regiao_Sintetica_Origem': True, 'Qtd_Rotas': True, 'Participacao_Nacional_%': ':.2f', 'Dist_Media': ':.1f', 'Tempo_Medio': ':.1f', 'Score_Medio': False},
                        title="Densidade Operacional da Seleção Ativa"
                    )
                    
                    if map_style_selection == "Satélite (Esri Imagens)": 
                        fig.update_layout(mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "sourceattribution": "Esri World Imagery", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]}])
                        
                    fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0}, height=600)
                    st.plotly_chart(fig, use_container_width=True)
                else: 
                    st.info("O filtro atual não retornou coordenadas válidas no Brasil para plotagem.")
                    
            st.markdown("#### 🏆 Rankings e Extremos Logísticos da Seleção Atual (Top 10)")
            with st.container(border=True):
                tab_dist_max, tab_dist_min, tab_tempo = st.tabs(["Maiores Distâncias (+)", "Menores Distâncias (-)", "Maiores Tempos (Gargalos)"])
                with tab_dist_max: st.dataframe(df_cf.nlargest(10, 'Distancia')[['Origem', 'Destino', 'Distancia', 'Tempo', 'Status da Rota']], use_container_width=True)
                with tab_dist_min: st.dataframe(df_cf.nsmallest(10, 'Distancia')[['Origem', 'Destino', 'Distancia', 'Tempo', 'Status da Rota']], use_container_width=True)
                with tab_tempo: st.dataframe(df_cf.nlargest(10, 'Tempo_Minutos')[['Origem', 'Destino', 'Tempo', 'Distancia', 'Status da Rota']], use_container_width=True)
                
            st.markdown("#### 🔎 Matriz de Dados Drill-Down da Seleção (Data Explorer)")
            with st.container(border=True):
                tabela_h = min(800, max(300, len(df_cf) * 35 + 43))
                st.dataframe(df_cf[['Origem', 'Destino', 'Distancia', 'Linha Reta', 'Tempo', 'Status da Rota', 'Status Linha Reta', 'Link da Rota']], use_container_width=True, height=tabela_h, column_config={"Link da Rota": st.column_config.LinkColumn("🗺️ Abrir no Maps")}, hide_index=True)
                
            st.markdown("#### ✅ Controle de Qualidade de Dados (Auditoria Geodésica e de Falhas)")
            with st.container(border=True):
                df_suspeitas = df_cf[(df_cf['Score Final Global'] < 70) | (df_cf['Status da Rota'] == "Erro") | (df_cf['Confianca Origem'] == "BAIXA") | ((df_cf['Linha Reta'] <= 0.01) & (df_cf['Origem'] != df_cf['Destino']))]
                if not df_suspeitas.empty:
                    st.warning(f"Atenção: Identificadas {len(df_suspeitas)} rotas requerendo revisão humana dentro do seu recorte atual.")
                    st.dataframe(df_suspeitas[['Origem', 'Destino', 'Linha Reta', 'Status Linha Reta', 'Score Final Global', 'Confianca Origem', 'Motivo Roteamento']], use_container_width=True)
                else: 
                    st.success(" Excelente! Nenhuma anomalia geodésica ou operacional encontrada no recorte atual.")
    else:
        st.warning("Aguardando processamento de planilha corporativa na aba de Lotes (⚙️) para ativar e renderizar o Enterprise Data Analytics Engine.")

with tab_calculadora:
    st.info("🧮 **Objetivo desta aba:** Uma ferramenta de autoatendimento Analítico (Self-Service BI). Realize extrações, crie tabelas dinâmicas e pivote informações de forma flexível utilizando a base que já passou pela blindagem e filtros globais.")
    renderizar_guia_aba("calculadora")
    col_c_title, col_c_btn = st.columns([80, 20])
    with col_c_title: 
        st.markdown("### 🧮 Calculadora Analítica Corporativa")
        
    if 'df_cf_master' in st.session_state and not st.session_state['df_cf_master'].empty:
        df_base_calc = st.session_state['df_cf_master'].copy()
        
        st.markdown("#### 🎛️ Painel de Filtros da Calculadora (Cascata Local)")
        with st.container(border=True):
            c_f1, c_f2, c_f3, c_f4 = st.columns(4)
            op_regiao = sorted(df_base_calc['Regiao_Sintetica_Origem'].dropna().unique())
            calc_reg = c_f1.multiselect("Região", op_regiao)
            if calc_reg: df_base_calc = df_base_calc[df_base_calc['Regiao_Sintetica_Origem'].isin(calc_reg)]
            
            op_uf = sorted(df_base_calc['UF_Sintetica_Origem'].dropna().unique())
            calc_uf = c_f2.multiselect("UF", op_uf)
            if calc_uf: df_base_calc = df_base_calc[df_base_calc['UF_Sintetica_Origem'].isin(calc_uf)]
            
            op_mun = sorted(df_base_calc['Municipio Origem'].dropna().unique())
            calc_mun = c_f3.multiselect("Município Origem", op_mun)
            if calc_mun: df_base_calc = df_base_calc[df_base_calc['Municipio Origem'].isin(calc_mun)]
            
            op_distrito = sorted(df_base_calc['Distrito Origem'].dropna().unique())
            calc_distrito = c_f4.multiselect("Distrito Origem", op_distrito)
            if calc_distrito: df_base_calc = df_base_calc[df_base_calc['Distrito Origem'].isin(calc_distrito)]
            
            c_f5, c_f6, c_f7, c_f8 = st.columns(4)
            op_status = sorted(df_base_calc['Status da Rota'].dropna().unique())
            calc_status = c_f5.multiselect("Status da Rota", op_status)
            if calc_status: df_base_calc = df_base_calc[df_base_calc['Status da Rota'].isin(calc_status)]
            
            op_fonte = sorted(df_base_calc['Fonte Geocoding Origem'].dropna().unique())
            calc_fonte = c_f6.multiselect("Fonte Geocoding", op_fonte)
            if calc_fonte: df_base_calc = df_base_calc[df_base_calc['Fonte Geocoding Origem'].isin(calc_fonte)]
            
            op_fonte_rota = sorted(df_base_calc['Fonte da Rota'].dropna().unique())
            calc_fonte_rota = c_f7.multiselect("Fonte da Rota", op_fonte_rota)
            if calc_fonte_rota: df_base_calc = df_base_calc[df_base_calc['Fonte da Rota'].isin(calc_fonte_rota)]
            
            op_balsa = sorted(df_base_calc['Balsas'].dropna().astype(str).unique())
            calc_balsa = c_f8.multiselect("Possui Balsa", op_balsa)
            if calc_balsa: df_base_calc = df_base_calc[df_base_calc['Balsas'].astype(str).isin(calc_balsa)]
            
        st.markdown("#### ⚙️ Configuração dos Cálculos")
        with st.container(border=True):
            cc1, cc2, cc3 = st.columns([1, 1, 2])
            colunas_disponiveis = df_base_calc.columns.tolist()
            calc_campo = cc1.selectbox("Campo de Análise", colunas_disponiveis, index=colunas_disponiveis.index('Distancia') if 'Distancia' in colunas_disponiveis else 0)
            operacoes = ['Contagem (Count)', 'Contagem Distinta (Count Distinct)', 'Soma (Sum)', 'Média (Average)', 'Mínimo (Min)', 'Máximo (Max)', 'Mediana (Median)', 'Desvio Padrão', 'Variância', 'Percentil 25', 'Percentil 50', 'Percentil 75']
            calc_op = cc2.selectbox("Operação Matemática/Estatística", operacoes, index=3)
            calc_agrup = cc3.multiselect("Agrupar por (Pivot)", colunas_disponiveis, default=['Regiao_Sintetica_Origem'])
            
        st.markdown("#### 📈 Resultados Analíticos Extraídos")
        if df_base_calc.empty:
            st.warning("O conjunto resultante das filtragens locais (Calculadora) ou globais (Analytics) está vazio.")
        else:
            try:
                fig = None
                if not calc_agrup:
                    if 'Contagem' in calc_op and 'Distinta' not in calc_op: 
                        resultado_final = df_base_calc[calc_campo].count()
                    elif 'Contagem Distinta' in calc_op: 
                        resultado_final = df_base_calc[calc_campo].nunique()
                    else: 
                        resultado_final = df_base_calc[calc_campo].agg(get_agg_func(calc_op))
                        
                    st.metric(f"Resultado Consolidado: {calc_op} de {calc_campo}", round(resultado_final, 2) if isinstance(resultado_final, (float, int)) else resultado_final)
                    df_agg = pd.DataFrame([{"Métrica": f"{calc_op} de {calc_campo}", "Valor": resultado_final}])
                else:
                    df_agg = df_base_calc.groupby(calc_agrup).agg(Resultado_Metrica=(calc_campo, get_agg_func(calc_op))).reset_index()
                    df_agg = df_agg.rename(columns={'Resultado_Metrica': f"{calc_op} de {calc_campo}"})
                    if 'Soma' in calc_op or 'Contagem' in calc_op: 
                        df_agg = df_agg.sort_values(by=f"{calc_op} de {calc_campo}", ascending=False)
                        
                col_r1, col_r2 = st.columns([40, 60])
                with col_r1: 
                    st.dataframe(df_agg, use_container_width=True, hide_index=True)
                with col_r2:
                    if len(calc_agrup) == 1: 
                        fig = px.bar(df_agg, x=calc_agrup[0], y=f"{calc_op} de {calc_campo}", color=calc_agrup[0], title=f"Distribuição de {calc_campo}")
                    elif len(calc_agrup) >= 2: 
                        fig = px.bar(df_agg, x=calc_agrup[0], y=f"{calc_op} de {calc_campo}", color=calc_agrup[1], barmode='group', title=f"Análise Multidimensional de {calc_campo}")
                    if fig:
                        fig.update_layout(showlegend=True, height=400, margin=dict(l=0, r=0, t=40, b=0))
                        st.plotly_chart(fig, use_container_width=True)
                        
                st.markdown("#### 💾 Exportação Avançada Multi-Abas (Calculadora + Gráficos)")
                output_calc = io.BytesIO()
                with pd.ExcelWriter(output_calc, engine='xlsxwriter') as writer:
                    df_resumo = pd.DataFrame([{"Métrica Principal": f"{calc_op} de {calc_campo}", "Total de Linhas Analisadas": len(df_base_calc)}])
                    df_resumo.to_excel(writer, sheet_name='Resumo Executivo', index=False)
                    df_agg.to_excel(writer, sheet_name='Dados Consolidados', index=False)
                    
                    if fig:
                        workbook = writer.book
                        worksheet = workbook.add_worksheet('Gráficos Exportados')
                        try:
                            img_bytes = fig.to_image(format="png", width=900, height=500)
                            worksheet.insert_image('B2', 'grafico.png', {'image_data': io.BytesIO(img_bytes)})
                        except Exception as e:
                            worksheet.write('A1', f"Aviso: O motor de renderização de imagens estáticas (Kaleido) não está ativo neste ambiente. O gráfico interativo não pôde ser convertido para PNG. Detalhes: {str(e)}")
                            
                csv_calc = df_agg.to_csv(index=False).encode('utf-8')
                c_exp1, c_exp2, c_exp3 = st.columns(3)
                c_exp1.download_button("📊 Exportar Relatório Excel Completo (.xlsx)", data=output_calc.getvalue(), file_name="relatorio_calculadora_avancado.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                c_exp2.download_button("Exportar Tabela Bruta (CSV)", data=csv_calc, file_name="dados_calculadora.csv", mime="text/csv", use_container_width=True)
            except Exception as e:
                st.error(f"⚠️ Impossível realizar o cálculo solicitado. A operação estatística '{calc_op}' falhou. Verifique se o campo '{calc_campo}' contém números válidos. Erro: {e}")
    else:
        st.warning("Os dados ainda não foram processados ou o filtro global está muito restrito. Processe um lote na Aba 'Processamento em Lote'.")

with tab_classificacao:
    st.info("🗂️ **Objetivo desta aba:** Segmentar a volumetria logística por município, criar faixas personalizadas e rotular os polos de distribuição. Utilize o Editor de Faixas abaixo para configurar os limites, divisores operacionais e níveis críticos.")
    renderizar_guia_aba("classificacao")
    st.markdown("### 🗂️ Classificação Territorial de Ocorrências Municipais")
    
    if 'df_cf_master' in st.session_state and not st.session_state['df_cf_master'].empty:
        df_base_class = st.session_state['df_cf_master'].copy()
        
        st.markdown("#### ⚙️ Parâmetro Base de Classificação")
        metrica_classificacao = st.radio(
            "Selecione a métrica que definirá as faixas territoriais:",
            ["Distância Total (km)", "Distância Média (km)", "Ocorrências (Volume)"],
            index=0,
            horizontal=True,
            help="A métrica selecionada será utilizada para enquadrar os municípios nas faixas configuradas abaixo."
        )
        
        col_metrica = "Distância_Total" if metrica_classificacao == "Distância Total (km)" else "Distância_Media" if metrica_classificacao == "Distância Média (km)" else "Ocorrências"
        st.markdown("#### 1️⃣ Editor Dinâmico de Faixas e Divisores")
        st.caption(f"Configure os limites Mínimos e Máximos considerando a métrica base escolhida: **{metrica_classificacao}**.")
        
        if 'class_bins' not in st.session_state:
            st.session_state['class_bins'] = pd.DataFrame([
                {"Min": 1, "Max": 500, "Divisor": 500, "Rótulo": " Operação Normal", "Cor": "#2ECC71"},
                {"Min": 501, "Max": 2000, "Divisor": 2000, "Rótulo": " Alerta Laranja", "Cor": "#F39C12"},
                {"Min": 2001, "Max": 999999, "Divisor": 5000, "Rótulo": " Volume Crítico", "Cor": "#E74C3C"}
            ])
            
        edited_bins = st.data_editor(st.session_state['class_bins'], num_rows="dynamic", use_container_width=True, hide_index=True)
        
        with st.spinner("Reagrupando e Classificando Malha Territorial..."):
            df_base_class['Lat Origem'] = pd.to_numeric(df_base_class['Lat Origem'], errors='coerce')
            df_base_class['Lon Origem'] = pd.to_numeric(df_base_class['Lon Origem'], errors='coerce')
            df_base_class['Distancia'] = pd.to_numeric(df_base_class['Distancia'], errors='coerce').fillna(0)
            
            df_agg_class = df_base_class.groupby(['Municipio Origem', 'UF_Sintetica_Origem', 'Regiao_Sintetica_Origem']).agg(
                Ocorrências=('Origem', 'count'),
                Distância_Total=('Distancia', 'sum'),
                Distância_Media=('Distancia', 'mean'),
                Lat_Media=('Lat Origem', 'mean'),
                Lon_Media=('Lon Origem', 'mean')
            ).reset_index()
            
            df_agg_class = df_agg_class[df_agg_class['Municipio Origem'] != "Não Identificado"]
            
            def classificar_ocorrencia(valor):
                bins_sorted_local = edited_bins.sort_values("Min").reset_index(drop=True)
                for _, row in bins_sorted_local.iterrows():
                    try:
                        vmin, vmax = float(row['Min']), float(row['Max'])
                        if vmin <= valor <= vmax:
                            divisor = float(row['Divisor']) if row['Divisor'] > 0 else 1
                            pct = round((valor / divisor) * 100, 2)
                            return row['Rótulo'], pct, row['Cor']
                    except: 
                        pass
                return "⚪ Não Classificado", 0.0, "#95A5A6"
                
            bins_sorted_vet = edited_bins.sort_values("Min").reset_index(drop=True)
            bins_vals_vet  = [float(b) for b in bins_sorted_vet["Max"].tolist()]
            bins_labels_vet = bins_sorted_vet["Rótulo"].tolist()
            bins_divs_vet   = bins_sorted_vet["Divisor"].tolist()
            bins_cores_vet  = bins_sorted_vet["Cor"].tolist()
            bins_cuts = [-float("inf")] + bins_vals_vet
            
            try:
                cats = pd.cut(df_agg_class[col_metrica], bins=bins_cuts, labels=bins_labels_vet, right=True)
                df_agg_class["Rótulo"] = cats.astype(str).where(cats.notna(), "⚪ Não Classificado")
                df_agg_class["Cor Hex"] = df_agg_class["Rótulo"].map(dict(zip(bins_labels_vet, bins_cores_vet))).fillna("#95A5A6")
                
                def _calc_pct(row):
                    try:
                        b = bins_sorted_vet[bins_sorted_vet["Rótulo"] == row["Rótulo"]]
                        if not b.empty:
                            d = float(b.iloc[0]["Divisor"]) if float(b.iloc[0]["Divisor"]) > 0 else 1
                            return round((row[col_metrica] / d) * 100, 2)
                    except: 
                        pass
                    return 0.0
                    
                df_agg_class["Percentual (%)"] = df_agg_class.apply(_calc_pct, axis=1)
                resultados_clas = None
            except Exception as _e_cut:
                resultados_clas = df_agg_class[col_metrica].apply(classificar_ocorrencia)
                
            if resultados_clas is not None:
                df_agg_class['Rótulo'] = [r[0] for r in resultados_clas]
                df_agg_class['Percentual (%)'] = [r[1] for r in resultados_clas]
                df_agg_class['Cor Hex'] = [r[2] for r in resultados_clas]
                
            df_agg_class = df_agg_class.sort_values(by=col_metrica, ascending=False)
            
            st.markdown("#### 2️⃣ Indicadores e Extremos da Malha")
            cc_k1, cc_k2, cc_k3, cc_k4 = st.columns(4)
            cc_k1.metric("Municípios Analisados", df_agg_class.shape[0])
            valor_total_metrica = df_agg_class[col_metrica].sum() if col_metrica != "Distância_Media" else df_agg_class[col_metrica].mean()
            cc_k2.metric(f"Total: {metrica_classificacao}", round(valor_total_metrica, 1))
            cc_k3.metric("Percentual Médio Aplicado", f"{round(df_agg_class['Percentual (%)'].mean(), 1)}%")
            
            if not df_agg_class.empty:
                m_critico = df_agg_class.iloc[0]['Municipio Origem']
                v_critico = round(df_agg_class.iloc[0][col_metrica], 1)
                cc_k4.metric("Polo Mais Crítico", f"{m_critico} ({v_critico})")
                
            st.markdown("#### 3️⃣ Ecossistema Visual Temático")
            map_colors = dict(zip(df_agg_class['Rótulo'], df_agg_class['Cor Hex']))
            
            t_col1, t_col2 = st.columns([60, 40])
            with t_col1:
                fig_bar_clas = px.bar(df_agg_class.head(20), x='Municipio Origem', y=col_metrica, color='Rótulo', color_discrete_map=map_colors, title=f"Top 20 Cidades por {metrica_classificacao}", text='Percentual (%)')
                fig_bar_clas.update_traces(texttemplate='%{text}%', textposition='outside')
                st.plotly_chart(fig_bar_clas, use_container_width=True)
            with t_col2:
                fig_pie_clas = px.pie(df_agg_class, names='Rótulo', values=col_metrica, color='Rótulo', color_discrete_map=map_colors, hole=0.4, title="Distribuição por Nível Crítico")
                st.plotly_chart(fig_pie_clas, use_container_width=True)
                
            fig_tree = px.treemap(df_agg_class, path=[px.Constant("Brasil"), 'Regiao_Sintetica_Origem', 'UF_Sintetica_Origem', 'Municipio Origem'], values=col_metrica, color='Rótulo', color_discrete_map=map_colors, title="Volumetria Hierárquica por Rótulo Territorial")
            st.plotly_chart(fig_tree, use_container_width=True)
            
            df_mapa_clas = df_agg_class.dropna(subset=['Lat_Media', 'Lon_Media'])
            df_mapa_clas = df_mapa_clas[(df_mapa_clas['Lat_Media'] != 0.0) & (df_mapa_clas['Lon_Media'] != 0.0)]
            if not df_mapa_clas.empty:
                fig_mapa_clas = px.scatter_mapbox(
                    df_mapa_clas, lat='Lat_Media', lon='Lon_Media', size=col_metrica, color='Rótulo', color_discrete_map=map_colors,
                    size_max=35, zoom=3.5, mapbox_style="carto-darkmatter", hover_name='Municipio Origem',
                    hover_data={'Lat_Media': False, 'Lon_Media': False, 'UF_Sintetica_Origem': True, col_metrica: True, 'Percentual (%)': True, 'Rótulo': False},
                    title="Mapeamento Temático Pós-Classificação"
                )
                fig_mapa_clas.update_layout(margin={"r":0,"t":40,"l":0,"b":0}, height=550)
                st.plotly_chart(fig_mapa_clas, use_container_width=True)
                
            st.markdown("#### 4️⃣ Tabela Mestre e Exportação Direta")
            st.dataframe(df_agg_class.drop(columns=['Lat_Media', 'Lon_Media', 'Cor Hex']), use_container_width=True, hide_index=True)
            
            out_class = io.BytesIO()
            with pd.ExcelWriter(out_class, engine='xlsxwriter') as writer:
                df_agg_class.drop(columns=['Lat_Media', 'Lon_Media', 'Cor Hex']).to_excel(writer, sheet_name='Ocorrencias e Classificacao', index=False)
            st.download_button("📥 Baixar Tabela de Classificação (.xlsx)", data=out_class.getvalue(), file_name="classificacao_territorial_ocorrencias.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            # ─────────────────────────────────────────────────────────────────────
            # [CLASS-MULTI - 37ª geração] RANKING MULTI-INDICADOR POR ROTA
            # Além da classificação territorial por município (acima), permite classificar e
            # ordenar ROTA A ROTA por dezenas de indicadores logísticos/espaciais derivados
            # (sinuosidade, tempo/km, velocidade média, diferença viária−reta, scores...), com
            # ordenação crescente/decrescente, critério de desempate e filtros avançados.
            # Estritamente aditivo (não altera a classificação municipal). Exporta CSV + XLSX.
            # ─────────────────────────────────────────────────────────────────────
            st.divider()
            st.markdown("### 📊 Ranking Multi-Indicador por Rota (classificação avançada)")
            st.caption("Classifique e ordene **rota a rota** por indicadores logísticos e espaciais derivados. "
                       "Combine critérios, inverta a ordem e aplique filtros — ideal para achar gargalos e anomalias.")

            def _tempo_para_min(txt):
                try:
                    s = str(txt).lower()
                    _h = re.search(r'(\d+)\s*h', s); _m = re.search(r'(\d+)\s*min', s)
                    return float((int(_h.group(1)) * 60 if _h else 0) + (int(_m.group(1)) if _m else 0))
                except Exception:
                    return 0.0

            df_rota_ind = df_base_class.copy()
            _via = pd.to_numeric(df_rota_ind['Distancia'], errors='coerce').fillna(0.0) if 'Distancia' in df_rota_ind.columns else pd.Series([0.0] * len(df_rota_ind), index=df_rota_ind.index)
            _reta = pd.to_numeric(df_rota_ind['Linha Reta'], errors='coerce').fillna(0.0) if 'Linha Reta' in df_rota_ind.columns else pd.Series([0.0] * len(df_rota_ind), index=df_rota_ind.index)
            _tempo_col = df_rota_ind['Tempo'] if 'Tempo' in df_rota_ind.columns else pd.Series(["0 min"] * len(df_rota_ind), index=df_rota_ind.index)
            _tmin = _tempo_col.apply(_tempo_para_min)
            _via_safe, _reta_safe, _tmin_safe = _via.replace(0, np.nan), _reta.replace(0, np.nan), _tmin.replace(0, np.nan)
            df_ind = pd.DataFrame({
                "Origem": df_rota_ind.get('Origem', ''),
                "Destino": df_rota_ind.get('Destino', ''),
                "Município Origem": df_rota_ind.get('Municipio Origem', ''),
                "UF Origem": df_rota_ind.get('UF_Sintetica_Origem', ''),
                "Motor Vencedor": df_rota_ind.get('Fonte da Rota', ''),
                "Distância Viária (km)": _via.round(2),
                "Linha Reta (km)": _reta.round(2),
                "Diferença Viária−Reta (km)": (_via - _reta).round(2),
                "Índice de Sinuosidade": (_via / _reta_safe).round(3),
                "Tempo (min)": _tmin.round(0),
                "Tempo por km (min/km)": (_tmin / _via_safe).round(3),
                "km por minuto": (_via / _tmin_safe).round(3),
                "Velocidade Média (km/h)": (_via / (_tmin_safe / 60.0)).round(1),
                "Score Global": pd.to_numeric(df_rota_ind.get('Score Final Global', 0), errors='coerce').fillna(0.0).round(1),
                "Score da Rota": pd.to_numeric(df_rota_ind.get('Score da Rota', 0), errors='coerce').fillna(0.0).round(1),
            })
            _fcol1, _fcol2, _fcol3 = st.columns(3)
            with _fcol1:
                _motores = sorted([m for m in df_ind["Motor Vencedor"].dropna().unique().tolist() if str(m).strip()])
                _sel_mot = st.multiselect("Filtrar por motor vencedor", _motores, default=_motores, key="cls_mot")
            with _fcol2:
                _ufs = sorted([u for u in df_ind["UF Origem"].dropna().unique().tolist() if str(u).strip()])
                _sel_uf = st.multiselect("Filtrar por UF de origem", _ufs, default=_ufs, key="cls_uf")
            with _fcol3:
                _topn = st.number_input("Exibir top N", min_value=5, max_value=100000, value=100, step=25, key="cls_topn")
            _indicadores = [c for c in df_ind.columns if c not in ("Origem", "Destino", "Município Origem", "UF Origem", "Motor Vencedor")]
            _scol1, _scol2, _scol3 = st.columns([40, 30, 30])
            with _scol1:
                _sort1 = st.selectbox("Ordenar por (critério principal)", _indicadores,
                                      index=_indicadores.index("Índice de Sinuosidade") if "Índice de Sinuosidade" in _indicadores else 0, key="cls_sort1")
            with _scol2:
                _sort2 = st.selectbox("Critério de desempate (opcional)", ["—"] + _indicadores, index=0, key="cls_sort2")
            with _scol3:
                _ordem = st.radio("Ordem", ["Decrescente", "Crescente"], horizontal=True, key="cls_ordem")
            _asc = (_ordem == "Crescente")
            _df_show = df_ind.copy()
            if _sel_mot:
                _df_show = _df_show[_df_show["Motor Vencedor"].isin(_sel_mot)]
            if _sel_uf:
                _df_show = _df_show[_df_show["UF Origem"].isin(_sel_uf)]
            _sort_cols = [_sort1] + ([_sort2] if _sort2 != "—" else [])
            _df_show = _df_show.sort_values(by=_sort_cols, ascending=_asc, na_position="last").head(int(_topn)).reset_index(drop=True)
            _sin_series = df_ind['Índice de Sinuosidade'].replace([np.inf, -np.inf], np.nan).dropna()
            _vel_series = df_ind['Velocidade Média (km/h)'].replace([np.inf, -np.inf], np.nan).dropna()
            _tpk_series = df_ind['Tempo por km (min/km)'].replace([np.inf, -np.inf], np.nan).dropna()
            _ik1, _ik2, _ik3, _ik4 = st.columns(4)
            _ik1.metric("Rotas no ranking", _df_show.shape[0])
            _ik2.metric("Sinuosidade média", f"{_sin_series.mean():.3f}" if not _sin_series.empty else "—", help="Distância viária ÷ linha reta (1,0 = rota perfeitamente reta; maior = mais sinuosa).")
            _ik3.metric("Velocidade média (km/h)", f"{_vel_series.mean():.1f}" if not _vel_series.empty else "—")
            _ik4.metric("Tempo médio por km", f"{_tpk_series.mean():.2f} min" if not _tpk_series.empty else "—")
            st.dataframe(_df_show, use_container_width=True, hide_index=True)
            _exp1, _exp2 = st.columns(2)
            with _exp1:
                st.download_button("📥 Baixar ranking (.csv)", data=_df_show.to_csv(index=False).encode("utf-8-sig"),
                                   file_name="ranking_multi_indicador_rotas.csv", mime="text/csv", use_container_width=True,
                                   help="Dados completos que originaram este ranking (todos os indicadores derivados).")
            with _exp2:
                _out_ind = io.BytesIO()
                with pd.ExcelWriter(_out_ind, engine='xlsxwriter') as _w:
                    _df_show.to_excel(_w, sheet_name='Ranking Multi-Indicador', index=False)
                st.download_button("📥 Baixar ranking (.xlsx)", data=_out_ind.getvalue(),
                                   file_name="ranking_multi_indicador_rotas.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
            st.caption("💡 **Indicadores derivados:** *Sinuosidade* = viária ÷ linha reta (quanto maior, mais a rota serpenteia); "
                       "*Tempo por km* e *Velocidade Média* revelam gargalos; *Diferença Viária−Reta* destaca grandes desvios "
                       "geográficos. Combine o critério principal com o de desempate para rankings compostos.")
    else:
        st.warning("O conjunto de dados base global está vazio. Por favor, processe seu Lote para alimentar este módulo espacial.")

with tab_enciclopedia:
    st.info("📚 **Objetivo desta aba:** Servir como o repositório mestre de conhecimento. Esta enciclopédia detalha toda a jornada técnica de um dado dentro do aplicativo, abordando 100% das funcionalidades corporativas, desde a limpeza gramatical até a validação geométrica extrema anti-colisão.")
    renderizar_guia_aba("enciclopedia")
    st.markdown("# 📚 Enciclopédia Operacional e Base de Conhecimento Core")
    
    with st.expander("1. Visão Geral do Sistema", expanded=True):
        st.markdown("""
        **O que é o sistema?**
        O *Motor Nacional de Roteirização Inteligente* é uma plataforma corporativa B2B projetada para processar, em escala industrial, a conversão de endereços de texto livre em rotas matemáticas roteirizáveis.
        
        **Qual problema ele resolve?**
        Elimina a dependência de APIs logísticas frágeis (como o Google isolado, que pode falhar em áreas rurais), superando problemas de *falsos positivos topológicos*, onde endereços incompletos são jogados aleatoriamente no centro do estado ou do país.
        
        **Público-Alvo e Benefícios:**
        * **Operadores de Logística:** Descobrem o tempo viário oficial, pedágios virtuais e alocação de caminhões.
        * **Auditores de Frete:** Utilizam a plataforma para desmascarar cobranças de distância indevidas, comparando o asfalto com a linha reta geodésica.
        * **Analistas de Dados:** Aproveitam o Enterprise Analytics para mapas de calor, clusters de entrega e estatísticas robustas cruzadas por região.
        """)
        
    with st.expander("2. Arquitetura Geral e Fluxo de Dados"):
        st.markdown("""
        O sistema opera através de um funil hierárquico extremamente estrito e escalável via *ThreadPoolExecutor*:
        
        ```text
        [ ENTRADA DE DADOS ] → (Usuário insere Lote Excel ou Single-Shot)
                 ↓
        [ PARSER LEXICAL ] → (Normalização semântica, limpeza de acentos e extração de CEP)
                 ↓
        [ CACHE CHECK ] → (Intercepção instantânea de rotas já processadas)
                 ↓
        [ GEOCODIFICAÇÃO MULTIMOTOR ] → (Busca paralela no ArcGIS, Nominatim, TomTom e Photon)
                 ↓
        [ BARREIRA ANTI-COLISÃO ] → (Se Ponto A == Ponto B, força modo estrito nas APIs)
                 ↓
        [ CÁLCULO DA LINHA RETA ] → (Árbitro Supremo: WGS-84 ou Haversine)
                 ↓
        [ ROTEIRIZAÇÃO ASFÁLTICA ] → (Híbrido Google Maps + OSRM → menor distância; fallback: Geodésico)
                 ↓
        [ SCORE XAI E AUDITORIA ] → (Cálculo de penalidades e confiança baseada em Bayes)
                 ↓
        [ ANALYTICS & EXPORT ] → (Geração de Heatmaps, Tabelas Dinâmicas e Relatórios O(U))
        ```
        """)
        
    with st.expander("3. Bases de Dados Utilizadas"):
        st.markdown("""
        * **IBGE (Instituto Brasileiro de Geografia e Estatística):** Atua como malha central offline e **base nacional de referência** do motor. O sistema baixa e consome as 5.570 cidades e distritos do Brasil pela API oficial de Localidades, armazenando para cada município seu **código IBGE oficial** (7 dígitos) e resolvendo o **centróide da cidade** (centro do município, nunca um estabelecimento). É a autoridade que padroniza nomes, recupera UF + código e desambigua homônimos (ex.: Corumbá/GO × Corumbá/MS) — reconhecendo o município mesmo com o nome sem acento, em forma curta ou com pequenos erros de digitação, **antes** de consultar qualquer API. Permite o modo de sobrevivência offline caso a internet corporativa falhe.
        * **OpenStreetMap (OSM):** O maior banco de dados aberto espacial do planeta. Fundamental para estradas de terra e interior do Brasil, servindo dados para a geocodificação via Nominatim e Photon.
        * **CNEFE / Base Local:** Dicionário estrutural acoplável (opcional) mantido no cache, permitindo obediência absoluta a regras locais de filiais.
        """)
        
    with st.expander("4. APIs Utilizadas"):
        st.markdown("""
        ** Geocodificação (Texto para Lat/Lon)**
        * **ArcGIS (ESRI):** Principal motor B2B predial. Padrão-ouro em conversão de ruas com alta fidelidade na numeração corporativa.
        * **Nominatim (OpenStreetMap):** Busca minuciosa. Confiabilidade máxima para áreas rurais, lotes distantes e referências geográficas indiretas.
        * **Photon (Komoot):** Auxiliar de alta velocidade. Atua sob o OSM para fechar o triângulo do Ensemble.
        * **TomTom Logistics:** Foco na malha viária pesada e rotas de caminhões.
        
        **️ Roteirização (Traçado Viário) — Modelo Híbrido**
        * **Google Directions Engine:** um dos dois motores. Fornece asfalto, tempo, distância, mapa e link 100% auditável. Vence quando tem a menor distância (ou empate técnico ≤2%).
        * **OSRM (Open Source Routing Machine):** o segundo motor, sobre a malha aberta OpenStreetMap, avaliando até 3 alternativas. Vence quando encontra um trajeto mais curto (>2%). Quando vence, o mapa desenha a geometria EXATA da rota e há download do traçado em HTML autocontido.
        * **Seleção automática:** os dois rodam sempre; a aplicação adota a **menor distância** e exibe um comparativo auditável (diferença abs/%/tempo, selo do vencedor).
        * **Comparação dupla de rotas (novo):** independentemente de quem vença, a tela individual mostra **ambos os mapas e ambos os links** — o mapa do vencedor (principal) e o mapa do motor comparativo (não vencedor), lado a lado. Assim você audita visualmente as duas rotas de uma só vez.
        * **Projeção Geodésica Adaptativa (fallback):** se nenhum motor responder, a distância é estimada pela linha reta (WGS-84/Haversine) × fator de desvio rodoviário, de forma determinística e sinalizada — garantindo que a esteira não trave. Recomenda-se reprocessar.
        
        ** Auditoria e Cascatas**
        * **BrasilAPI, ViaCEP e OpenCEP:** Formam a "Cascata Postal-Tripla" para garantir a quebra estrutural e reversa do CEP da operação, mitigando falhas na rede.
        """)
        
    with st.expander("5. Motor de Geocodificação (Como o endereço é compreendido?)"):
        st.markdown("""
        1. **Classificação Fuzzy:** O texto passa por um classificador com a biblioteca `RapidFuzz`, que entende a tipologia: É CEP? É Condomínio? É Área Rural?
        2. **Disparo Simultâneo:** O motor atira a string normalizada para 5 provedores na nuvem ao mesmo tempo.
        3. **Consenso Espacial (DBSCAN):** Com as 5 respostas de coordenadas, o algoritmo de *Machine Learning* agrupa quem caiu perto de quem. Pontos discrepantes (outliers) são removidos.
        4. **Score de Confiança:** Calcula a penalidade multiplicando fatores. Ex: Falta de número tira 5 pontos. O motor reverso acusou estado errado tira 50 pontos.

        **🏙️ Priorização de Municípios (evita POIs indevidos):** quando você digita apenas uma cidade —
        por exemplo, **"Corumbá, GO"** ou **"Pirenópolis, GO"** — o sistema reconhece que se trata de um
        **município** (e não de um endereço, hotel, chalé ou estabelecimento). Sem esse cuidado, o Google
        poderia interpretar "Corumbá, GO" como "R. Francisco Miranda, 466" ou "Pirenópolis, GO" como um
        chalé específico, alterando distâncias e tempos. Como o sistema resolve:
        * **Detecção:** a classificação semântica identifica a entrada como MUNICÍPIO/DISTRITO (sem número
          predial, sem palavra de via, sem POI, sem CEP).
        * **Nome oficial:** a forma curta é corrigida para o nome oficial do IBGE — "Corumbá" → "Corumbá de
          Goiás" — dentro da UF informada (evitando confusão com homônimos como Corumbá-MS).
        * **Link e mapa pelo NOME oficial:** o link do Google e os rótulos do mapa passam a usar o **nome
          oficial totalmente qualificado** do município — "Corumbá de Goiás, Goiás, Brasil". Por que isso é
          seguro (não reintroduz o bug de POI): o problema antigo vinha do **texto cru e curto** do usuário
          ("Corumbá, GO"), que o Google podia interpretar como um POI. Aqui usamos o nome **oficial e
          qualificado** já resolvido pelo pipeline (via IBGE), com o estado por extenso e "Brasil" — a forma
          textual mais estável, que o Google resolve para a cidade de forma confiável **e exibe o nome** ao
          usuário (em vez de um par de coordenadas). As coordenadas seguem como âncora interna do cálculo.
        * **Endereços reais preservados:** quando você digita um endereço completo (com número) ou um POI,
          o sistema respeita essa intenção e não força o município.
        """)
        
    with st.expander("6. Motor de Roteirização (Modelo Híbrido Auditável — Google + OSRM)"):
        st.markdown("""
        O sistema primeiro exige ter a Latitude/Longitude Exata de Origem e Destino. A partir delas, aciona os dois motores de roteamento.

        **🎯 Arquitetura Híbrida: Google Maps + OSRM com seleção de menor distância**

        A aplicação executa **dois motores** em toda rota e adota automaticamente a de **MENOR
        distância**, com auditabilidade total da escolha:
        * **Google Maps:** rede oficial, em tempo real. Quando vence, fornece distância, tempo, mapa e
          link 100% auditável (o link abre exatamente a rota traçada).
        * **OSRM (OpenStreetMap):** malha aberta, avalia até 3 alternativas. Quando vence, fornece a menor
          distância e o **mapa desenha a geometria EXATA da rota** (traçado fiel via Leaflet).
        * **Tolerância de 2%:** em empate técnico, prefere-se o Google (link de navegação auditável),
          evitando alternância sem ganho real. Acima de 2%, a menor distância vence sempre.

        **Cenário 1 — Google vence:** distância, tempo e link são do Google. **O mapa embarcado SEMPRE
        desenha o traçado da rota** (curvas, conversões, segmentos), com origem e destino identificados
        **pelo nome oficial**, não por coordenadas. O OSRM aparece no comparativo (diferença abs/%/tempo).

        **Cenário 2 — OSRM vence:** distância e tempo são do OSRM; o **mapa desenha a geometria exata** do
        trajeto OSRM; há **download de um mapa HTML autocontido** com o traçado exato (abre offline em
        qualquer navegador — robusto e auditável); o **link de navegação** abre a rota no Google Maps
        (forma estável de navegar). Um **comparativo obrigatório** mostra os valores do Google ao lado,
        com selo do vencedor e a explicação da diferença.

        **🗺️ O mapa SEMPRE desenha um traçado (nunca só marcadores):** este é o ponto-chave. O mapa
        embarcado é um Leaflet autocontido que nós mesmos desenhamos (não dependemos do embed clássico
        instável do Google). A geometria do traçado segue uma **hierarquia com degradação graciosa**:
        (1) geometria do próprio **Google** (extraída e validada geograficamente — começa perto da origem,
        termina perto do destino); (2) se a extração do Google falhar, usa-se a geometria **confiável do
        OSRM** (que já roda no modelo híbrido), com trajeto praticamente idêntico, claramente rotulado como
        referência; (3) sem nenhuma geometria, desenha-se a **ligação direta** origem→destino. Em todos os
        casos, os rótulos usam o **nome oficial** das localidades, com fit bounds e zoom automáticos. Assim
        o "mapa só com 2 marcadores" foi eliminado de vez — há sempre um traçado desenhado.

        **🔗 Sobre o link da rota OSRM (investigação técnica):** não existe forma robusta, documentada e
        sustentável de um link COMPARTILHÁVEL público que abra a geometria exata do OSRM — os visualizadores
        `map.project-osrm.org` e `geojson.io` são frágeis e não-documentados, e um visualizador próprio
        exigiria hospedar uma página com URL pública persistente (fora do escopo de um app de arquivo único).
        A solução robusta e auditável adotada: o **mapa embarcado** desenha a geometria exata, o **download
        HTML** guarda o traçado exato offline, e a **navegação** usa o Google. Assim o trajeto do OSRM é
        sempre visualizável e auditável, mesmo sem um link público de terceiros.

        **Nomes guiam a experiência (coordenadas são suporte técnico):** o usuário informa nomes de
        localidades; a aplicação os identifica, valida e normaliza para os nomes oficiais; as coordenadas
        são obtidas apenas como suporte interno (cálculo e ancoragem da rota); e toda a apresentação —
        mapas, links, comparativo — é guiada pelos nomes oficiais.

        **Geocodificação para a rota:** quando você informa um município (ex.: "Corumbá, GO"), a aplicação
        ancora o ponto nas **coordenadas exatas do centróide oficial** do município — assim nenhum motor
        reinterpreta a entrada como um POI (hotel, chalé, endereço), garantindo a rota correta.

        **Fallback (Projeção Geodésica Adaptativa):** se nenhum motor responder, a distância é **estimada**
        pela linha reta × fator de desvio rodoviário — determinística e claramente sinalizada. Recomenda-se
        reprocessar para obter o valor viário oficial.
        """)
        
    with st.expander("7. Distância em Linha Reta (A Matemática do Árbitro)"):
        st.markdown(r"""
        A distância em linha reta atua como o juiz do motor. É a menor distância curva possível sobre a superfície terrestre.
        
        **Fórmulas Utilizadas:**
        * **WGS-84 (GeographicLib):** Calcula considerando o achatamento polar da Terra elipsoidal. Erro quase zero.
        * **Haversine (Contingência):** Assume a Terra como uma esfera perfeita (Raio = 6371 km).
        
        As fórmulas internas trigonométricas implementadas para fallback (Haversine):
        $$ a = \sin^2\left(\frac{\Delta\phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta\lambda}{2}\right) $$
        $$ c = 2 \cdot \text{atan2}\left(\sqrt{a}, \sqrt{1-a}\right) $$
        $$ d = 6371 \cdot c $$
        
        **Como é auditada e para que serve?**
        Se o caminhão rodou 200km e a linha reta é 10km, existe fraude, estrangulamento viário ou erro na API. A linha reta serve como base indestrutível para detectar anomalias do Google Maps.
        """)
        
    with st.expander("8. Sistema de Auditoria Interna"):
        st.markdown("""
        Todo o processo da nuvem é gravado na "Caixa Preta".
        * **Score Global (0 a 100):** Composto por: `35% Confiança Origem + 35% Confiança Destino + 30% Qualidade de Roteamento (Asfalto x Linha Reta)`.
        * **XAI (Explicabilidade):** A auditoria registra em texto exato *o porquê* de o motor ter feito a escolha. Você lerá algo como: "Correspondência administrativa confirmada via Ensemble ArcGIS + TomTom".
        """)
        
    with st.expander("9. Sistema de Cache Corporativo"):
        st.markdown("""
        O sistema é dotado de inteligência `diskcache`.
        * Se você subir 5.000 clientes e metade já foi calculada ontem, o sistema bate no banco SQLITE embarcado em milissegundos.
        * **Unpoisoning Automático:** Se por ventura uma Linha Reta falhou no passado armazenando "0", a arquitetura identifica, desfaz o cache e reprocessa na hora.
        """)
        
    with st.expander("10. Analytics Corporativo"):
        st.markdown("""
        O Enterprise Analytics consolida todos os retornos. 
        Possui filtragem bidirecional estilo Power BI: Clicar num estado de um Gráfico de Rosca reduz todos os mapas de calor, scatter plots e cálculos de tempo apenas para a volumetria daquele estado, cruzando KPIs de Distância e Tempo.

        **Estatística Descritiva da Distribuição (como interpretar):** além dos KPIs de média e máximo,
        o painel apresenta medidas que revelam o *formato* da distribuição das rotas:
        * **Mediana:** o valor central — metade das rotas é menor, metade é maior. É mais robusta que a
          média quando há rotas muito longas (outliers) puxando a média para cima.
        * **Desvio Padrão:** o quanto as distâncias variam em torno da média. Alto = rotas heterogêneas.
        * **Percentis (P25, P75, P90):** P75 = 75% das rotas têm até aquele valor. O intervalo entre P25 e
          P75 (amplitude interquartil) mostra onde está a "massa" das rotas. P90 isola os 10% mais longos.
        * **Coeficiente de Variação:** desvio padrão relativo à média (%). Regra prática: < 30% = operação
          homogênea (rotas parecidas); > 60% = muito heterogênea (mistura de curtas e longas).
        * **Leitura de assimetria:** se a média é bem maior que a mediana, a distribuição é *assimétrica à
          direita* (poucas rotas longas dominam o total) — neste caso, a **mediana** descreve melhor a rota
          típica. Esta é a situação mais comum em logística (muitas entregas locais + algumas viagens longas).

        Essas medidas apoiam decisões: dimensionar frota pela mediana (rota típica) e não pela média inflada,
        identificar se há concentração de rotas curtas, e detectar caudas longas que merecem atenção logística.
        """)
        
    with st.expander("11. Segurança e Confiabilidade"):
        st.markdown("""
        * **Failover Multi-Level:** Timeout no ArcGIS? Pula pro OSM. Timeout no OSM? Bate na Base Local IBGE. Timeout no Google Routing? Pula pro OSRM. Timeout no OSRM? Retorna a Projeção Matemática da Linha Reta.
        * O sistema foi arquitetado para nunca travar as execuções em lote, registrando os erros graciosamente nos Logs e marcando a linha do Excel afetada como "Erro Operacional", para prosseguir com os milhares de outros cálculos da fila sem paralisação.
        """)

    with st.expander("12. Comparação Dupla de Rotas (Google × OSRM)"):
        st.markdown("""
        Para **máxima auditabilidade**, a tela de rota individual agora exibe **sempre as duas rotas**, não importa quem vença:

        | Cenário | Mapa principal | Mapa comparativo | Links |
        |---|---|---|---|
        | 🟢 **Google vence** | Embed do Google (rota traçada, por nome) | 🔀 Mapa OSRM (geometria exata, Leaflet) | Link Google + Visualizador OSRM + download HTML |
        | 🔵 **OSRM vence** | Mapa OSRM (geometria exata, Leaflet) | 🔀 Embed do Google (rota traçada, por nome) | Link Google + Visualizador OSRM + download HTML |
        | 📐 **Geodésico** | Ligação direta estimada (Leaflet) | — (só há uma estimativa) | Link Google + download HTML |

        **Por que isso importa?** Você vê, na mesma tela, como cada motor traçou a rota — útil para entender divergências (uma balsa, um pedágio, uma via não pavimentada) e para justificar a escolha a clientes ou auditores. O painel **Comparativo entre Provedores** complementa com as métricas: distância de cada um, diferença absoluta e percentual, tempo e o selo do vencedor.
        """)

    with st.expander("13. Indicadores Derivados e Índice de Sinuosidade"):
        st.markdown("""
        A aba **Classificação** traz um **Ranking Multi-Indicador por Rota** que deriva, a partir da distância viária, da linha reta e do tempo, uma família de indicadores logísticos e espaciais:

        * **Índice de Sinuosidade** = distância viária ÷ distância em linha reta. É **1,0** para uma rota perfeitamente reta e cresce quanto mais a estrada serpenteia. Um valor muito alto pode indicar relevo/rios (desvio real) **ou** um erro de geocodificação — por isso é um ótimo detector de anomalias.
        * **Diferença Viária−Reta (km)** — o desvio absoluto entre o asfalto e o voo de pássaro; destaca rotas com grandes contornos geográficos.
        * **Tempo por km (min/km)** e **km por minuto** — medem a "densidade" de tempo da rota; valores fora do padrão sinalizam gargalos (trânsito, vias lentas).
        * **Velocidade Média Implícita (km/h)** = distância viária ÷ tempo — leitura rápida da fluidez do trajeto.
        * **Scores** (Global e da Rota) — confiabilidade da geocodificação/roteirização.

        No ranking você **ordena** por qualquer indicador (crescente/decrescente), adiciona um **critério de desempate**, aplica **filtros** (motor vencedor, UF, top N) e **baixa os dados** (CSV e XLSX) que originaram a visualização — pronto para auditoria externa.

        > **Nota técnica:** rotas com distância viária igual a zero (pontos coincidentes ou falha de geocodificação) aparecem com alguns indicadores em branco, pois a divisão por zero é matematicamente indefinida — o sistema trata isso sem travar.
        """)

    with st.expander("14. Motor de Processamento Contínuo (por que o lote nunca para no meio)"):
        st.markdown("""
        O Streamlit executa o script **de cima a baixo e reinicia (rerun)** a cada interação. Isso torna
        desafiador rodar tarefas longas: se o processamento inteiro rodasse numa **única execução longa**,
        a conexão em tempo real (WebSocket) do navegador poderia expirar no meio, e a tela ficava "parada"
        exigindo um novo clique.

        **Como resolvemos (arquitetura time-boxed com checkpoint):**

        ```text
        [ CLIQUE ÚNICO ] → salva o estado (fila de rotas, progresso) no session_state
                 ↓  (rerun)
        [ PRÉ-AQUECIMENTO ] → geocodifica os endpoints únicos em mini-lotes, ~8s por execução
                 ↓  (reruns automáticos, curtos)
        [ ROTEAMENTO ] → processa as rotas em mini-lotes, ~8s por execução, salvando o progresso
                 ↓  (reruns automáticos, curtos)
        [ FINALIZAÇÃO ] → monta a planilha, recalcula a linha reta e libera o download
        ```

        Cada execução é **curta e limitada por um orçamento de tempo** (não por um número fixo de linhas):
        a rede rápida processa muitas rotas por execução; a rede lenta processa menos, mas **cada execução
        continua curta**. Como o progresso é salvo a cada passo, o lote **retoma exatamente de onde parou**
        se houver qualquer interrupção — e, na prática, **avança sozinho até o fim, sem novo clique**.
        Isso se aplica tanto à aba **Processamento em Lote** quanto à aba **Alocação**.
        """)

    with st.expander("15. Camada Única de Identificação e Auditoria das Consultas aos Motores"):
        st.markdown("""
        **Uma só verdade para todos os motores.** Origem e destino passam por uma **única camada** de
        identificação antes de qualquer roteamento:

        ```text
        Texto do usuário → Normalização (acentos, caixa, abreviações)
                         → Validação na base nacional IBGE (município, UF, código)
                         → Desambiguação (homônimos pela UF)
                         → Representação ÚNICA: nome oficial + coordenada validada
        ```

        Dessa representação única, **todos os motores partem do mesmo ponto**:
        - **Google Maps** recebe o **nome oficial** (para desenhar a rota pelos nomes, sem cair em coordenadas);
        - **OSRM** recebe a **coordenada validada** (a mesma do geocode) — não reinterpreta o texto por conta própria;
        - qualquer motor futuro (GraphHopper, Valhalla) usaria exatamente a mesma origem/destino validados.

        Antes, o OSRM podia partir de um ponto genérico; agora ele usa a **mesma coordenada validada** (com a
        blindagem anti-alucinação já aplicada), eliminando divergências de interpretação.

        **Auditoria total (novo painel).** Na aba de rota individual, o expander **🔎 Auditoria das Consultas aos
        Motores de Rota** mostra, campo a campo: o texto original, o normalizado, o validado, a coordenada, o que
        foi enviado a cada motor (nome para o Google, coordenada para o OSRM), as **URLs completas** e o consenso
        (vencedor + divergência em km e %). É a rastreabilidade completa, do que você digitou ao que cada motor recebeu.
        """)

    with st.expander("16. Por que o OSRM às vezes diverge do Google (snap à malha viária) e como validamos"):
        st.markdown("""
        **A causa raiz — comprovada.** Google e OSRM recebem a **mesma** origem/destino validados. A diferença
        que às vezes aparece **não** vem de interpretações distintas do texto, e sim de como cada motor trata a
        coordenada:

        | | Google Maps | OSRM |
        |---|---|---|
        | Entrada | **nome oficial** | **coordenada validada** |
        | O que faz com ela | re-resolve o nome na **própria** malha | **projeta (snap)** a coordenada na via mais próxima do **OpenStreetMap** |
        | Efeito em área rural | usa seu ponto/rede (cobertura ampla) | se a malha OSM é esparsa, o snap pode deslocar a origem/destino em **quilômetros** |

        Ou seja: o OSRM **não usa a coordenada enviada diretamente** — ele a "gruda" no nó viário mais próximo.
        Em regiões com poucas vias mapeadas no OSM, esse *snap* afasta o ponto do local pedido. **É a causa raiz.**

        **Como tornamos isso auditável e seguro:**
        - O painel **🔎 Auditoria das Consultas aos Motores** mostra, para o OSRM, a **coordenada enviada**, a
          **coordenada usada após o snap** e o **deslocamento em metros** — a evidência direta.
        - Uma **validação espacial** confere se os pontos (após o snap) continuam **dentro dos limites da UF**
          pedida e se o snap ficou dentro do limiar. Alertas são exibidos quando algo foge do esperado.
        - **Guard de confiança:** se o snap do OSRM jogar a origem ou o destino para **fora da UF** solicitada
          (um erro objetivo), o OSRM **não é aceito como vencedor** — prevalece o Google. A regra de "menor
          distância" permanece; apenas rejeitamos um resultado do OSRM comprovadamente inválido. Quando o Google
          está indisponível, o OSRM ainda é usado (melhor que nada), mas com o alerta registrado.
        - **Mitigação ativa do snap (novo):** quando o deslocamento é grande (> 1,5 km), o sistema **não se
          conforma** — ele reúne coordenadas candidatas de **vários geocoders** (ArcGIS, Nominatim, Photon),
          mede o snap de cada uma via OSRM `/nearest` e escolhe a **coordenada road-adjacent de menor
          deslocamento que esteja dentro da UF**, re-roteando o OSRM com ela. Assim o início/fim da rota passa a
          representar melhor o local pedido. A coordenada validada (canônica) não muda — a road-adjacent é usada
          só para o cálculo do OSRM. É memoizado por município (custo amortizado no lote) e só dispara nos casos
          de snap grande (não afeta a latência das rotas urbanas). Se nenhum candidato for melhor (malha OSM
          realmente esparsa na região), o sistema informa e mantém a validação/guard como salvaguarda.

        Todo o processo — candidatos avaliados, snap de cada um, coordenada escolhida e a rota antes/depois — é
        exibido no painel **🔎 Auditoria das Consultas aos Motores**, garantindo rastreabilidade total dos ajustes.
        """)

with tab_manual:
    st.info("📖 **Bem-vindo ao Manual Operacional!** Este espaço é destinado a todos os usuários da plataforma, ensinando de forma prática o 'passo a passo' para executar as operações do dia a dia.")
    renderizar_guia_aba("manual")
    st.markdown("### 📖 Manual do Usuário e Treinamento")
    
    with st.expander("1. Primeiro Acesso e Navegação", expanded=True):
        st.markdown("""
        Ao entrar na plataforma, você verá um **Menu Lateral (Sidebar)** e **Abas Superiores**.
        * **Menu Lateral:** Contém informações estáticas e o contato de suporte (Ticket de Manutenção).
        * **Abas Superiores:** São os "módulos" do sistema. É ali que a mágica acontece. Você clica numa aba (Ex: ⚙️ Processamento Lote) e a tela muda apenas para essa função.
        """)
        
    with st.expander("2. Processamento de Rota Individual (Testes Rápidos)"):
        st.markdown("""
        **Quando usar?** Você quer saber a distância de um galpão específico até um cliente sem subir planilhas.
        **Passo a passo:**
        1. Clique na aba ** Geocodificação**.
        2. No campo **Origem**, digite o endereço completo ou coordenada (Ex: *Rua Teste, 100, São Paulo, SP*).
        3. No campo **Destino**, digite o final da viagem.
        4. Clique em ** Calcular Rota Individual**.
        5. **Resultado:** O painel exibirá as caixas (Cards) contendo a Distância de Asfalto, a Distância Aérea, e se usa balsas. Abaixo, clique no card de 'Auditoria Detalhada' para ler o log gerado pelo robô.
        """)
        
    with st.expander("3. Processamento em Lote (Milhares de Rotas simultâneas)"):
        st.markdown("""
        **Quando usar?** Você tem o faturamento do mês num Excel com milhares de entregas e quer a quilometragem oficial de todas.
        **Passo a passo:**
        1. Crie uma planilha em Excel (formato `.xlsx`). Ela **obrigatoriamente** precisa ter uma coluna chamada `Origem` e uma coluna chamada `Destino`.
        2. Entre na aba **⚙️ Processamento Lote**.
        3. Arraste e solte o arquivo no bloco pontilhado central.
        4. (Opcional) Digite sua matrícula para auditoria no campo de Operador.
        5. Clique em **Iniciar Processamento em Lote**.
        6. **Resultado:** Uma barra de progresso encherá rapidamente. No final balões sobem à tela e um botão azul ** Baixar Planilha (.xlsx)** aparecerá. Ao abrir seu novo Excel, as distâncias e as auditorias estarão preenchidas!
        7. **Exportações para mapas (GIS):** logo abaixo do botão de download, abra **🌍 Exportações Avançadas para GIS**. Lá você baixa o mesmo lote em formatos abertos para visualizar todas as rotas num mapa:
           * **GeoJSON** → abre no QGIS, ArcGIS, kepler.gl, ou em qualquer visualizador online de GeoJSON;
           * **KML** → abre no Google Earth (duplo-clique) e no Google My Maps;
           * **GPX** → para aparelhos GPS, Garmin e apps de navegação;
           * **CSV** → para Power BI, Tableau e análises em Python/Excel.
           Cada rota desenha a origem (verde), o destino (vermelho) e a linha entre eles. Como as coordenadas já foram calculadas, essas exportações são instantâneas e não atrasam o processamento.
        """)
        
    with st.expander("4. Alocação de Hubs (Descobrir o Centro de Distribuição mais próximo)"):
        st.markdown("""
        **Quando usar?** Você tem 5 Filiais e 10.000 Clientes. Você não sabe de qual filial a mercadoria de cada cliente deve sair para economizar frete.
        **Passo a passo:**
        1. Vá na aba ** Alocação de Hubs**.
        2. Suba o arquivo 1 (Seus Clientes / Entregas).
        3. Suba o arquivo 2 (A lista com as suas Filiais / Hubs).
        4. Embaixo, escolha nas caixas de seleção o nome da coluna de origem (no Excel 1) e o nome da coluna das filiais (no Excel 2).
        5. Clique em **️ Processar Cruzamento Espacial**.
        6. O sistema cruzará cada cliente contra todas as filiais na matemática. Depois, fará o duelo viário no asfalto e te devolverá um arquivo em Excel apontando exatamente a qual Centro o Cliente pertence.
        """)
        
    with st.expander("5. Calculadora Analítica"):
        st.markdown("""
        **Quando usar?** Você processou um Lote gigantesco e quer "tirar relatórios" na própria tela sem precisar abrir o Excel (Ex: Somar distâncias por Estado).
        **Passo a passo:**
        1. Após ter processado um lote, vá na aba ** Calculadora Analítica**.
        2. No painel de configuração, escolha o **Campo** (ex: `Distancia`).
        3. Escolha a **Operação** (Ex: `Soma (Sum)` ou `Média (Average)`).
        4. Escolha **Agrupar por** (Ex: `Regiao_Sintetica_Origem` ou `Status da Rota`).
        5. O gráfico e a tabela serão montados instantaneamente com a soma calculada. Você pode baixar em PDF/Excel a tabela que gerou.
        """)
        
    with st.expander("6. Classificação Territorial"):
        st.markdown("""
        **Quando usar?** Você quer agrupar municípios em faixas de "Tabela de Frete" (Ex: Cidades Críticas, Cidades Normais) **ou** classificar rotas por indicadores logísticos (sinuosidade, velocidade, tempo/km...).
        **Passo a passo:**
        1. Entre na aba ** Classificação Territorial**.
        2. Escolha se as faixas serão baseadas em "Distância" ou "Volume de Rotas".
        3. Você verá uma tabela editável na tela. Pode apagar, adicionar linhas e mudar as cores/rótulos (Ex: de `1` a `500` km = Verde, de `501` para frente = Vermelho).
        4. O sistema processará imediatamente o mapa de calor com as novas regras e te dará um botão para baixar a tabela mestre de segmentação.
        5. **Ranking Multi-Indicador (novo):** role até o fim da aba. Escolha o **critério principal** (ex.: *Índice de Sinuosidade*), um **desempate** opcional, a **ordem** (crescente/decrescente) e **filtre** por motor vencedor, UF ou top N. Baixe o resultado em **CSV ou XLSX**. Use-o para achar rotas anômalas (muito sinuosas, muito lentas) e comparar o desempenho entre os motores.
        """)
        
    with st.expander("7. Enterprise Analytics (Dashboards)"):
        st.markdown("""
        **Quando usar?** Módulo estilo Power BI para analisar a saúde logística geral e apresentar resultados em reuniões.
        **Passo a passo:**
        1. Acesse a aba ** Enterprise Analytics**.
        2. Todos os gráficos (Pizza, Barras, Linha, Mapa e Bolhas) são interativos.
        3. **Como Filtrar:** Basta clicar na fatia do estado "SP" no gráfico de Pizza. Todos os outros gráficos (Mapa, Indicadores) vão mudar na hora para mostrar os dados exclusivos de São Paulo.
        4. Para voltar, clique em um espaço branco do gráfico ou no botão " Limpar Todos os Filtros" no topo da página.
        """)
        
    with st.expander("8. Filtros Avançados"):
        st.markdown("""
        Além dos cliques nos gráficos, a aba Analytics possui caixas brancas expansíveis chamadas **"️ Painel de Controle de Filtros Avançados"**.
        Nelas você pode selecionar explicitamente Regiões, Cidades, ou arrastar a barra de distância (Slider) para forçar o dashboard a te mostrar apenas viagens entre `1.000` km e `2.000` km. A resposta é instantânea e bidirecional.
        """)
        
    with st.expander("9. Monitoramento de APIs"):
        st.markdown("""
        **Quando usar?** O sistema está demorando e você quer ver se o Google ou o servidor caíram.
        **Passo a passo:**
        1. Acesse a aba ** Monitor APIs**.
        2. A tabela informará se a Latência e os Erros (Falhas de Rede) estão normais. O indicador  significa que o fornecedor em nuvem está operando bem. O  avisa de quedas, indicando que o sistema começou a utilizar os "Fallbacks de Segurança" automaticamente.
        """)
        
    with st.expander("10. Auditoria"):
        st.markdown("""
        **Quando usar?** Você suspeita que o motor colocou um cliente na cidade errada.
        **Passo a passo:**
        1. Vá até a aba **️ Auditoria**.
        2. A tabela gigante na tela detalha o "Dossiê Investigativo". Pesquise pela sua rua ali. A coluna de "XAI Explicabilidade" mostrará exatamente a dedução lógica e cruzamento de APIs que o servidor usou.
        """)
        
    with st.expander("11. Exportações (Excel, CSV e Relatórios)"):
        st.markdown("""
        Todo o sistema foi criado para exportar fácil. 
        * Nas abas de Lote/Alocação, procure os botões retangulares azuis ou brancos como ` Baixar Planilha (.xlsx)`.
        * Na aba "Calculadora Analítica", existem opções de CSV e a "Exportação Multi-Abas" que embute o gráfico visual dentro da sua planilha de Excel corporativa pronta para a chefia.
        """)
        
    with st.expander("12. Perguntas Frequentes (FAQ Corporativo)"):
        st.markdown("""
        * **Por que uma rota retornou `0 km` ou `Input Inválido`?**
        Provavelmente a célula original no seu Excel estava vazia, ou você escreveu lixo indecifrável (ex: `%$#¨#`).
        * **O que significa o Score de Confiança?**
        Um número de 0 a 100 indicando a precisão da geocodificação. Acima de 80, a mercadoria chega na porta. Abaixo de 50, o endereço caiu apenas genericamente na cidade.
        * **O que significa a `Distância Linha Reta`?**
        É o voo de um pássaro entre o Ponto A e B ignorando ruas. É essencial para você não cair no golpe do frete "asfáltico" cobrado em rotas com desvios artificiais.
        * **Como identifico uso de balsa?**
        A coluna `Balsas` no Excel exportado sairá marcada como `Sim` se os radares aquáticos do OSRM/Google detectarem travessia obrigatória.
        * **Meus gráficos sumiram na aba Analytics. O que fazer?**
        Provavelmente seus filtros deixaram a base vazia (Ex: Filtrar Nordeste, e depois cruzar pedindo estado SP). Vá no topo da página e clique em ** Limpar Todos os Filtros**.
        """)

with tab_motores:
    st.info("🩺 **Objetivo desta aba:** Monitorar a saúde técnica do ecossistema e o Uptime (SLA) de cada parceiro. Visualize quais APIs em nuvem responderam melhor, identifique instabilidades (timeouts), observe os tempos médios de resposta e verifique a integridade algorítmica do último lote.")
    renderizar_guia_aba("motores")
    st.markdown("### 🩺 Painel de Monitoramento de Infraestrutura (APIs Health Check)")
    
    # [P34 - 3ª geração] Painel de capacidade/infraestrutura — observabilidade de recursos
    with st.expander("🖥️ Capacidade do Servidor e Configuração de Concorrência", expanded=False):
        cap1, cap2, cap3 = st.columns(3)
        cap1.metric("CPUs Detectadas", f"{_CPU_COUNT}", help="Núcleos lógicos disponíveis no ambiente de hospedagem.")
        cap2.metric("Workers de Rota (paralelos)", f"{WORKERS_DISPONIVEIS}", help="Threads simultâneas processando rotas. Adaptativo: min(32, CPUs×4). Carga é I/O-bound.")
        cap3.metric("Workers de Geocoding API", f"{EXECUTOR_APIS._max_workers}", help="Threads simultâneas consultando APIs de geocodificação por rota.")
        st.caption(f"💡 **Limite de lote atual:** até 100.000 linhas por arquivo. O gargalo dominante é a latência das APIs externas, "
                   f"não CPU/RAM. Rotas repetidas são reaproveitadas do cache L1 (RAM, {CACHE_L1_ROTAS.maxsize:,} entradas) e L2 (disco, persistente).")
    
    if 'df_processado' in st.session_state:
        df_kpi = st.session_state['df_processado']
        
        with st.container(border=True):
            col_p0, col_p1, col_p2, col_p3 = st.columns(4)
            col_p0.metric("Entradas Cache L1 (RAM)", f"{len(CACHE_L1_ROTAS)} / {CACHE_L1_ROTAS.maxsize}", help=f"Cache LRU thread-safe — limite {CACHE_L1_ROTAS.maxsize:,} entradas. Reaproveita rotas e evita OOM.")
            col_p1.metric("Tempo Médio Geocoding (Rede Externa)", f"{round(df_kpi['Tempo Geocoding (s)'].mean(), 2)} s")
            col_p2.metric("Tempo Médio Roteamento (Google/OSRM)", f"{round(df_kpi['Tempo Roteamento (s)'].mean(), 2)} s")
            col_p3.metric("Overhead Global Total / Rota", f"{round(df_kpi['Tempo Total (s)'].mean(), 2)} s")
            
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.caption("**Volume de Requisições de Resolução por Motor (Market Share Base)**")
            grafico_apis = alt.Chart(df_kpi).mark_arc(innerRadius=60).encode(
                theta=alt.Theta(field="Fonte Geocoding Origem", aggregate="count"),
                color=alt.Color(field="Fonte Geocoding Origem", type="nominal", legend=alt.Legend(title="Motores", orient='bottom')),
                tooltip=['Fonte Geocoding Origem', 'count()']
            ).properties(height=350)
            st.altair_chart(grafico_apis, use_container_width=True)
            
        with col_m2:
            st.caption("**Distribuição Qualitativa: Status Bayesiano Pós-Processamento**")
            status_palette_bar = alt.Scale(domain=['Excelente', 'Boa', 'Aceitável', 'Revisar', 'Erro'], range=['#2ECC71', '#3498DB', '#F1C40F', '#E67E22', '#E74C3C'])
            grafico_status = alt.Chart(df_kpi).mark_bar().encode(
                x=alt.X('Status da Rota:N', title='Classificação de Confiança e Exatidão'),
                y=alt.Y('count():Q', title='Volume de Requisições'),
                color=alt.Color('Status da Rota:N', scale=status_palette_bar, legend=None),
                tooltip=['Status da Rota', 'count()']
            ).properties(height=350)
            st.altair_chart(grafico_status, use_container_width=True)
            
    st.markdown("---")
    st.markdown("#### 📡 Tabela Mestre de SLA e Latência em Tempo Real")
    health_data = []
    
    for api in ["GOOGLE_MAPS", "ARCGIS", "TOMTOM", "NOMINATIM", "PHOTON", "OVERPASS", "OSRM"]:
        dados = cache_api_health.get(api, {"hits": 0, "calls": 0, "falhas": 0, "tempo_total": 0.0})
        t_med = f"{round((dados['tempo_total'] / max(1, dados['calls'])) * 1000)} ms" if dados['calls'] > 0 else "N/A"
        tx_err = f"{round((dados['falhas'] / max(1, dados['calls'] + dados['falhas'])) * 100, 1)}%" if dados['calls'] > 0 else "0.0%"
        health_data.append({
            "Provedor/Cloud Oficial": api, 
            "Status da Conexão": " Estável/Online" if dados["falhas"] == 0 else " Instável/Erros Detectados", 
            "Latência Média Observada": t_med, 
            "Taxa de Falha Sistêmica": tx_err, 
            "Total de Pings Realizados": dados["calls"]
        })
        
    st.dataframe(pd.DataFrame(health_data), use_container_width=True)
    
    st.markdown("#### 📐 Auditoria do Motor Geodésico Contínuo (Métricas de Integridade Matemática)")
    # [M24] Calcula uptime e taxas por período para observabilidade temporal
    uptime_s = time.time() - METRICAS_DISTANCIA.get("_inicio_metricas", time.time())
    uptime_h = max(uptime_s / 3600, 0.001)
    total_calc = METRICAS_DISTANCIA.get("total_calculos", 0)
    taxa_haversine_pct = round((METRICAS_DISTANCIA.get("fallback_haversine", 0) / max(1, total_calc)) * 100, 1)
    
    metricas_display = {
        "Total de Cálculos de Linha Reta": total_calc,
        "Sucesso: GeographicLib (WGS84)": METRICAS_DISTANCIA.get("sucesso_geographiclib", 0),
        "Sucesso: Geopy": METRICAS_DISTANCIA.get("sucesso_geopy", 0),
        "Fallback: Haversine": METRICAS_DISTANCIA.get("fallback_haversine", 0),
        "Correções Automáticas (Anti-Zero)": METRICAS_DISTANCIA.get("correcoes_automaticas", 0),
        "Falhas Críticas": METRICAS_DISTANCIA.get("falhas_criticas", 0),
        "Rotas Unpoisoned (Cache Reparado)": METRICAS_DISTANCIA.get("cache_unpoisoned", 0),
        "Barreiras Territoriais (Bounding Box)": METRICAS_DISTANCIA.get("barreira_territorial", 0),
        "Desambiguações Topológicas": METRICAS_DISTANCIA.get("desambiguacoes_estritas", 0),
        "Uptime da Sessão (h)": round(uptime_h, 2),
        "Taxa de Fallback Haversine (%)": taxa_haversine_pct,
        "Cálculos/hora": round(total_calc / uptime_h, 1),
    }
    df_metricas_lr = pd.DataFrame([metricas_display])
    st.dataframe(df_metricas_lr, use_container_width=True)

with tab_auditoria:
    st.info("🔍 **Objetivo desta aba:** Transparência Total e Explicabilidade (XAI). Funciona como uma 'Caixa Preta' aberta do sistema. Verifique em detalhes qual algoritmo tomou a decisão para cada coordenada e por que ele escolheu descartar outras opções em caso de empate de proximidade.")
    renderizar_guia_aba("auditoria")
    st.markdown("### 🔍 Dossiê Investigativo de Auditoria Viária e Espacial")
    
    tab_aud_lote, tab_aud_hub = st.tabs(["⚙️ Logs do Lote de Roteamento Padrão", " Logs do Motor de Alocação (Hubs Competitive)"])
    
    with tab_aud_lote:
        if 'logs_auditoria' in st.session_state and st.session_state['logs_auditoria']:
            st.write("Abaixo consta a árvore de decisões algorítmicas explicáveis tomada pelo motor durante o cálculo do Lote:")
            st.dataframe(pd.DataFrame(st.session_state['logs_auditoria']), use_container_width=True)
        else:
            st.info("Nenhum registro de auditoria em memória cache. Processe uma nova planilha corporativa na aba de Processamento em Lote (⚙️) para gerar o relatório XAI.")
            
    with tab_aud_hub:
        if 'logs_auditoria_alocacao' in st.session_state and st.session_state['logs_auditoria_alocacao']:
            st.write("Abaixo constam as inferências espaciais estritas feitas individualmente para cada Base (Destino) e Endereço (Origem) na leitura e mapeamento da Matriz Geográfica:")
            st.dataframe(pd.DataFrame(st.session_state['logs_auditoria_alocacao']), use_container_width=True)
        else:
            st.info("Nenhuma árvore de decisão persistida. Processe o cálculo de matrizes matemáticas na aba de Alocação de Hubs () para carregar as justificativas competitivas.")
