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
#   v3.8 (86ª geração) → PRESERVAÇÃO DA LOCALIDADE NO RÓTULO [GRANULARIDADE] (o problema REAL, não o suposto)
#     O painel da 85ª PROVOU que a rota NÃO era municipalizada: origem (-15.8673,-48.0845) fica a 0.9 km
#     da Samambaia real e 23 km do centro de Brasília; destino a 2.3 km da Taguatinga real e 18.7 km do
#     centro — coordenadas CORRETAS, rota de 7.55 km coerente. O problema real é o RÓTULO: o geocoder
#     rotulava 'BRASÍLIA, DF' / 'SANDU, BRASÍLIA, DF' em vez de 'Samambaia Sul' / 'Taguatinga Sul',
#     degradando a exibição E a consulta TEXTUAL ao Google (que recebia 'BRASÍLIA' → 13.2 km, enquanto o
#     OSRM com as coordenadas certas dava 7.55 km). FIX: helper puro _preservar_localidade preserva a
#     localidade específica pedida à frente do endereço administrativo quando o geocoder o reduz ao
#     município (ou usa logradouro que não contém o termo do usuário), aplicado no pipeline logo após a
#     geocodificação — SEM tocar coordenadas (a rota já usava as corretas). Conservador: só age com termo
#     limpo (sem número/via), que não é o município e ainda não está no endereço. Provado por teste
#     (Samambaia Sul/Taguatinga Sul/Lapa/Santa Teresa/Boa Viagem preservados; Copacabana com rua já
#     específica e 'São Paulo'=município intactos). Sem regressão; 12 abas, RotaPipeline 41, balões 1×.
#   v3.8 (85ª geração) → IDENTIDADE GEOGRÁFICA + INDICADOR DE GRANULARIDADE [GRANULARIDADE] (visibilidade)
#     Você reportou que "ainda municipaliza", mas a tela só mostrava o MUNICÍPIO (identidade
#     administrativa = Brasília, CORRETA p/ RAs do DF) — sem revelar as COORDENADAS efetivamente
#     roteadas. Diagnóstico: a confiança exibida é REVISAO_MANUAL, não VALIDACAO_ANTI_ALUCINACAO → a
#     blindagem NÃO disparou (o fix da 84ª segurou); o município 'Brasília' é o rótulo administrativo, e
#     não dá p/ saber pela tela se a rota usa o ponto específico ou o centróide. FIX de visibilidade:
#     painel "🌐 Identidade Geográfica" no Validador Rápido (SEPARADO do administrativo) mostrando
#     endereço + coordenadas ROTEADAS (res_ind[19..22]) para origem/destino, com INDICADOR de
#     granularidade automático — distância do ponto ao centróide do município (≈ 0 km ⇒ municipalizado;
#     acima ⇒ ponto específico), via _identidade_por_coordenada (78ª, offline) + helper puro
#     _rotulo_granularidade. Implementa a separação administrativa × geográfica que você pediu E revela
#     objetivamente se a granularidade foi preservada. Provado por teste (_rotulo_granularidade: ≤2 km →
#     municipal; >2 km → específico; defensivos). Sem regressão; 12 abas, RotaPipeline 41, balões 1×.
#   v3.8 (84ª geração) → PRESERVAÇÃO DE GRANULARIDADE (RAs do DF) [GRANULARIDADE] (efeito colateral do IBGE)
#     Bug reportado: locais específicos (Samambaia Sul-DF, Taguatinga Sul-DF) passaram a ser
#     "municipalizados" → rota calculada como se fosse Brasília (centróide), perdendo a granularidade.
#     CAUSA RAIZ (rastreada): a blindagem anti-alucinação (_blindar_municipio) substitui o ponto
#     geocodificado pelo CENTRÓIDE municipal quando _intencao_municipio() é True E o resultado é
#     hiperespecífico. _intencao_municipio devolvia True DE IMEDIATO para tipo_entrada MUNICIPIO/DISTRITO
#     — e RAs do DF (município oficial = Brasília) caíam nisso. Além disso, a blindagem só "morde" quando
#     _centroide_municipio devolve lat/lon ≠ 0 — o que a BASE EMBUTIDA da 83ª passou a fornecer,
#     ATIVANDO a substituição que antes era inerte. FIX: _intencao_municipio só afirma intenção municipal
#     se o TERMO do usuário corresponde ao NOME do município (igual/prefixo/subconjunto de tokens); uma
#     localidade sub-municipal com nome distinto (RA/bairro/distrito, ex.: 'Samambaia Sul' ≠ 'Brasília')
#     NÃO é municipalizada → coordenadas/rota específicas preservadas. Município/Cód IBGE seguem no
#     enriquecimento/auditoria (identidade administrativa), sem degradar a geografia. Provado por teste
#     (Samambaia Sul/Taguatinga Sul preservados; município real 'Brasília' e forma curta ainda disparam;
#     via/número/POI seguem preservados). Sem regressão; 12 abas, RotaPipeline 41, balões 1×, score imut.
#   v3.8 (83ª geração) → BASE NACIONAL EMBUTIDA (OFFLINE, ZERO REDE) [IBGE-EMBUTIDA] (itens #1/#6/#8 — DEFINITIVO)
#     Você continuava vendo '—' MESMO no 82ª (com fallback GitHub) → prova de que a base fica incompleta
#     no deploy e o fallback é curto-circuitado pelo PICKLE (base cacheada >1000 mas incompleta retorna
#     antes do fallback) ou o GitHub não é alcançável. Diagnóstico decisivo: testei a normalização — ela
#     casa (semantica.normalizar('Ribeirão Cascalheira') == chave da base); logo a única explicação é a
#     base NÃO CONTER o município. Solução DEFINITIVA e offline: BASE NACIONAL EMBUTIDA no próprio código
#     (~5.570 municípios: código IBGE oficial + nome + UF + lat/lon, comprimida gzip+base64, ~120 KB),
#     MESCLADA no import DEPOIS de carregar_dados_ibge — imune ao pickle, à API do IBGE e ao GitHub.
#     Preenche qualquer município ausente sem sobrescrever a base viva; corrige de vez 'Cód IBGE: —',
#     'não identificado na base IBGE', a hierarquia por código E a cobertura nacional de Municípios
#     Próximos (os itens trazem lat/lon → centróides). Provado por teste (merge real: Ribeirão
#     Cascalheira→5107180/MT e São Miguel do Araguaia→5220207/GO resolvem sobre base vazia; base viva tem
#     prioridade; cobertura >5500). RESSALVA: hierarquia FINA (meso/micro/imediata/intermediária) ainda
#     vem da API do IBGE — se ela falhar, é a próxima (embutir a hierarquia). Sem regressão; 12 abas,
#     RotaPipeline 41, balões 1×, score imutável.
#   v3.8 (82ª geração) → FALLBACK NACIONAL IBGE (INDEP. DA API) [IBGE-ROBUSTO] (itens #1/#8 — causa raiz²)
#     'Veio vazio ainda' após a 81ª → investigação mais funda descartou UF e normalização (as regras de
#     abreviação/sinônimo NÃO tocam 'São Miguel do Araguaia'/'Ribeirão Cascalheira'). Causa raiz real mais
#     provável: a BASE não carrega quando a API do IBGE (servicodados) falha/timeouta no deploy — e aí
#     TUDO (Cód IBGE, Região, Municípios Próximos) vira '—'. FIX estrutural: fonte de FALLBACK nacional
#     confiável no GitHub (código IBGE oficial + nome + UF + lat/lon dos ~5.570 municípios), montada no
#     MESMO formato/chave da base; usada automaticamente quando a API do IBGE volta incompleta; cacheada
#     em DiskCache + pickle. Como os itens já trazem lat/lon, cascateia p/ centróides e cobertura nacional
#     de Municípios Próximos. Helpers: _parse_municipios_github (PURO) + _carregar_municipios_fallback_
#     github. Provado por teste (parsing do dataset REAL do GitHub: São Miguel do Araguaia→5220207,
#     Ribeirão Cascalheira→5107180; >5500 municípios; defensivos). RESSALVA: a hierarquia FINA (meso/
#     micro/imediata/intermediária) ainda vem da API do IBGE — se ela também falhar, é a próxima rodada.
#     Sem regressão; 12 abas, RotaPipeline 41, balões 1×, score imutável.
#   v3.8 (81ª geração) → CAUSA RAIZ 'CÓD IBGE: —' PARA NOMES ÚNICOS [IBGE-ROBUSTO] (item #1)
#     Correção estrutural do bug reportado: campos IBGE/hierarquia vazios (—) para municípios conhecidos
#     (ex.: Ribeirão Cascalheira-MT, São Miguel do Araguaia-GO). CAUSA RAIZ: _info_municipio_ibge exigia
#     correspondência de UF SEMPRE; quando extrair_uf_precisa não achava a UF no endereço geocodificado
#     (comum em municípios remotos), o Cód IBGE vinha vazio mesmo para nomes INEQUÍVOCOS — e como a
#     hierarquia (meso/micro/imediata/intermediária) é resolvida POR CÓDIGO, tudo cascateava para —.
#     FIX: (1) match por UF continua desambiguando homônimos; (2) se a UF não casar mas o nome for ÚNICO
#     na base (1 município só), resolve por nome — independe da UF; (3) _resolver_identidade_ibge herda a
#     UF OFICIAL do item quando a extração falha. Homônimos com UF ausente seguem retornando None (sem
#     desambiguação insegura). Corrige Cód IBGE, UF E a hierarquia inteira de uma vez para nomes únicos.
#     Provado por teste (código real: Ribeirão Cascalheira/São Miguel do Araguaia únicos resolvem sem UF;
#     homônimo sem UF → None; homônimo com UF → resolve; UF errada em nome único → resolve pelo único).
#     Sem regressão; 12 abas, RotaPipeline 41, balões 1×, score imutável.
#   v3.8 (80ª geração) → GEOCODIFICAÇÃO+SNAP DO CONCORRENTE: AUDIT COMPLETO [CONC-QUALIDADE] (fecha A)
#     Fecha a auditoria completa do concorrente. SNAP (distância do ponto ao eixo viário) vem de GRAÇA do
#     snap_info da rota OSRM da 79ª (dest = hub concorrente). FONTE/SCORE/CONFIANÇA da geocodificação vêm
#     do hub_qual_map — reaproveitando hub_geo (0 chamadas extras): geocodificar_endpoints_paralelo passou
#     a preservar também 'conf' (índice 7, aditivo); a Alocação monta o mapa {hub: fonte/score/conf} e o
#     passa ao builder (_montar_dataframe_final ganhou param hub_qual_map=None — Lote passa None). Builder
#     lê a qualidade por NOME do concorrente → 'Fonte/Score/Confianca Geo Concorrente' e 'Snap Concorrente
#     (m)'. Provado por teste (montagem do hub_qual_map a partir do formato real de geocodificar; leitura
#     por nome + defaults; extração de snap do snap_info; conf preservada). Sem regressão de índices
#     (geocodificar consumido por índice ≤6; res idem); 12 abas, RotaPipeline 41, balões 1×, score imut.
#   v3.8 (79ª geração) → OSRM + DIVERGÊNCIA GOOGLE×OSRM DO CONCORRENTE [CONC-OSRM] (opção A, parte 3)
#     Estende a auditoria do concorrente sem novo núcleo: roteia o runner-up TAMBÉM no OSRM
#     (API_OSRM_Routing — 1 chamada, latência aceita) e calcula a divergência Google×OSRM com a MESMA
#     métrica única do vencedor (_metricas_divergencia, sempre 0-100%). Grava no dict auditoria_concorrente:
#     osrm_km, divergencia_km/pct/classe, motor de menor distância. Isolado em try/except (falha do OSRM →
#     sem divergência, batch segue). Planilha ganhou 'OSRM km Concorrente', 'Divergencia Motores
#     Concorrente (km)/(%)', 'Motor Vencedor Concorrente'; painel exibe a divergência. RESSALVA: a chamada
#     OSRM depende de rede — NÃO executável aqui; validei a lógica de divergência (métrica única) e a
#     leitura do dict; o end-to-end você confirma reprocessando no ambiente real. Sem regressão de índices;
#     12 abas, RotaPipeline 41 (inalterado), balões 1×, score imutável.
#   v3.8 (78ª geração) → IDENTIDADE IBGE DO CONCORRENTE [CONC-IBGE] (opção A, parte 2 — sem novo núcleo)
#     Estende a auditoria do concorrente com a IDENTIDADE MUNICIPAL OFICIAL — SEM nova mudança de núcleo
#     (reaproveita as coordenadas capturadas na 77ª). Resolve o município do hub concorrente pelo
#     CENTRÓIDE MAIS PRÓXIMO à sua coordenada (Haversine/IUGG vetorizado sobre a base nacional em
#     memória) — IN-MEMORY, sem rede. Novos: _arrays_centroides_municipais (cacheado) +
#     _identidade_por_coordenada (defensivo → None). No builder (thread principal, não no worker),
#     grava 'Cod IBGE Concorrente', 'UF Concorrente', 'Municipio Concorrente'; painel exibe a identidade.
#     Colunas registradas no export. Obs.: identificação por centróide é aproximação (não point-in-
#     polygon); dist ao centróide fica disponível internamente. Provado por teste (código real com base
#     stub: escolhe o município correto por proximidade; (0,0)/base vazia/coord None → None sem quebrar).
#     Sem regressão; 12 abas, RotaPipeline 41 (inalterado nesta rodada), balões 1×, índices intactos.
#   v3.8 (77ª geração) → AUDITORIA DO CONCORRENTE NO NÚCLEO: TEMPO+VELOCIDADE [CONC-AUDIT] (opção A)
#     A pedido (opção A, "sempre", latência aceita), inicia a auditoria completa do concorrente no BATCH
#     alterando o núcleo — da forma MAIS segura possível. RotaPipeline ganhou 1 campo aditivo NO FIM,
#     'auditoria_concorrente' (dict extensível), lido SEMPRE por NOME (getattr) — não altera nenhum
#     índice 0-39 (verificado: construção por keyword, nenhum res[40+], tuplas de falha padded ≥35). O
#     ramo do runner-up passou a capturar o TEMPO do concorrente (res_g_runner[1], mesmo índice do
#     vencedor — já computado, antes descartado) e derivar VELOCIDADE MÉDIA implícita (helpers puros
#     _parse_tempo_min/_velocidade_media_kmh) + coordenadas, gravando tudo no dict via _replace. Planilha
#     ganhou 'Tempo Concorrente' e 'Velocidade Media Concorrente'; painel exibe ambos. INVARIANTE: 40 →
#     41 campos (mudança INTENCIONAL e aditiva). PRÓXIMO (mesma mecânica, o dict é extensível): OSRM +
#     divergência, Cód IBGE/fonte/score, snap — cada um some 1 chamada e será rodada dedicada. RESSALVA:
#     o fluxo (roteamento do runner-up) NÃO é executável aqui — helpers e leitura do dict testados; o
#     end-to-end você valida no ambiente real. Sem regressão de índices; 12 abas, balões 1×, score imut.
#   v3.8 (76ª geração) → COORDENADAS DO CONCORRENTE + LIMITE HONESTO DO AUDIT [CONC-COORD] (disputa)
#     Passo seguro rumo à "auditoria completa do concorrente sempre": grava as COORDENADAS próprias do
#     concorrente ('Lat Concorrente'/'Lon Concorrente'), já presentes em runner_up_map ([2]/[3]) — custo
#     ZERO, sem rede, sem tocar o núcleo. Colunas na planilha + coordenadas exibidas no painel da disputa.
#     LIMITE HONESTO (documentado): os demais dados do concorrente (Cód IBGE, fonte/score/confiança, tipo
#     do ponto, snap, divergência Google×OSRM, tempo, velocidade média) exigem rotear/geocodificar o
#     runner-up pelo PIPELINE INTEIRO + ADICIONAR CAMPOS ao RotaPipeline (NamedTuple de 40 campos, núcleo
#     acessado por índice em todo o app). Isso NÃO é validável sem executar o Streamlit; mesmo com a
#     latência aceita ("sempre"), fazê-lo às cegas violaria o zero-regressão. Fica como mudança dedicada,
#     a validar no ambiente real. Provado por teste (leitura das coordenadas de runner_up_map: [2]=lat/
#     [3]=lon; defensivo p/ tupla curta). Sem regressão; 12 abas, 40 campos, balões 1×, score 0.35/.35/.30.
#   v3.8 (75ª geração) → RADAR + ÍNDICES DE DISPUTA NA PLANILHA [DISPUTA-INDICES] (expansão sem latência)
#     Continua a expansão da Auditoria da Disputa SEM latência (derivado dos dados já gravados). PLANILHA
#     da Alocação ganhou 3 colunas: 'Indice Competitividade' (0-100, quão acirrada — 100−dif%),
#     'Indice Robustez' (0-100, quão folgada a escolha — satura em 200 km) e 'Motivo Resumido Perda'
#     (texto). Helpers puros _indice_competitividade/_indice_robustez/_motivo_resumido_perda; colunas
#     registradas nas listas de export (numéricas onde cabe). PAINEL ganhou RADAR comparativo vencedor ×
#     concorrente (plotly go.Scatterpolar — já é dependência do app; eixos normalizados 0-100:
#     proximidade viária, proximidade linha reta, diretividade V/R) + os dois índices como métricas.
#     Radar isolado em try/except. Provado por teste (índices: competitividade=100−dif%, robustez
#     saturada/clamp, motivo pelo fator dominante, defensivos; e construção real da figura de radar com
#     plotly instalado no teste). Sem regressão; 12 abas, 40 campos, balões 1×, score 0.35/0.35/0.30.
#   v3.8 (74ª geração) → AUDITORIA DA DISPUTA: "POR QUE NÃO VENCEU?" + GRÁFICO [DISPUTA-XAI] (expansão)
#     Expande a Auditoria da Disputa de Hubs com o que é DERIVÁVEL dos dados atuais (sem rerodar o
#     pipeline do concorrente — evita latência). Novo helper puro _explicar_derrota_concorrente: monta
#     os motivos estruturados de "🧠 Por que o concorrente não venceu?" a partir das diferenças já
#     calculadas (viária, linha reta corrigida da 72ª, Razão V/R), só citando o que é desfavorável ao
#     concorrente; se nada for, explica o desempate por menor viária. O painel ganhou essa seção + um
#     gráfico de barras comparativo vencedor × concorrente (viária e linha reta), isolado em try/except.
#     RESSALVA/PRÓXIMO: a auditoria COMPLETA do concorrente (Cód IBGE, coordenadas, fonte/score/
#     confiança, snap, divergência Google×OSRM, tempo, velocidade média) exige rotear o runner-up no
#     pipeline inteiro (dobra trabalho por cliente) — DOCUMENTADO como rodada dedicada opt-in. Provado
#     por teste isolado (motivos corretos por combinação de diferenças; vazio quando concorrente não é
#     pior em nada). Sem regressão; 12 abas (Pesquisa incl.), 40 campos, balões 1×, score 0.35/0.35/0.30.
#   v3.8 (73ª geração) → ABA PESQUISA DE SATISFAÇÃO [PESQUISA] (item #5 do novo prompt)
#     Nova aba "⭐ Pesquisa de Satisfação" (abas 11 → 12, mudança INTENCIONAL a pedido). Formulário
#     (st.form) com gostou/resolveu/indicaria/erro (radios), quanto ajudou + nota geral (sliders) e
#     campos livres (mais/menos gostou, melhorias, erro, comentários). Ao enviar: monta assunto+corpo
#     (helper puro _montar_corpo_pesquisa), gera link mailto URL-encoded (_mailto_pesquisa) — entrega SEM
#     backend/credenciais, robusta em qualquer ambiente — e salva backup local em DiskCache. E-mail do
#     produtor via Secrets (EMAIL_PRODUTOR, acesso defensivo) ou campo editável. Expander documenta
#     opções de envio automático em produção (FormSubmit/SMTP/Apps Script/Webhook). NOTA: Cód IBGE nas
#     planilhas (item #1 do prompt) JÁ estava implementado (54ª/71ª: Cod IBGE + UF + Região origem/
#     destino) — verificado, não reimplementado. Provado por teste isolado (helpers puros: corpo formata
#     todas as respostas + assunto com nota; mailto URL-encoded correto e reversível). Sem regressão nas
#     11 abas originais; 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (72ª geração) → CORREÇÃO CAUSA RAIZ: LINHA RETA DO CONCORRENTE [DISPUTA-FIX] (bug crítico)
#     CAUSA RAIZ do bug reportado na "🏆 Auditoria da Disputa de Hubs": a distância em LINHA RETA do
#     concorrente aparecia IGUAL à do vencedor. Motivo: o runner_up_map já trazia a linha reta própria do
#     2º colocado (dists[i2]), mas ela NUNCA era armazenada — embrulhar_task_paralela recebia
#     runner_up_info[0] e o descartava, e o painel então reusava _venc_reta (a reta do VENCEDOR) tanto na
#     linha do concorrente quanto na Razão V/R do concorrente. CORREÇÃO: _montar_dataframe_final passou a
#     gravar 'Linha Reta Concorrente' = runner_up_map[origem][0] (a reta PRÓPRIA do concorrente); o painel
#     passou a usar essa coluna para a linha reta E para a Razão V/R do concorrente, e ganhou coluna Δ
#     (Concorrente − Vencedor) em viária, linha reta e Razão V/R. Coluna também registrada nas listas de
#     export da Alocação (sai na planilha, numérica). Verificado que nenhum outro campo do concorrente
#     herdava valor do vencedor. Provado por teste (matriz real: reta do 2º ≠ reta do 1º; réplica da
#     gravação + razão do painel usando a reta correta). Sem regressão; 11 abas, 40 campos, balões 1×.
#   v3.8 (71ª geração) → REGIÃO E HOMÔNIMOS NA PLANILHA [TERRITORIO-PLANILHA] (item #3 no export)
#     Estende o item #3 ao FLUXO PRINCIPAL: Região e grau de ambiguidade (homônimos) passam a sair na
#     PLANILHA (Lote e Alocação), não só na tela do Validador Rápido. Colunas novas: "Regiao Origem",
#     "Regiao Destino" (via UF → _UF_PARA_REGIAO) e "Homonimos Origem (UFs)"/"Homonimos Destino (UFs)"
#     (nº de UFs distintas do nome na base IBGE, via _grau_ambiguidade_homonimos da 63ª). PURO e em
#     memória — SEM rede, SEM dependência nova; aditivo no builder compartilhado _montar_dataframe_final;
#     defensivo (Região "Indefinido" quando UF ausente). Provado por teste isolado (construção real das
#     colunas: Região por UF; homônimos = contagem; UF "N/A"/vazia → "Indefinido"; sem quebrar). Sem
#     regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (70ª geração) → RÓTULO DE MÉTODO UNIFICADO TELA×PLANILHA [METODO-UNIFICADO] (item #8 — fecha)
#     Unifica o rótulo do "método" entre TELA e PLANILHA sob o helper único _rotulo_metodo_rota (57ª). A
#     planilha (coluna "Metodo Utilizado", Lote e Alocação) deixa de ter lógica inline própria e passa a
#     usar o MESMO helper da tela — eliminando a divergência e, sobretudo, CORRIGINDO o rótulo do caso
#     geodésico: antes "Viária (Geodésico Adaptativo)" (impreciso — geodésico não é viário), agora
#     "Linha reta (GeographicLib)". Google/OSRM viária idênticos à tela. MUDANÇA DE STRINGS na coluna
#     "Metodo Utilizado": "Viária (Google Maps)"→"Distância viária (Google Maps)"; "Viária (OSRM -
#     fallback)"→"Distância viária (OSRM - fallback)"; "Viária (Geodésico...)"→"Linha reta
#     (GeographicLib)". Verificado que NENHUM código lê/filtra/agrupa por esse valor (só o usuário a
#     jusante) — impacto interno zero; sinalizado para você ajustar consumidores externos se houver.
#     Provado por teste (helper real: rótulos canônicos p/ Google/OSRM/geodésico/outros/vazio; planilha
#     agora chama o helper; mislabel antigo ausente). Sem regressão; 11 abas, 40 campos, balões 1×,
#     score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (69ª geração) → NÚCLEO DE SELEÇÃO POR ROTA VIÁRIA [SELECAO-VIARIA] (itens #7/#9 — núcleo testado)
#     Entrega o CORE da 2ª opção de seleção de hubs ("por rota viária") como função PURA e testada,
#     pronta para integração — SEM ligar o fluxo às cegas (a máquina de estados em chunks não é
#     executável aqui; ativá-la sem teste no ambiente real violaria o zero-regressão). Novo
#     _selecionar_hub_por_viaria(candidatos): dado os hubs candidatos de um cliente já roteados, elege o
#     de MENOR distância viária e devolve ranking (ordenado por asfalto), runner-up, margem km/%,
#     empate técnico (<5 km) e nº de candidatos válidos; descarta rotas inválidas (None/≤0). Complementa
#     o topk_map da 58ª (candidatos por linha reta → roteados → vencedor por asfalto). Docstring traz o
#     GUIA DE INTEGRAÇÃO em 5 passos (2 botões, modo opt-in, rotear top-K, eleger vencedor, painel #9),
#     com o caminho de linha reta permanecendo byte-a-byte. Função ainda NÃO conectada à UI (aguarda
#     rodada de integração com seu teste real). Provado por teste isolado (vencedor = menor viária;
#     ranking/margem/empate; descarte de inválidos; 0/1/N candidatos). Sem regressão; 11 abas,
#     40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (68ª geração) → ENCICLOPÉDIA/MANUAL ATUALIZADOS (57ª→67ª) [DOCS-UPDATE] (item #12)
#     Item #12: documentação alinhada às entregas recentes, SEM tocar lógica (puro texto). Enciclopédia
#     ganhou 4 seções novas (17–20): Método de cálculo explícito + Ranking de hubs; Identificação IBGE
#     em toda a plataforma; Hierarquia territorial + Ambiguidade de homônimos; Explorador Global +
#     filtros + gráficos + Parquet. Manual: seção 11 (Exportações) passou a citar Parquet e uma seção 13
#     nova ("Municípios Próximos e Explorador Global") ensina o passo a passo. Seções 12–16 da
#     Enciclopédia preservadas (invariante). Sem regressão; 11 abas, 40 campos, balões 1×,
#     score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (67ª geração) → PARQUET NO FLUXO LOTE/ALOCAÇÃO [PARQUET-LOTE] (item #6 no Lote/Alocação)
#     Leva o item #6 ao fluxo Lote/Alocação. A LEITURA mostrou que filtros (UF/Região) e gráficos JÁ
#     existiam na aba Analytics (não re-implementados) — o gap real era o export Parquet dos resultados.
#     Adiciona download **Parquet** nas telas de resultado do LOTE (aba Processamento) e da ALOCAÇÃO,
#     via capability-check da 65ª (só aparece com pyarrow/fastparquet; sem a lib, aviso; nunca quebra).
#     Novo helper _gerar_parquet_bytes com FALLBACK robusto: se a serialização direta falhar (colunas
#     object de tipos mistos), coage object→string e refaz. Isolado em try/except. RESSALVA: requer
#     pyarrow no requirements de produção (opt-in; degrada com elegância). Provado por teste com
#     round-trip real + caminho de fallback (coluna mista → string, relê OK). Sem regressão; 11 abas,
#     40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (66ª geração) → EXPLORADOR GLOBAL DE MUNICÍPIOS [EXPLORADOR-GLOBAL] (item #5)
#     Explorador da base INTEIRA (~5.5k municípios) na aba "Municípios Próximos" (expander próprio, sem
#     nova aba — invariante 11 abas preservado): busca por nome (lupa, ignora acento/caixa) e por código
#     IBGE (substring), filtros por UF e Região (combináveis), paginação (50/página) e export CSV +
#     Parquet (capability-check da 65ª) do conjunto FILTRADO inteiro. Núcleo em 3 helpers PUROS e
#     testáveis (_flatten_base_municipios, _filtrar_base_explorador, _paginar_lista) + base cacheada
#     (_base_municipios_explorador). Tudo em memória, sem rede; render isolado em try/except (não
#     interfere na busca por proximidade). Provado por teste isolado (código real: flatten estrutura/
#     ordena; filtro por nome/código/UF/Região e combinação; paginação com clamp e total_paginas
#     corretos; vazio → sem erro). Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30.
#   v3.8 (65ª geração) → EXPORTAÇÃO PARQUET COM CAPABILITY-CHECK [PARQUET-EXPORT] (item #6 — fecha #5/#6)
#     Fecha os itens #5/#6. Adiciona o download **Parquet** (formato colunar) dos resultados de
#     "Municípios Próximos" (tabela linha reta, respeitando o filtro territorial da 64ª). Robusto a
#     ambiente: novo helper _parquet_engine_disponivel() detecta pyarrow/fastparquet e o botão só
#     aparece quando há engine — sem a lib, exibe aviso para instalar (nunca quebra). Geração isolada em
#     try/except. Downloads passam de 2 p/ 3 colunas (CSV | Excel | Parquet). RESSALVA: para habilitar em
#     produção, adicionar `pyarrow` ao requirements — a dependência é opt-in e o app degrada com elegância
#     sem ela. Provado por teste com round-trip real (pyarrow instalado no teste): to_parquet grava e
#     read_parquet relê preservando colunas/valores; helper detecta engine presente e retorna None quando
#     ausente. Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (64ª geração) → FILTROS TERRITORIAIS + GRÁFICOS EM MUNICÍPIOS PRÓXIMOS [BUSCA-FILTROS] (itens #5/#6)
#     Entrega os pilares SEM dependência nova dos itens #5/#6 na aba "Municípios Próximos": (1) filtros
#     por UF e por Região sobre os vizinhos já calculados (helper puro _filtrar_vizinhos_por_territorio;
#     seleção vazia = visão atual IDÊNTICA — identidade, zero regressão); aplicados à tabela geodésica,
#     ao mapa, à tabela viária, aos gráficos e à exportação. (2) Gráfico de barras da distância em linha
#     reta por município (Altair, colorido por mesmo/outro Estado) e comparativo Linha Reta × Viária
#     (st.bar_chart) — ambos isolados em try/except (falha de render não afeta a aba). RESSALVA/PRÓXIMO:
#     export Parquet do item #6 NÃO foi implementado — pyarrow/fastparquet indisponíveis no ambiente de
#     teste e a dependência é decisão sua (adicionar ao requirements); DOCUMENTADO. Busca já cobre todos
#     os municípios pelo seletor nativo. Provado por teste isolado (filtro real: vazio→identidade;
#     UF/Região/combinado→subconjunto correto; preserva ordem; deduplicação de opções). Sem regressão;
#     11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (63ª geração) → GRAU DE AMBIGUIDADE DE HOMÔNIMOS [AMBIGUIDADE-HOMONIMOS] (item #3 — fecha)
#     Fecha o item #3. Novo helper _grau_ambiguidade_homonimos(municipio): conta em quantas UFs
#     DISTINTAS o mesmo nome de município aparece na base IBGE em memória (ex.: "Bom Jesus" existe em
#     várias UFs). PURO e OFFLINE (sem rede, sem dependência nova); usa a MESMA normalização da base
#     (semantica.normalizar) — coerente com _info_municipio_ibge. O painel de identidade do Validador
#     Rápido ganhou "⚖️ Grau de ambiguidade (homônimos)" para origem e destino: nome exclusivo (1 UF)
#     vs homônimo em N UFs (lista as siglas), reforçando por que informar a UF desambigua. Provado por
#     teste isolado (código real com base IBGE stub: nome em várias UFs → contagem/lista corretas e
#     ordenadas; nome único → 1; desconhecido/vazio/"—"/"N/A" → 0 sem quebrar; UFs deduplicadas).
#     Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (62ª geração) → HIERARQUIA TERRITORIAL OFICIAL DO IBGE [HIERARQUIA-IBGE] (item #3)
#     Enriquece a identidade territorial no Validador Rápido (origem E destino) com a divisão oficial
#     do IBGE: Região (derivada da UF, instantânea) + Mesorregião + Microrregião + Região Imediata +
#     Região Intermediária. Três funções novas e ISOLADAS: _parse_hierarquia_payload (puro: payload
#     /localidades/municipios → {codigo: {regiao,meso,micro,imediata,intermediaria}}, defensivo),
#     _carregar_hierarquia_ibge (baixa UMA vez a base nacional e persiste em DiskCache 30d; NÃO toca
#     carregar_dados_ibge nem o pickle — não pode regredir a base; falha graciosa → dict vazio) e
#     _hierarquia_territorial (resolve por código, defensivo → "—"). RESSALVA/latência: meso/micro/
#     imediata/intermediária NÃO são deriváveis da UF — exigem a base do IBGE; o download único é a
#     latência MITIGADA por DiskCache + spinner rotulado; sem rede, os campos ficam "—" e voltam a
#     preencher quando a base responder. Provado por teste isolado (parser real com payload sintético
#     completo/parcial; resolver com stub: código conhecido, ausente, "—"/"N/A"/None → "—"). Sem
#     regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (61ª geração) → IDENTIDADE IBGE NOS LOGS DE AUDITORIA [IBGE-LOGS] (item #2 — fecha logs)
#     Leva a identificação municipal oficial para os DOIS logs de auditoria (aba 🔍 Auditoria): Lote e
#     Alocação. Log do LOTE (montado em _montar_dataframe_final) ganha Município + UF + Cód IBGE (já
#     presentes em linha_dict desde a 54ª; já exibia Fonte/'Vencedor' e Confiança/'Score'). Log da
#     ALOCAÇÃO (hubs e origens) ganha Município + UF + Cód IBGE + Fonte da Geocodificação: para isso,
#     geocodificar_endpoints_paralelo passou a PRESERVAR município (v[5]) e fonte (v[6]) — que já
#     recebia e descartava —, mudança ADITIVA (índices 0–4 intactos; ambos os callers consomem por
#     índice ≤4). Identidade resolvida pelo helper único _resolver_identidade_ibge (base IBGE em
#     memória, sem rede). Provado por teste isolado (construção real das entradas de log: campos
#     preenchidos no caminho feliz; tupla curta/erro → '—'/'N/A' sem quebrar; retorno de 7 elementos).
#     Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (60ª geração) → CÓD IBGE NA TABELA VIÁRIA DE MUNICÍPIOS PRÓXIMOS [IBGE-PROXIMIDADE] (item #2)
#     Fecha o item #2 na aba "Municípios Próximos". A tabela de LINHA RETA já exibia "Cód. IBGE"
#     (pré-existente); a de MALHA VIÁRIA (Google/OSRM) NÃO — agora exibe também. Duas mudanças
#     aditivas: (1) cada vizinho roteado passa a levar 'codigo_ibge' (já presente na base de
#     coordenadas) para o dict viário; (2) coluna "Cód. IBGE" no _df_via, mesma posição/rótulo da
#     tabela de linha reta. Bônus: como a aba "Viaria" do Excel exporta a lista bruta, o código passa a
#     sair também na exportação. Custo ZERO, sem rede, sem dependência nova, sem tocar geocodificação/
#     roteamento. Provado por teste isolado (código real de _municipios_mais_proximos_geodesico com base
#     stub: todo vizinho carrega codigo_ibge; propagação p/ o dict viário e fallback "—" quando ausente).
#     Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (59ª geração) → CÓD IBGE NO VALIDADOR RÁPIDO [IBGE-SINGLESHOT] (item #2, parte 2)
#     Propaga a identificação municipal oficial para a TELA do Validador Rápido (Single-Shot), origem
#     E destino: painel "🗺️ Identificação Municipal Oficial (IBGE)" com Município + UF + Cód IBGE +
#     Fonte da identificação (geocoder vencedor) + Nível de confiança (rótulo + score/100). Reaproveita
#     a MESMA resolução da planilha (54ª) via novo helper único _resolver_identidade_ibge (UF de
#     extrair_uf_precisa + código de _info_municipio_ibge sobre a base IBGE em memória) — ADITIVO, sem
#     rede, sem nova dependência, sem tocar o pipeline de rota. Leitura defensiva por índice (res_ind).
#     Provado por teste isolado (município+UF conhecidos → código correto; UF Indefinido/vazio → '—';
#     município fora da base → '—'; entrada None → '—' sem exceção). PRÓXIMOS (item #2, resto): mesmos
#     campos em KPIs/logs/comparativos onde ainda faltarem. Sem regressão; 11 abas, 40 campos,
#     balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (58ª geração) → RANKING N-HUBS (TOP-5) NA DISPUTA [RANK-NHUBS] (itens #7/#9 e #9 — fundação)
#     Fundação segura para a "2ª opção de seleção por rota viária" (itens #7/#9), entregando já o
#     "ranking completo / quais quase entraram" do item #9. calcular_matriz_competitiva_vetorizada
#     passa a retornar também topk_map: por cliente, os N hubs mais próximos por LINHA RETA (teto
#     TOP-5) como lista (dist_reta_km, hub) já ordenada — ADITIVO (dest_to_hub/runner_up_map
#     inalterados), reaproveitando o MESMO argsort já feito (custo desprezível, ZERO rede). O painel
#     "🏆 Auditoria da Disputa de Hubs" ganha a tabela "Ranking dos hubs candidatos (linha reta ·
#     top-5)", marcando o escolhido (rota viária) e o concorrente roteado. Único caller atualizado
#     (5-tupla); topk_map guardado em session_state (limpo no cancelar). Provado por teste isolado
#     (top-5 ordenado correto; retornos existentes byte-a-byte iguais; 1 hub; sem hubs). RESSALVA/
#     PRÓXIMO: a SELEÇÃO por rota viária propriamente dita (rotear os top-K por cliente e escolher o
#     de MENOR distância viária) NÃO foi implementada — é cirurgia na máquina de estados em chunks e
#     AUMENTA latência (K× roteamento, opt-in); DOCUMENTADA como próxima rodada por exigir teste no
#     ambiente real (não executável aqui). Sem regressão; 11 abas, 40 campos, balões 1×,
#     score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (57ª geração) → MÉTODO UTILIZADO NA TELA [METODO-TELA] (item #8, parte 2 — conclui o item #8)
#     Torna EXPLÍCITO na interface o método da distância, complementando a coluna já existente na
#     planilha (55ª). (1) Validador Rápido (Single-Shot): linha "Método utilizado: ✓ Distância viária
#     (Google Maps)" / "(OSRM - fallback)" / "✓ Linha reta (GeographicLib)" — derivada da 'Fonte da
#     Rota' (res_ind[5]) já calculada, custo ZERO; o caso geodésico é sinalizado como estimativa.
#     (2) Alocação de Hubs: rótulo "Método de seleção dos hubs: ✓ Linha reta (GeographicLib · WGS-84)"
#     nos resultados, com nota honesta (valor via GeographicLib/Karney <1mm; ranking por Haversine/IUGG
#     de ordem idêntica) e ponteiro para a coluna 'Método Utilizado' da planilha. Novo helper único
#     _rotulo_metodo_rota reaproveitado pelas duas telas. Puramente aditivo e offline (só exibição de
#     dados já calculados) — sem nova chamada de API, sem tocar em _montar_dataframe_final. Provado por
#     teste isolado (Google→viária Google; OSRM→viária fallback; Geodésico→Linha reta GeographicLib;
#     vazio→N/A). PRÓXIMOS (item #7/#9): 2ª opção de seleção de hubs "por rota viária" na Alocação.
#     Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (56ª geração) → AZIMUTE/RUMO GEODÉSICO EM MUNICÍPIOS PRÓXIMOS [BEARING-AZIMUTE] (item #3, parte 1)
#     Nova coluna "Azimute" nas DUAS tabelas da aba Municípios Próximos (Linha Reta e Malha Viária):
#     o rumo inicial de círculo máximo da origem até cada município (Norte=0°, sentido horário) mais a
#     abreviação da rosa dos ventos em pt-BR (N, NE, L=Leste, SE, S, SO=Sudoeste, O=Oeste, NO). Cálculo
#     100% determinístico e VETORIZADO (numpy) sobre as MESMAS coordenadas já usadas na ordenação por
#     distância — custo O(n) desprezível, SEM rede, SEM nova dependência e SEM chamada de API. Helper
#     _rumo_cardeal (setores de 45°) + azimutes injetados em _municipios_mais_proximos_geodesico como
#     campos aditivos ('azimute','rumo') nos dicts de vizinho; leitura defensiva (.get) tolera sessão
#     antiga. Exportações CSV/Excel herdam a coluna automaticamente. Provado por teste isolado
#     (cardeais exatos 0/90/180/270°, 18 limites de setor, pares reais BR: Manaus→NO, Recife→NE,
#     Porto Alegre→S, Salvador→L, e reciprocidade ida/volta ≈180°). PRÓXIMOS (item #3, resto):
#     dados administrativos (Região/Meso/Microrregião via mapeamento IBGE, cacheável) e grau de
#     ambiguidade — DOCUMENTADO como próximo passo por exigir download/enriquecimento (risco de rede/
#     latência a mitigar com DiskCache). Sem regressão; 11 abas, 40 campos, balões 1×, score
#     0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (55ª geração) → MÉTODO EXPLÍCITO NA PLANILHA [METODO-EXPLICITO] (item #8, parte 1)
#     Coluna 'Metodo Utilizado' na planilha (Lote e Alocação, mesmo builder): deixa explícito o motor
#     da distância viária vencedora — "Viária (Google Maps)" (prioritário) ou "Viária (OSRM - fallback)",
#     derivado de 'Fonte da Rota' já calculada (custo ZERO, sem chamada nova). Fallback "N/A" seguro.
#     Provado por teste isolado (Google prioritário, OSRM fallback). Complementa o Código IBGE na
#     planilha (54ª). PRÓXIMOS: método na TELA do Validador/Alocação; "Linha reta (GeographicLib)" na
#     seleção de hubs por linha reta; e os demais itens do roteiro (Cód IBGE no painel Single-Shot;
#     ranking N-hubs; meso/microrregião + bearing/azimute; Google prioritário explícito nas rotas
#     viárias de Municípios Próximos). Sem regressão; 11 abas, 40 campos, balões 1×, score
#     0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (54ª geração) → CÓDIGO IBGE NA PLANILHA (LOTE E ALOCAÇÃO) [IBGE-EVERYWHERE] (item #2, parte 1)
#     Início da propagação do Código IBGE como identificador oficial da localidade em toda a app.
#     Nesta rodada: colunas 'Cod IBGE Origem', 'UF Origem', 'Cod IBGE Destino', 'UF Destino' na planilha
#     processada — servem Lote E Alocação (mesmo _montar_dataframe_final). Busca defensiva na base
#     nacional IBGE (_info_municipio_ibge) pelo município já resolvido + UF extraída do endereço oficial;
#     try/except com fallback "N/A" (nunca quebra). Custo desprezível (lookup em dict em memória).
#     PRÓXIMOS INCREMENTOS do item #2 (documentados): exibir Cód IBGE + Fonte + Confiança no painel do
#     Validador Rápido, nos KPIs, logs e comparativos. Demais itens do roteiro (ranking N-hubs;
#     meso/microrregião + bearing/azimute em Municípios Próximos; Google prioritário explícito; método
#     na tela/planilha) seguem para rodadas dedicadas. Sem regressão; 11 abas, 40 campos, balões 1×,
#     score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (53ª geração) → AUDITORIA DA DISPUTA DE HUBS NA INTERFACE [DISPUTA-HUB]
#     Item central de um roteiro amplo (12 itens): trazer para a TELA a comparação vencedor × melhor
#     concorrente da Alocação de Hub (antes só na planilha exportada). Novo painel "🏆 Auditoria da
#     Disputa de Hubs" nos resultados da Alocação: seletor de cliente → hub escolhido × melhor
#     concorrente (distância viária, linha reta, Razão V/R, tempo, score, motor), tabela comparativa,
#     Diferença (km e %, pela função centralizada da 50ª), SENSIBILIDADE da escolha (empate técnico →
#     robusta), ÍNDICE DE COMPETITIVIDADE (★★★★★) e JUSTIFICATIVA automática (por que venceu / por que
#     o concorrente perdeu). Usa dados JÁ calculados (colunas Concorrente Analisado/Distancia
#     Concorrente) — custo ZERO, sem novas chamadas de API. Provado por teste isolado (empate→★★★★★;
#     folgada→★☆☆☆☆). Demais itens do roteiro (código IBGE como identificador em toda a app; dados
#     administrativos meso/microrregião + bearing/azimute em Municípios Próximos; ranking N-hubs;
#     Google prioritário nas rotas viárias; revisão de arquitetura) DOCUMENTADOS como próximos passos
#     no relatório — escopo grande p/ rodadas dedicadas, sem inflar/arriscar numa só leva. Sem
#     regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (52ª geração) → CORREÇÃO DE COBERTURA DO "MUNICÍPIOS PRÓXIMOS" (NACIONAL) [FIX-COBERTURA]
#     BUG (aba nova da 51ª): resultados incompletos e incoerentes entre UFs. CAUSA RAIZ: a API de
#     municípios do IBGE NÃO retorna lat/lon → a base tinha coordenadas só para um SUBCONJUNTO, então
#     o ranking geodésico cobria poucos municípios (e cruzava UFs de forma incoerente por falta de
#     candidatos). SOLUÇÃO: enriquecimento com os centróides de TODOS os ~5.570 municípios —
#     _carregar_centroides_municipais baixa o dataset nacional UMA vez, persiste em DiskCache
#     (cache_base_local, 30 dias) e _municipios_com_coordenadas passa a mesclar: coordenada offline
#     da base quando houver, senão enriquece por CÓDIGO IBGE (prioritário/confiável) ou nome+UF.
#     Degradação graciosa: se o download falhar, mantém o subconjunto (sem crash) e a UI mostra a
#     COBERTURA (✅ nacional ≥5000 / ⚠️ parcial). Provado por teste isolado (enriquecimento por código
#     e nome; fallback gracioso; prioridade do código). NOTA: o download roda no runtime de produção
#     (que alcança a internet); meu ambiente de teste é restrito, então validei a LÓGICA de mesclagem
#     isoladamente. Sem regressão; 11 abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (51ª geração) → NOVA ABA "🗺️ MUNICÍPIOS PRÓXIMOS" (INTELIGÊNCIA ESPACIAL) [ABA-PROXIMIDADE]
#     11ª aba (nova, a pedido explícito). Implementa o 'Near' que fora documentado como viável:
#     estratégia de DUAS FASES (rápida/econômica) — (1) pré-filtro GEODÉSICO em memória (Haversine
#     vetorizado p/ ordenar; Karney/WGS-84 é o padrão da app) sobre os municípios da base IBGE com
#     coordenadas, retornando os N mais próximos SEM consumir APIs; (2) rota VIÁRIA sob demanda
#     (Google/OSRM via calcular_pipeline_logistico) só para os 5 já filtrados, minimizando chamadas.
#     Busca inteligente (selectbox com filtro nativo, ignora acento/caixa, por município-UF). Sinaliza
#     🔵 Mesmo Estado / 🟠 Outro Estado com XAI de integração regional; compara reta × viária; mapa
#     pydeck (origem+vizinhos+linhas, fallback st.map); tabelas ordenáveis; Razão(V/R)+faixa, balsa,
#     motor vencedor, links de auditoria (Google/OSRM); export CSV/Excel. Helpers novos
#     _municipios_com_coordenadas, _opcoes_municipios_busca, _municipios_mais_proximos_geodesico
#     (cacheados). Provado por teste isolado (origem excluída; ordenação; vizinho de outra UF incluído).
#     NOTA: cobertura do Near depende dos municípios com coordenadas na base. Sem regressão: agora 11
#     abas (intencional), 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (50ª geração) → CORREÇÃO DA DIFERENÇA (%) + MÉTRICAS CENTRALIZADAS [METRICA-UNICA]
#     BUG CORRIGIDO: a Diferença (%) do Comparativo Google×OSRM usava denominador = MENOR valor
#     (min), explodindo o percentual (ex.: 36 vs 552 km → 1433%; daí os 220/347/1342 relatados).
#     Havia DUAS fórmulas divergentes: o painel de auditoria já usava max (correto, ≤100%), o
#     comparativo usava min (errado). SOLUÇÃO: função ÚNICA _metricas_divergencia (Diferença % =
#     |a−b| ÷ MAIOR(a,b) × 100 → sempre [0,100], robusta a valor pequeno espúrio) + Diferença
#     Absoluta (km) + classificação. Roteados por ela: painel de auditoria, comparativo Google×OSRM
#     e as colunas da planilha (Lote e Alocação, mesmo builder). NOVAS COLUNAS obrigatórias: Razão
#     (V/R) + Classificação (faixas do circuity/detour factor: Muito eficiente→Extremamente elevada),
#     Diferença (%), Diferença Absoluta (km), Classificação da Divergência, Grau de Confiabilidade da
#     Medição, Observações Automáticas da Auditoria. Nova função _classificar_razao_vr. Provado por
#     teste isolado (1433%→93,5%; % sempre em [0,100]; faixas corretas). Sem regressão; 10 abas, 40
#     campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (49ª geração) → ALOCAÇÃO DE HUBS ELEVADA AO PADRÃO ENTERPRISE [ALOC-ENTERPRISE]
#     Paridade com o Processamento em Lote por REUSO (sem duplicar lógica). Descoberta-chave: a
#     Alocação JÁ usa o mesmo _montar_dataframe_final → a planilha já vinha enriquecida (links OSRM,
#     distância Google/OSRM, diferença motores, sinuosidade, barreira física, alertas — rodadas
#     43ª–47ª). O que faltava e foi somado à Alocação: (1) o mesmo SCORECARD de qualidade; (2) a mesma
#     AUDITORIA AUTOMÁTICA DE ROTAS SUSPEITAS (razão viária/reta, limiar técnico 1,8× + IQR); (3) a
#     seção "🚀 Como obter o MÁXIMO desempenho e precisão" adaptada à Alocação (2 planilhas, hubs).
#     Tudo reaproveitando renderizar_scorecard_qualidade e _auditar_rotas_suspeitas já existentes.
#     Processamento contínuo/time-boxed e enriquecimento já eram compartilhados. Sem regressão; 10
#     abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (48ª geração) → INDICADORES TERRITORIAIS NO VALIDADOR RÁPIDO [BARREIRA-SINGLE]
#     Trouxe a análise territorial (antes só na planilha em lote) para o Validador Rápido (Single-Shot),
#     com EXPLICAÇÕES: novo painel "🌍 Análise Territorial e Barreiras Físicas" mostrando fator de
#     sinuosidade (viária÷reta) + interpretação, barreira física provável (inferida) + origem do
#     cálculo/justificativa/grau de confiança, e consistência física (viária≥reta). Helper central
#     _montar_indicadores_territoriais (mesma lógica do lote, sem duplicar cálculo). Provado por teste
#     isolado (inclui o caso impossível 36 vs 1852 → INCONSISTENTE). Itens já entregues em rodadas
#     anteriores e reconfirmados no relatório: ArcGIS prioritário (43ª), Karney/WGS-84 na linha reta
#     (40ª/43ª), mitigação de snap do OSRM (41ª), auditoria de rotas suspeitas (45ª), correção do bug
#     AL→ALAMEDA (46ª). Discrepância residual do OSRM (~44km) e camadas GIS pesadas (hidrografia/DEM/
#     malha) documentadas: inerentes à malha OSM / incompatíveis com single-file. Sem regressão; 10
#     abas, 40 campos, balões 1×, score 0.35/0.35/0.30, 0 bare excepts.
#   v3.8 (47ª geração) → INFERÊNCIA DE BARREIRA FÍSICA + AVALIAÇÃO DE FERRAMENTAS SIG [ANALISE-BARREIRA]
#     Rumo a SIG profissional. Implementado (zero dependência, alta explicabilidade): coluna "Barreira
#     Fisica Provavel" na planilha, inferida do fator de sinuosidade (viária÷reta — indicador clássico
#     de desvio por obstáculo) + detecção de balsa: Sim→travessia; ≥2,2×→muito provável (rio/represa/
#     serra); ≥1,6×→provável; <0,98→inconsistente. Complementa a defesa física (viária≥reta) da 46ª e a
#     auditoria de suspeitas da 45ª. AVALIADO e DOCUMENTADO como NÃO viável in-process (custo-benefício
#     no relatório): Spatial Join, Calculate Travel Cost, camadas de hidrografia/relevo/DEM/malha
#     rodoviária/IBGE — exigem GeoPandas/GDAL/GEOS + dados offline (dezenas a centenas de MB) que o
#     runtime restrito de rede e o modelo single-file não comportam. 'Near' (município mais próximo por
#     geodésica) é viável com os centróides IBGE já em memória — documentado como próximo passo natural.
#     A identificação por Código IBGE + Nome+UF e a validação espacial (bbox UF + reverse-geo, re-armada
#     na 46ª) já cobrem o núcleo do item #3/#4. Sem regressão; 10 abas, 40 campos, balões 1×.
#   v3.8 (46ª geração) → RCA: BUG "AL→ALAMEDA" NA NORMALIZAÇÃO + DEFESAS [FIX-UF-NORMALIZA + DEFESA-FISICA]
#     CAUSA RAIZ de erro grave (Águas Belas/PE → Santana do Ipanema/AL virou Santana/AP, reta 1852km,
#     viária 36km): a expansão de abreviações mapeava r'\bAL\b'→'ALAMEDA'; como a normalização troca a
#     vírgula por espaço ANTES de expandir, "SANTANA DO IPANEMA, AL" (AL=Alagoas) virava "...ALAMEDA",
#     DESTRUINDO a UF. Sem UF, a geocodificação caiu em "SANTANA, AP" (Amapá) — e a linha reta de
#     1852km estava correta PARA as coordenadas erradas (o cálculo geodésico Karney está certo; o erro
#     foi a montante). Ponto exato: MotorEnderecoCanônico._normalizar_impl, dicionário abreviacoes_raw.
#     CORREÇÃO ESTRUTURAL: (1) removido AL→ALAMEDA (única abreviação que colide com UF); (2) BLINDAGEM
#     de UF — as 27 siglas viram sentinela (bytes nulos) antes das expansões e são restauradas depois,
#     rodando após a padronização de rodovia (AL-220 intacto). Isso RE-ARMA as validações espaciais
#     existentes (bbox UF + reverse-geo), que antes eram burladas pela UF corrompida. DEFESA FÍSICA
#     nova: viária ≥ linha reta é lei física; se viária < reta (impossível, como 36<1852), alerta
#     automático de inconsistência. Provado por teste isolado (AL preservado; AP/SP/PA/AC ok; rodovias
#     e abreviações legítimas intactas; sinuosidade<0.98 sinalizada). Sem regressão; 10 abas, 40 campos.
#   v3.8 (45ª geração) → LOTE/ALOCAÇÃO: PLANILHA ENRIQUECIDA + ETA DINÂMICA + GUIA DE DESEMPENHO
#     Foco em Processamento em Lote e Alocação. (#1/#2) Planilha processada ENRIQUECIDA com colunas
#     de auditoria extraídas do rastro já calculado (custo ZERO): Distância Google/OSRM, Diferença
#     Motores (km e %), Fator Sinuosidade, Tipo de Ponto origem/destino, Deslocamento Snap
#     origem/destino + Nível, Coord Usada OSRM (pós-snap), Validação Espacial origem/destino,
#     Mitigação de Snap, Alertas Automáticos — além do 'Link Mapa OSRM'/'Link Rota Comparativo' da
#     43ª. Serve Lote E Alocação (mesmo _montar_dataframe_final). (#6) Processamento contínuo já
#     resolvido (FLUXO-CONTINUO, 38ª/39ª) — verificado intacto. (#7) ETA DINÂMICA: combina taxa média
#     (estável) com taxa recente (reativa) via EMA, peso migrando p/ a recente conforme progride —
#     converge ao ritmo real (não mais enviesada pela partida lenta). (#8) Nova seção "🚀 Como obter
#     o MÁXIMO desempenho e precisão" na aba de Processamento (boas práticas de preenchimento,
#     padronização, limpeza, formato). Provado por testes isolados (enriquecimento, ETA, sinuosidade,
#     alertas). Sem regressão; 10 abas, 40 campos, balões 1×, score 0.35/0.35/0.30 intactos.
#   v3.8 (44ª geração) → AUDITORIA FINAL DE PRODUÇÃO + HARDENING DE CONFIABILIDADE
#     Rodada de VALIDAÇÃO (não de features): reavaliadas criticamente todas as decisões (arquitetura,
#     motores, APIs, cálculos, performance, UX). Veredito documentado no relatório: a arquitetura já
#     reflete o estado da arte após 43 gerações (Karney/WGS-84 na linha reta; consenso Bayesiano +
#     DBSCAN + validação espacial na geocodificação; ArcGIS prioritário com portões de qualidade;
#     OSRM com mitigação de snap + validação + guard; fluxo contínuo time-boxed; auditoria total).
#     ÚNICO ganho seguro aplicado: 4 cláusulas `except:` nuas → `except Exception:` (evita engolir
#     KeyboardInterrupt/SystemExit; boa prática de resiliência, risco nulo). Estudos comparativos de
#     motores (Google/OSRM/GraphHopper/Valhalla) e APIs (ArcGIS/Nominatim/Photon/TomTom/Pelias/etc.)
#     no relatório, justificando as escolhas atuais. Itens NÃO implementados por risco desproporcional
#     documentados (troca de motor, GeoPandas in-process, Redis/distribuído, circuit breaker formal,
#     migração de st.tabs). Sem regressão: 10 abas, 40 campos, balões 1×, score 0.35/0.35/0.30 intactos.
#   v3.8 (43ª geração) → ARCGIS PRIORITÁRIO + AUDITORIA DE SUSPEITAS + LINKS OSRM NO LOTE
#     [ARCGIS-PRIORITARIO + AUDIT-SUSPEITAS + OSRM-LINK-LOTE]. (#2) ArcGIS vira a FONTE GEODÉSICA
#     PRIORITÁRIA: no consenso, um candidato ArcGIS com confiança aceitável (score ≥60) é tentado
#     ANTES dos demais; a validação espacial (reverse-geo + UF/município) segue como filtro
#     obrigatório, então só caímos para fallback se o ArcGIS estiver ausente, com score baixo ou
#     reprovado na validação — hierarquia pedida SEM reduzir exatidão. Ordem de fallback pelos pesos
#     já existentes (ArcGIS>TomTom>Overpass>Nominatim>Photon). (#3) Distância em linha reta AUDITADA:
#     já usa GeographicLib/Karney WGS-84 (erro <1mm) → Geopy → Haversine IUGG, com guardas anti-zero
#     e de bounding box — é o algoritmo de máxima precisão (Karney > Vincenty); nada a trocar. (#4)
#     Links do OSRM agora também no Lote e na Alocação (colunas 'Link Mapa OSRM' e 'Link Rota
#     Comparativo') — custo ZERO (URLs derivadas das coordenadas já calculadas). (#6) Auditoria
#     automática pós-lote de ROTAS SUSPEITAS: sinaliza razão viária/reta anômala por limiar técnico
#     (≥1,8×) e estatístico (Q3+1,5·IQR), com painel dedicado. Provado por testes isolados (ArcGIS
#     priorizado/fallback; razão+IQR; classificações). (#7) Auto-preenchimento do Validador a partir
#     da planilha: DOCUMENTADO como pendente — o st.tabs não permite troca programática de aba sem
#     reestruturar a navegação das 10 abas (risco alto vs. regra inegociável); design proposto no
#     relatório. Sem regressão; 10 abas, 40 campos, balões 1×, score 0.35/0.35/0.30 intactos.
#   v3.8 (42ª geração) → AUDITORIA APROFUNDADA: NÍVEL DE SNAP + TIPO DE PONTO [AUDIT-CLASSIF]
#     Brief idêntico ao da 41ª (mitigação de snap, já implementada e intacta). Revisão crítica
#     achou 2 itens do próprio brief ainda não atendidos: item #9 "classificar o nível de
#     deslocamento" e item #8 "tipo de ponto retornado (município/centroide/endereço/POI/bairro)".
#     Adicionados 2 classificadores de AUDITORIA (não alteram cálculo): _classificar_snap (faixas
#     Excelente/Ótimo/Bom/Moderado/Alto/Crítico) e _classificar_tipo_ponto (infere o tipo a partir
#     da fonte + endereço oficial). Exibidos no painel de auditoria (tipo de ponto na identificação
#     origem/destino; nível do snap ao lado do deslocamento em metros). Provado por teste isolado
#     (2346m→Alto, 782m→Bom; centróide IBGE, endereço, logradouro, POI, bairro, município). Aditivo,
#     sem regressão, sem custo (classificação local sobre dados já capturados).
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

    # [CONC-AUDIT - 77ª geração] Auditoria COMPLETA do concorrente (2º colocado), acumulada num único
    # dicionário estruturado (tempo, velocidade média, e — em rodadas seguintes — OSRM/divergência/IBGE).
    # ADITIVO no FIM do NamedTuple: não altera nenhum índice 0-39; lido SEMPRE por NOME (getattr),
    # nunca por índice fixo. Default None (compatível com construção por keyword + tuplas de falha).
    auditoria_concorrente: dict = None

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


def _rotulo_metodo_rota(fonte_rota):
    """[METODO-TELA - 57ª geração / item #8] Rótulo legível do MÉTODO da distância, derivado de
    'Fonte da Rota' já calculada (custo ZERO, sem chamada nova). Google prioritário → OSRM fallback;
    quando nenhum motor viário respondeu, a distância exibida é a linha reta geodésica (GeographicLib).
    Padroniza o texto mostrado na TELA do Validador Rápido e da Alocação (spec do item #8)."""
    _fr = str(fonte_rota or "").upper()
    if "GOOGLE" in _fr:
        return "Distância viária (Google Maps)"
    if "OSRM" in _fr:
        return "Distância viária (OSRM - fallback)"
    if "GEOD" in _fr:
        return "Linha reta (GeographicLib)"
    if _fr and _fr not in ("DESCONHECIDA", "N/A", "NAO INFORMADA"):
        return f"Distância viária ({fonte_rota})"
    return "N/A"


def _montar_corpo_pesquisa(respostas, nota):
    """[PESQUISA - 73ª geração] Formata as respostas da Pesquisa de Satisfação num assunto + corpo de
    e-mail legível. PURO e determinístico (sem data/estado) — testável. respostas: dict {rótulo: valor}
    (ordem preservada). Retorna (assunto, corpo)."""
    assunto = f"[Pesquisa de Satisfação] Nota {nota}/10 — Motor de Roteirização Inteligente"
    _linhas = ["Nova avaliação da aplicação:", ""]
    for _rot, _val in (respostas or {}).items():
        _v = _val if (_val is not None and str(_val).strip() != "") else "—"
        _linhas.append(f"- {_rot}: {_v}")
    return assunto, "\n".join(_linhas)


def _mailto_pesquisa(email_destino, assunto, corpo):
    """[PESQUISA - 73ª geração] Monta um link mailto: para o e-mail do produtor, com assunto e corpo
    já preenchidos. PURO. Funciona SEM backend (abre o cliente de e-mail do usuário) — a forma mais
    robusta e sem dependências de entregar a avaliação. O endereço fica literal (padrão mailto); apenas
    subject e body são URL-encoded."""
    from urllib.parse import quote
    return f"mailto:{str(email_destino or '').strip()}?subject={quote(assunto)}&body={quote(corpo)}"


def _parse_tempo_min(tempo_str):
    """[CONC-AUDIT - 77ª geração] Converte um tempo formatado ('45 min', '1 h 20 min', '2 h') em
    MINUTOS (float), ou None se não parsear. PURO — base para a velocidade média implícita."""
    import re as _re
    if not tempo_str:
        return None
    _s = str(tempo_str).lower()
    _h = _re.search(r'(\d+)\s*h', _s)
    _m = _re.search(r'(\d+)\s*min', _s)
    if not _h and not _m:
        _n = _re.search(r'(\d+)', _s)
        return float(_n.group(1)) if _n else None
    _total = (int(_h.group(1)) if _h else 0) * 60 + (int(_m.group(1)) if _m else 0)
    return float(_total) if _total > 0 else None


def _velocidade_media_kmh(dist_km, tempo_str):
    """[CONC-AUDIT - 77ª geração] Velocidade média implícita (km/h) = distância / tempo. PURO.
    Retorna 0.0 se o tempo não parsear ou for zero."""
    _min = _parse_tempo_min(tempo_str)
    try:
        _d = float(dist_km)
    except (TypeError, ValueError):
        return 0.0
    if not _min or _min <= 0 or _d <= 0:
        return 0.0
    return round(_d * 60.0 / _min, 1)


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
        "dados": "**Duas planilhas .xlsx**: uma com os endereços dos clientes (Origens) e outra com os municípios/bases (Destinos
