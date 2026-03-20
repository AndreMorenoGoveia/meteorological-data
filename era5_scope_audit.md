# Auditoria de Escopo ERA5-Land

## Metodo

- O repositorio contem dados locais em `iag/`, dados do INMET em `inmet/` e um unico script de extracao do ERA5-Land em [script/extract_era5_data.py](/home/andre/repos/meteorological-data/script/extract_era5_data.py).
- Nao ha codigo de feature engineering, treino ou avaliacao no repositorio. Portanto, "usada no treino?" foi classificado como "nao identificada no repositorio" para todas as variaveis do ERA5-Land.
- A decisao abaixo foi feita pelo escopo observavel dos dados locais, pela redundancia fisica e pela cobertura/missingness das fontes em Sao Paulo.
- O diretorio [script/data/era5-land](/home/andre/repos/meteorological-data/script/data/era5-land) ainda nao contem arquivos baixados; so ha estado/log da execucao. Ou seja: esta auditoria e preventiva, antes do custo de ingestao acontecer.

## Escopo inferido do projeto

- O IAG fornece series locais de superficie em Sao Paulo para temperatura do ar, ponto de orvalho, umidade relativa, umidade especifica, pressao, precipitacao, radiacao/insolacao, vento e temperatura do solo.
- O INMET no repositorio contem o Brasil inteiro, mas para um TCC local com IAG o subconjunto coerente e o de estacoes da regiao de Sao Paulo, principalmente `SAO PAULO - MIRANTE` e `SAO PAULO - INTERLAGOS`.
- Pela estrutura dos dados, o escopo mais consistente e clima local/meteorologia de superficie. Isso nao sustenta baixar variaveis de lago, neve, gelo, mascaras estaticas, vegetacao fixa e hidrologia/solo como padrao.

## 1. Inventario dos dados

| fonte | variavel | papel no pipeline | frequencia | cobertura temporal | missingness | observacoes |
| --- | --- | --- | --- | --- | --- | --- |
| IAG | Temperatura do ar (`Tar`) | target/feature principal | horaria | 2020-2025 | 0% em 2020-2022; 31.5% em 2023; 62.6% em 2024; 53.4% em 2025 | Serie local central. A partir de 2023 a serie fica incompleta, o que justifica um ERA5 minimo como backfill/exogeno. |
| IAG | Ponto de orvalho (`TPonto_Orvalho`) | target/feature principal | horaria | 2020-2025 | 0% em 2020-2022; 31.5% em 2023; 62.6% em 2024 | Redundante com UR/UE especifica para derivacoes, mas util como variavel atmosferica direta. |
| IAG | Umidade relativa (`UR`) | feature principal/derivada | horaria | 2020-2025 | 0% em 2020-2022; 31.4% em 2023; 62.6% em 2024 | Pode ser derivada de temperatura + ponto de orvalho; nao exige ERA5 proprio. |
| IAG | Umidade especifica (`UEspecifica`) | auxiliar/derivada | horaria | 2020-2025 | 0% em 2020-2022; 31.5% em 2023; 62.6% em 2024 | Forte candidata a sair do escopo principal se o foco for superficie atmosferica basica. |
| IAG | Pressao (`Pressao`) | target/feature principal | horaria | 2020-2025 | 0% em 2022; 31.5% em 2023; 53.3% em 2025; 2020/2024 em XLS nao auditados | Ja existe observacao local de pressao. |
| IAG | Precipitacao (`Precipitacao`) | target/feature principal | diaria com detalhes horarios na planilha | 2020-2025 | cerca de 0.8%-1.3% em 2020-2024 | Chuva local observada e mais aderente ao ponto do que a chuva do ERA5-Land. |
| IAG | Radiacao solar / brilho solar (`Solar`) | target/feature principal | diaria, com colunas horarias de insolacao | 2020-2025 | 0%-0.27% em 2020-2024 | Boa cobertura diaria. Para radiacao horaria regular, INMET e/ou ERA5 ainda podem ajudar. |
| IAG | Temperatura do solo 0-40 cm (`Solo`) | auxiliar / fora do escopo atual | horaria de 07h a 24h | 2020-2025 | 50.1% em 2024 no arquivo auditado de exemplo | So faz sentido se o TCC incluir memoria de solo/superficie. |
| IAG | Vento (`Vento`) | target/feature principal | horaria/mensal em planilhas XLS | 2020-2025 | nao auditado automaticamente | Os arquivos existem, mas o formato XLS antigo nao foi parseado automaticamente no ambiente atual. |
| INMET Mirante | Temperatura, ponto de orvalho, UR, pressao, vento, precipitacao, radiacao | feature/validacao complementar | horaria | 2020-2024 | 0.5%-1.5% nas variaveis atmosfericas; radiacao com 47.4% porque a serie vem vazia nas horas sem sol | Excelente estacao complementar local. |
| INMET Interlagos | Temperatura, ponto de orvalho, UR, pressao, vento, precipitacao, radiacao | feature/validacao complementar | horaria | 2020-2024 | 0.4%-3.5% nas variaveis principais; vento ~7.9%; radiacao ~45.9% pelo mesmo motivo de horas sem sol | Relevante para area metropolitana de SP. |
| INMET Barueri | Temperatura, ponto de orvalho, UR, pressao, vento, precipitacao, radiacao | opcional | horaria | 2020-2024 | precipitacao ~50%; radiacao ~66% | Menos confiavel para ser estacao base. |

## Leitura pratica do inventario

- O repositorio ja contem quase todo o estado atmosferico de superficie por observacao local.
- O gargalo nao e "falta de variavel", e sim excesso de abrangencia: o extrator do ERA5-Land esta pedindo 60 variaveis, muitas claramente fora do escopo.
- Como as series do IAG perdem cobertura depois de 2023, faz sentido manter um nucleo pequeno do ERA5-Land como apoio. Nao faz sentido manter solo, neve, lagos, evaporacao, runoff e mascaras estaticas como padrao.

## 2. Auditoria do ERA5-Land

Obs.: todas as variaveis abaixo aparecem hoje em [script/extract_era5_data.py](/home/andre/repos/meteorological-data/script/extract_era5_data.py). Nenhuma aparece em treino porque o repositorio nao tem pipeline de modelagem.

| variavel ERA5-Land | aparece no codigo? | usada no treino? | derivavel? | redundante? | decisao | justificativa |
| --- | --- | --- | --- | --- | --- | --- |
| `2m_temperature` | sim | nao identificada | nao | media | manter | Temperatura de superficie e um dos sinais mais defensaveis para backfill e como driver exogeno regular quando o IAG falha apos 2023. |
| `2m_dewpoint_temperature` | sim | nao identificada | nao | media | manter | Mantem o estado termodinamico minimo junto com temperatura e permite derivar UR/vapor pressure sem baixar outras variaveis de umidade do ERA5. |
| `10m_u_component_of_wind` | sim | nao identificada | nao | media | manter | Melhor manter o vetor de vento do que velocidade/direcao prontas. Serve de forcing atmosferico e permite derivar o resto. |
| `10m_v_component_of_wind` | sim | nao identificada | nao | media | manter | Mesmo motivo da componente U. Mantem o conjunto minimo para vento sem multiplicar variaveis. |
| `surface_solar_radiation_downwards` | sim | nao identificada | nao | media | manter | E a melhor variavel de radiacao horaria regular do ERA5 para o escopo local. IAG solar e mais diario; INMET tem blanks noturnos e algumas falhas. |
| `surface_pressure` | sim | nao identificada | nao | alta | opcional | Pressao ja existe no IAG e no INMET com boa cobertura. Vale so se voce quiser backfill adicional ou estado sinotico mais suave. |
| `total_precipitation` | sim | nao identificada | nao | alta | opcional | Chuva observada local (IAG/INMET) e mais aderente ao ponto. Deixe apenas se voce precisar de backfill ou um preditor de grande escala para episodios de chuva. |
| `skin_temperature` | sim | nao identificada | nao | alta | remover | O IAG ja tem temperatura de superficie/solo raso. Nao faz sentido baixar skin temperature por padrao no escopo atual. |
| `soil_temperature_level_1` | sim | nao identificada | nao | alta | remover | Variavel de solo fora do escopo meteorologico de superficie do TCC. |
| `soil_temperature_level_2` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `soil_temperature_level_3` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `soil_temperature_level_4` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `lake_bottom_temperature` | sim | nao identificada | nao | alta | remover | Lago nao condiz com o escopo local urbano/meteorologico observado no repositorio. |
| `lake_ice_depth` | sim | nao identificada | nao | alta | remover | Variavel fisicamente fora de escopo para Sao Paulo. |
| `lake_ice_temperature` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `lake_mix_layer_depth` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `lake_mix_layer_temperature` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `lake_shape_factor` | sim | nao identificada | nao | alta | remover | Fora de escopo e ainda e um descritor estrutural de lago. |
| `lake_total_layer_temperature` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snow_albedo` | sim | nao identificada | nao | alta | remover | Nao ha estudo de neve no repositorio. |
| `snow_cover` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snow_density` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snow_depth` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snow_depth_water_equivalent` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snowfall` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `snowmelt` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `temperature_of_snow_layer` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `skin_reservoir_content` | sim | nao identificada | nao | alta | remover | Variavel de armazenamento hidrologico/superficial sem encaixe no escopo atual. |
| `volumetric_soil_water_layer_1` | sim | nao identificada | nao | alta | remover | Umidade do solo so vale se o TCC migrar para memoria hidrica/evapotranspiracao. |
| `volumetric_soil_water_layer_2` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `volumetric_soil_water_layer_3` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `volumetric_soil_water_layer_4` | sim | nao identificada | nao | alta | remover | Mesmo motivo da camada 1. |
| `forecast_albedo` | sim | nao identificada | nao | alta | remover | Nao ha objetivo de balanco radiativo fino que justifique esse custo. |
| `surface_latent_heat_flux` | sim | nao identificada | nao | alta | remover | Fluxo turbulento de superficie fora do conjunto minimo do TCC. |
| `surface_net_solar_radiation` | sim | nao identificada | nao | alta | remover | Se radiacao for necessaria, `surface_solar_radiation_downwards` e suficiente para o MVP. |
| `surface_net_thermal_radiation` | sim | nao identificada | nao | alta | remover | Fora do nucleo minimo de meteorologia de superficie. |
| `surface_sensible_heat_flux` | sim | nao identificada | nao | alta | remover | Mesmo motivo dos outros fluxos. |
| `surface_thermal_radiation_downwards` | sim | nao identificada | nao | alta | remover | Nao ha alvo/local series que exijam radiacao termica de onda longa no MVP. |
| `evaporation_from_bare_soil` | sim | nao identificada | nao | alta | remover | Fora do escopo; depende de estudo hidrologico/solo. |
| `evaporation_from_open_water_surfaces_excluding_oceans` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `evaporation_from_the_top_of_canopy` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `evaporation_from_vegetation_transpiration` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `potential_evaporation` | sim | nao identificada | nao | alta | remover | So faria sentido num projeto hidrologico/agrometeorologico. |
| `runoff` | sim | nao identificada | nao | alta | remover | Fora de escopo para clima local de superficie. |
| `snow_evaporation` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `sub_surface_runoff` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `surface_runoff` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `total_evaporation` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `leaf_area_index_high_vegetation` | sim | nao identificada | nao | alta | remover | Descritor lento/estatico; nao justifica custo por requisicao para um ponto local. |
| `leaf_area_index_low_vegetation` | sim | nao identificada | nao | alta | remover | Mesmo motivo. |
| `high_vegetation_cover` | sim | nao identificada | nao | alta | remover | Descritor de cobertura, nao variavel atmosferica central. |
| `glacier_mask` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `lake_cover` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `low_vegetation_cover` | sim | nao identificada | nao | alta | remover | Descritor estatico, nao precisa entrar no download padrao. |
| `lake_total_depth` | sim | nao identificada | nao | alta | remover | Fora de escopo. |
| `geopotential` | sim | nao identificada | nao | alta | remover | Em serie de um ponto local, vira constante ou quase constante do grid. Custo sem ganho pratico. |
| `land_sea_mask` | sim | nao identificada | nao | alta | remover | Mascara estatica. Para um ponto fixo, nao faz sentido pedir em toda requisicao. |
| `soil_type` | sim | nao identificada | nao | alta | remover | Descritor estatico de solo; se um dia for preciso, melhor documentar uma vez, nao baixar sempre. |
| `type_of_high_vegetation` | sim | nao identificada | nao | alta | remover | Descritor estatico. |
| `type_of_low_vegetation` | sim | nao identificada | nao | alta | remover | Descritor estatico. |

## 3. Conjunto minimo recomendado

```python
ERA5_LAND_VARIABLES = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_solar_radiation_downwards",
]
```

### Por que esse e o minimo mais defensavel

- Temperatura + ponto de orvalho preservam o nucleo termodinamico com o menor numero de variaveis.
- `u` + `v` preservam o nucleo dinamico e permitem derivar velocidade/direcao localmente.
- `surface_solar_radiation_downwards` preserva o principal driver energetico horario.
- Pressao e chuva ficam como opcionais porque o repositorio ja tem observacoes locais boas para essas grandezas.

## 4. Variaveis que devem sair ja

Estas 53 variaveis devem parar de ser pedidas no escopo atual:

```python
DROP_NOW = [
    "skin_temperature",
    "soil_temperature_level_1",
    "soil_temperature_level_2",
    "soil_temperature_level_3",
    "soil_temperature_level_4",
    "lake_bottom_temperature",
    "lake_ice_depth",
    "lake_ice_temperature",
    "lake_mix_layer_depth",
    "lake_mix_layer_temperature",
    "lake_shape_factor",
    "lake_total_layer_temperature",
    "snow_albedo",
    "snow_cover",
    "snow_density",
    "snow_depth",
    "snow_depth_water_equivalent",
    "snowfall",
    "snowmelt",
    "temperature_of_snow_layer",
    "skin_reservoir_content",
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "surface_sensible_heat_flux",
    "surface_thermal_radiation_downwards",
    "evaporation_from_bare_soil",
    "evaporation_from_open_water_surfaces_excluding_oceans",
    "evaporation_from_the_top_of_canopy",
    "evaporation_from_vegetation_transpiration",
    "potential_evaporation",
    "runoff",
    "snow_evaporation",
    "sub_surface_runoff",
    "surface_runoff",
    "total_evaporation",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
    "high_vegetation_cover",
    "glacier_mask",
    "lake_cover",
    "low_vegetation_cover",
    "lake_total_depth",
    "geopotential",
    "land_sea_mask",
    "soil_type",
    "type_of_high_vegetation",
    "type_of_low_vegetation",
]
```

## 5. Variaveis opcionais para fase 2

```python
OPTIONAL_PHASE_2 = [
    "surface_pressure",
    "total_precipitation",
]
```

Use `surface_pressure` se voce quiser um estado sinotico mais completo ou preencher falhas locais de pressao.

Use `total_precipitation` apenas se o problema realmente depender de um driver de chuva de grande escala ou se a serie observada local estiver inviavel em algum recorte.

## 6. Refatoracao sugerida e aplicada

- O extrator foi refatorado para presets centralizados em [script/extract_era5_data.py](/home/andre/repos/meteorological-data/script/extract_era5_data.py).
- O default agora e `core_local_climate`.
- Para reproduzir o comportamento antigo, use `--variable-set legacy_full_catalog`.
- Para incluir pressao e precipitacao sem voltar ao catalogo inteiro, use `--variable-set surface_plus_observed_gaps`.

Exemplos:

```bash
script/extract.sh --dry-run
script/extract.sh --variable-set surface_plus_observed_gaps --dry-run
script/extract.sh --variable-set legacy_full_catalog --dry-run
```

## 7. Enxugada do INMET

O repositorio tem `2876` arquivos anuais do INMET. Para o escopo local do projeto, o recorte mais coerente e:

- `strict_sp_local`: 10 arquivos ao todo (2020-2024 de `A701 SAO PAULO - MIRANTE` e `A771 SAO PAULO - INTERLAGOS`).
- `metro_sp`: 15 arquivos ao todo (os 10 acima + `A755 BARUERI`).

Implementacao criada em [script/filter_inmet_scope.py](/home/andre/repos/meteorological-data/script/filter_inmet_scope.py).

Exemplos:

```bash
python3 script/filter_inmet_scope.py --list-presets
python3 script/filter_inmet_scope.py --preset strict_sp_local
python3 script/filter_inmet_scope.py --preset metro_sp --output-dir script/data/inmet-scope/metro_sp
```

Recomendacao pratica:

- Use `strict_sp_local` como padrao.
- Promova `BARUERI` so se voce realmente quiser uma terceira serie complementar.
- Descarte todo o restante do INMET do treinamento atual; ele aumenta volume, heterogeneidade climatica e desvia o modelo do alvo local.

## 8. Parametros do IAG que nao fazem sentido no treino base

### Manter como nucleo do modelo

- `Tar`
- `TPonto_Orvalho`
- `Pressao`
- `Precipitacao`
- `Vento`
- `Solar` se o alvo ou horizonte realmente se beneficia de um driver radiativo

### Nao usar juntos por redundancia

- `TPonto_Orvalho`, `UR`, `UEspecifica` e `TempBulboUmido` carregam quase a mesma familia psicrometrica.
- Para o treino base, escolha no maximo uma dupla simples: `Tar + TPonto_Orvalho`.
- Se usar `TPonto_Orvalho`, `UR` vira derivavel e `UEspecifica`/`TempBulboUmido` tendem a so inflar dimensionalidade.

### Candidatos fortes a exclusao do treino base

- `Solo` inteiro (`0`, `5`, `10`, `20`, `30`, `40 cm`): fora do escopo atual, com missingness alto no exemplo auditado e utilidade maior em estudos de memoria de solo/ilha de calor de superficie.
- `UEspecifica`: redundante se voce ja usar `Tar + TPonto_Orvalho` ou `Tar + UR`.
- `TempBulboUmido`: muito derivado; tende a acrescentar pouco em relacao a temperatura, orvalho e UR.
- `UR`: opcional, nao obrigatoria, se `TPonto_Orvalho` ja estiver no conjunto.

### Regra simples para o IAG

- Estado atmosferico base: `Tar`, `TPonto_Orvalho`, `Pressao`, `Vento`, `Precipitacao`.
- Driver energetico: `Solar` quando fizer sentido.
- Evite colocar mais de uma representacao de umidade no modelo base.
- Deixe `Solo`, `UEspecifica` e `TempBulboUmido` para ablacacao posterior, nao para o MVP.

## Recomendacao final

- Para o escopo atual do TCC, nao faz sentido continuar pedindo o catalogo inteiro do ERA5-Land.
- O corte mais defensavel e reduzir de 60 para 5 variaveis no padrao.
- Se voce quiser uma margem pequena para falhas das series locais, adicione so `surface_pressure` e `total_precipitation` como fase 2.
- Em paralelo, vale enxugar o proprio escopo local: no IAG, solo e algumas variaveis psicrometricas provavelmente sao auxiliares e nao deveriam dirigir o desenho do download do ERA5.
