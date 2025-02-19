#!/usr/bin/env python3

# fonte do dado:
# https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/renda-variavel/cotacoes-historicas/
# https://www.b3.com.br/en_us/market-data-and-indices/data-services/market-data/historical-data/equities/historical-quotes/

import argparse
import csv
import os
from datetime import datetime

def parse_line(line):
    """
    Recebe uma linha do arquivo COTAHIST e retorna um dicionário com os campos extraídos.
    As posições (0-indexadas) foram definidas conforme o layout oficial da B3:

    - codigo_registro:           line[0:2]
    - data_pregao:               line[2:10]   (AAAAMMDD)
    - codigo_mercado:            line[10:12]
    - codigo_negociacao:         line[12:24]  (ticker; será "strip" para remover espaços)
    - codigo_bdi:                line[24:27]
    - denominacao:               line[27:39]
    - especificador:             line[39:47]
    - identificacao_mercado:     line[47:52]
    - moeda_cotacao:             line[52:56]
    - preco_abertura:            line[56:69]
    - preco_maximo:              line[69:82]
    - preco_minimo:              line[82:95]
    - preco_fechamento:          line[95:108]
    - preco_melhor_compra:       line[108:121]
    - preco_melhor_venda:        line[121:134]
    - numero_negocios:           line[134:141]
    - quantidade_acoes:          line[141:157]
    - volume_financeiro:         line[157:173]
    - preco_fechamento_ajustado: line[173:186]
    - fator_ajuste:              line[186:190]
    - codigo_papel_registro:     line[190:] (se houver)
    """
    # Extração dos campos (usando slicing)
    codigo_registro = line[0:2]
    data_pregao = line[2:10]
    codigo_mercado = line[10:12]
    codigo_negociacao = line[12:24].strip()
    codigo_bdi = line[24:27]
    denominacao = line[27:39].strip()
    especificador = line[39:47].strip()
    identificacao_mercado = line[47:52].strip()
    moeda_cotacao = line[52:56].strip()
    preco_abertura = line[56:69]
    preco_maximo = line[69:82]
    preco_minimo = line[82:95]
    preco_fechamento = line[95:108]
    preco_melhor_compra = line[108:121]
    preco_melhor_venda = line[121:134]
    numero_negocios = line[134:141]
    quantidade_acoes = line[141:157]
    volume_financeiro = line[157:173]
    preco_fechamento_ajustado = line[173:186]
    fator_ajuste = line[186:190]
    codigo_papel_registro = line[190:].strip() if len(line) > 190 else ""

    # Conversão da data (formato AAAAMMDD para YYYY-MM-DD)
    try:
        dt = datetime.strptime(data_pregao, "%Y%m%d")
        data_pregao_formatada = dt.strftime("%Y-%m-%d")
    except Exception:
        data_pregao_formatada = data_pregao

    # Função auxiliar para converter preços (dividindo por 100)
    def convert_price(valor_str):
        try:
            return int(valor_str) / 100.0
        except:
            return None

    preco_abertura_val = convert_price(preco_abertura)
    preco_maximo_val = convert_price(preco_maximo)
    preco_minimo_val = convert_price(preco_minimo)
    preco_fechamento_val = convert_price(preco_fechamento)
    preco_melhor_compra_val = convert_price(preco_melhor_compra)
    preco_melhor_venda_val = convert_price(preco_melhor_venda)
    preco_fechamento_ajustado_val = convert_price(preco_fechamento_ajustado)
    volume_financeiro_val = convert_price(volume_financeiro)

    # Conversão dos campos inteiros
    try:
        numero_negocios_val = int(numero_negocios)
    except:
        numero_negocios_val = 0
    try:
        quantidade_acoes_val = int(quantidade_acoes)
    except:
        quantidade_acoes_val = 0
    try:
        fator_ajuste_val = int(fator_ajuste)
    except:
        fator_ajuste_val = 0

    # Cria o dicionário com os campos em snake_case (sem preposições)
    registro = {
        "codigo_registro": codigo_registro,
        "data_pregao": data_pregao_formatada,
        "codigo_mercado": codigo_mercado,
        "ativo": codigo_negociacao,
        "codigo_bdi": codigo_bdi,
        "nome": denominacao,
        "especificador": especificador,
        "identificacao_mercado": identificacao_mercado,
        "moeda_cotacao": moeda_cotacao,
        "preco_abertura": preco_abertura_val,
        "preco_maximo": preco_maximo_val,
        "preco_minimo": preco_minimo_val,
        "preco_fechamento": preco_fechamento_val,
        "preco_melhor_compra": preco_melhor_compra_val,
        "preco_melhor_venda": preco_melhor_venda_val,
        "numero_negocios": numero_negocios_val,
        "quantidade_acoes": quantidade_acoes_val,
        "volume_financeiro": volume_financeiro_val,
        "preco_fechamento_ajustado": preco_fechamento_ajustado_val,
        "fator_ajuste": fator_ajuste_val,
        "codigo_papel_registro": codigo_papel_registro
    }
    return registro

def main():
    parser = argparse.ArgumentParser(
        description="Extrai dados do arquivo COTAHIST da B3 linha a linha e gera um CSV filtrado pelos ativos desejados."
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Número máximo de linhas a serem processadas no CSV de destino.")
    parser.add_argument("ativos", help="Lista de ativos desejados, separados por vírgula (ex.: VALE3,EQPA3,CSAN3).")
    parser.add_argument("entrada", help="Arquivo de texto fonte ou diretório contendo os arquivos (ex.: COTAHIST_A2024.TXT ou ./dados/).")
    parser.add_argument("arquivo_csv", help="Arquivo CSV de destino (ex.: papeis.csv).")
    args = parser.parse_args()

    # Converte a lista de ativos para um conjunto para busca mais rápida
    ativos = set(args.ativos.split(","))
    entrada = args.entrada
    output_filename = args.arquivo_csv
    limit = args.limit

    # Verifica se 'entrada' é um diretório ou um arquivo
    if os.path.isdir(entrada):
        # Se for diretório, obtém a lista de arquivos (não recursivamente)
        arquivos = [os.path.join(entrada, f) for f in os.listdir(entrada)
                    if os.path.isfile(os.path.join(entrada, f))]
        arquivos.sort()
    else:
        arquivos = [entrada]

    # Define a ordem (e os nomes) das colunas no CSV
    fieldnames = [
        "codigo_registro",
        "data_pregao",
        "codigo_mercado",
        "ativo",
        "codigo_bdi",
        "nome",
        "especificador",
        "identificacao_mercado",
        "moeda_cotacao",
        "preco_abertura",
        "preco_maximo",
        "preco_minimo",
        "preco_fechamento",
        "preco_melhor_compra",
        "preco_melhor_venda",
        "numero_negocios",
        "quantidade_acoes",
        "volume_financeiro",
        "preco_fechamento_ajustado",
        "fator_ajuste",
        "codigo_papel_registro"
    ]

    linhas_processadas = 0

    # Abre o CSV de saída (único arquivo de destino)
    with open(output_filename, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        # Para cada arquivo de entrada (ou o único arquivo, se 'entrada' for um arquivo)
        for arq in arquivos:
            with open(arq, "r", encoding="utf-8") as infile:
                for line in infile:
                    # Pula linhas muito curtas (ou inválidas)
                    if len(line) < 2:
                        continue
                    # Processa somente registros de dados (código "01" nas duas primeiras posições)
                    if line[0:2] != "01":
                        continue

                    registro = parse_line(line)

                    # Filtra os ativos conforme o parâmetro informado
                    if registro["ativo"] not in ativos:
                        continue

                    writer.writerow(registro)
                    linhas_processadas += 1

                    # Se o parâmetro --limit foi definido, interrompe após atingir o limite
                    if limit is not None and linhas_processadas >= limit:
                        break

                # Se o limite já foi atingido, interrompe a leitura de novos arquivos
                if limit is not None and linhas_processadas >= limit:
                    break

    print(f"Arquivo CSV '{output_filename}' gerado com sucesso! Total de linhas: {linhas_processadas}")

if __name__ == "__main__":
    main()
