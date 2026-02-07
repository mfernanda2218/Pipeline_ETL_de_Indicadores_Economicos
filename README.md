# Pipeline ETL de Indicadores Econômicos

Script Python para extrair, transformar e carregar dados da Taxa Selic e IPCA da API do Banco Central do Brasil.

Selic e IPCA são indicadores fundamentais da economia brasileira.

A taxa Selic é a taxa básica de juros, utilizada pelo Banco Central para controlar a inflação e influenciar o crédito, os investimentos e o crescimento econômico.
O IPCA é o principal índice de inflação do país, medindo a variação de preços de bens e serviços e refletindo o custo de vida da população.

Juntos, Selic e IPCA são amplamente utilizados em análises econômicas, financeiras e em projetos de dados voltados à compreensão do cenário macroeconômico brasileiro.

## Funcionalidades

- **Extração**: Dados da API de Dados Abertos do BCB (SGS)
- **Transformação**: Limpeza e formatação com Pandas
- **Carga**: Armazenamento em PostgreSQL
- **Agendamento**: Execução automática a cada 2 dias
- **Deduplicação**: Evita upload de dados já existentes

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

1. Configure a string de conexão com PostgreSQL no arquivo `etl_bcb.py`:
```python
DB_CONNECTION_URI = 'postgresql://usuario:senha@host:porta/nome_banco'
```

## Uso

### Execução única
```bash
python etl_bcb.py
```

### Execução agendada (a cada 2 dias)
```bash
python etl_bcb.py --agendar
```

## Estrutura da Tabela

```sql
CREATE TABLE indicadores_economicos (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    indicador VARCHAR(50) NOT NULL,
    valor NUMERIC(10,4) NOT NULL,
    data_extracao TIMESTAMP NOT NULL,
    UNIQUE(data_referencia, indicador)
);
```

## Indicadores

- **SELIC_META_ANUAL** (Código 4189): Taxa Selic Meta anual
- **IPCA_MENSAL** (Código 433): IPCA Variação mensal

## Logs

O script gera logs detalhados com timestamps para monitoramento da execução.
