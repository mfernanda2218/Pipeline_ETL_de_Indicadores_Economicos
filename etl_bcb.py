import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import schedule
import time
import sys

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURAÇÕES ---
# Mapeamento: Nome do Indicador -> Código da Série no BCB
SERIES_BCB = {
    'SELIC_META_ANUAL': 4189,  # Taxa Selic Meta anual
    'IPCA_MENSAL': 433         # IPCA Variação mensal
}

# String de Conexão com o Banco (Substitua pelos seus dados)
# Formato: postgresql://usuario:senha@host:porta/nome_banco
DB_CONNECTION_URI = 'postgresql://postgres:20MFdmt07@localhost:5432/bd_pipeline_etl'

def extrair_dados_bcb(codigo_serie):
    """Busca dados brutos da API do Banco Central."""
    url = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Erro ao extrair série {codigo_serie}: {e}")
        return None

def transformar_dados(dados_json, nome_indicador):
    """Limpa e formata os dados para o padrão do banco."""
    if not dados_json:
        return pd.DataFrame()
    
    df = pd.DataFrame(dados_json)
    
    # Tratamento de Tipos
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['valor'] = df['valor'].astype(float)
    
    # Renomear e Adicionar Colunas
    df = df.rename(columns={'data': 'data_referencia'})
    df['indicador'] = nome_indicador
    df['data_extracao'] = datetime.now()
    
    return df[['data_referencia', 'indicador', 'valor', 'data_extracao']]

def verificar_dados_existentes(engine, nome_indicador, data_inicio):
    """Verifica se já existem dados no banco para o indicador e período."""
    try:
        query = text("""
            SELECT COUNT(*) as count 
            FROM indicadores_economicos 
            WHERE indicador = :indicador 
            AND data_referencia >= :data_inicio
        """)
        result = engine.execute(query, {'indicador': nome_indicador, 'data_inicio': data_inicio})
        count = result.fetchone()[0]
        return count > 0
    except Exception as e:
        logging.warning(f"Erro ao verificar dados existentes: {e}")
        return False

def carregar_dados(df, engine):
    """Salva os dados no PostgreSQL evitando duplicatas."""
    if df.empty:
        logging.warning("DataFrame vazio. Nada a salvar.")
        return

    try:
        # Verificar dados mais recentes para cada indicador
        data_minima = df['data_referencia'].min().strftime('%Y-%m-%d')
        
        for indicador in df['indicador'].unique():
            df_indicador = df[df['indicador'] == indicador]
            
            # Verificar se já existem dados para este período
            if verificar_dados_existentes(engine, indicador, data_minima):
                logging.info(f"Dados já existem para {indicador} a partir de {data_minima}. Pulando upload.")
                continue
            
            # Inserir novos dados
            df_indicador.to_sql('indicadores_economicos', con=engine, if_exists='append', index=False, method='multi')
            logging.info(f"{len(df_indicador)} linhas inseridas para {indicador}.")
        
    except Exception as e:
        logging.error(f"Erro ao carregar banco de dados: {e}")

def criar_tabela_se_nao_existir(engine):
    """Cria a tabela no PostgreSQL se ela não existir."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS indicadores_economicos (
        id SERIAL PRIMARY KEY,
        data_referencia DATE NOT NULL,
        indicador VARCHAR(50) NOT NULL,
        valor NUMERIC(10,4) NOT NULL,
        data_extracao TIMESTAMP NOT NULL,
        UNIQUE(data_referencia, indicador)
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        logging.info("Tabela verificada/criada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao criar tabela: {e}")

def executar_pipeline():
    """Função principal do pipeline ETL."""
    logging.info("Iniciando Pipeline ETL...")
    
    try:
        # Criar conexão com banco
        engine = create_engine(DB_CONNECTION_URI)
        
        # Criar tabela se não existir
        criar_tabela_se_nao_existir(engine)
        
        for nome, codigo in SERIES_BCB.items():
            logging.info(f"Processando: {nome} (Código {codigo})")
            
            # 1. Extract
            dados_raw = extrair_dados_bcb(codigo)
            
            # 2. Transform
            df_limpo = transformar_dados(dados_raw, nome)
            
            # 3. Load
            carregar_dados(df_limpo, engine)
            
        logging.info("Pipeline finalizado com sucesso.")
        
    except Exception as e:
        logging.error(f"Erro no pipeline: {e}")

def agendar_execucao():
    """Configura o agendamento para execução a cada 2 dias."""
    logging.info("Configurando agendamento para execução a cada 2 dias...")
    
    # Executar imediatamente na primeira vez
    executar_pipeline()
    
    # Agendar para executar a cada 2 dias
    schedule.every(2).days.do(executar_pipeline)
    
    logging.info("Agendamento configurado. O pipeline será executado a cada 2 dias.")
    logging.info("Pressione Ctrl+C para interromper.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(3600)  # Verifica a cada hora
    except KeyboardInterrupt:
        logging.info("Agendamento interrompido pelo usuário.")

def main():
    """Função principal - escolhe entre execução única ou agendada."""
    if len(sys.argv) > 1 and sys.argv[1] == '--agendar':
        agendar_execucao()
    else:
        executar_pipeline()

if __name__ == "__main__":
    main()