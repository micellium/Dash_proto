# RESUMO DAS MUDANÇAS: Refatorado o código para usar cursores independentes para cada operação de banco de dados, resolvendo o erro "Connection is busy".
import streamlit as st
import pandas as pd
import pyodbc
import json
from typing import List, Dict, Any

# --- Repositório para a tabela TIXLOG ---
class TixlogRepository:
    """
    Gerencia todas as interações com a tabela [indigo_pix].[dbo].[TIXLOG].
    Cada método corresponde a uma operação de busca específica nesta tabela.
    """
    def __init__(self, connection):
        # Inicializa o repositório com uma conexão de banco de dados ativa.
        self._connection = connection

    def _format_results(self, cursor) -> List[Dict[str, Any]]:
        """
        Função auxiliar para converter os resultados brutos do banco de dados (lista de tuplas)
        em um formato mais amigável para Python (lista de dicionários).

        Returns:
            List[Dict[str, Any]]: Uma lista de dicionários, onde cada dicionário representa uma linha da tabela.
        """
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _execute_query(self, base_query: str, where_clause: str, params: tuple) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SQL de forma segura e padronizada, usando um cursor temporário.

        Args:
            base_query (str): A parte principal da query (SELECT ... FROM ...).
            where_clause (str): A condição da cláusula WHERE.
            params (tuple): Os parâmetros a serem passados de forma segura para a query.

        Returns:
            List[Dict[str, Any]]: O resultado da consulta formatado.
        """
        cursor = self._connection.cursor()
        try:
            sql_query = f"{base_query} WHERE {where_clause} ORDER BY ID DESC"
            cursor.execute(sql_query, params)
            return self._format_results(cursor)
        finally:
            cursor.close()

    def find_by_nr_controle(self, nr_controle: str) -> List[Dict[str, Any]]:
        """Busca registros por um NR_CONTROLE específico."""
        if not nr_controle: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        query = "SELECT TOP (1000) * FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)"
        return self._execute_query(query, "NR_CONTROLE = ?", (nr_controle,))

    def find_by_idreqjdpi(self, idreqjdpi: str) -> List[Dict[str, Any]]:
        """Busca registros por um IDREQJDPI específico."""
        if not idreqjdpi: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        query = "SELECT TOP (1000) * FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)"
        return self._execute_query(query, "IDREQJDPI = ?", (idreqjdpi,))

    def find_by_nr_controle_in(self, nr_controles: List[str]) -> List[Dict[str, Any]]:
        """Busca registros que correspondam a uma lista de NR_CONTROLE."""
        if not nr_controles: return [] # Bifurcação: retorna lista vazia se não houver itens para buscar.
        query = "SELECT TOP (1000) * FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)"
        placeholders = ', '.join(['?' for _ in nr_controles])
        where_clause = f"NR_CONTROLE IN ({placeholders})"
        return self._execute_query(query, where_clause, tuple(nr_controles))

    def find_by_json_content(self, text_to_find: str) -> List[Dict[str, Any]]:
        """Busca um texto dentro das colunas JSON e JSON_RETORNO."""
        if not text_to_find: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        cursor = self._connection.cursor()
        try:
            param_value = f"%{text_to_find}%"
            sql_query = """
                SELECT TOP (1000) 
                    *,
                    CASE
                        WHEN [JSON] LIKE ? AND [JSON_RETORNO] LIKE ? THEN 'Ambos'
                        WHEN [JSON] LIKE ? THEN 'JSON Enviado'
                        WHEN [JSON_RETORNO] LIKE ? THEN 'JSON Retorno'
                    END AS LocalEncontrado
                FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)
                WHERE ([JSON] LIKE ? OR [JSON_RETORNO] LIKE ?)
                ORDER BY ID DESC
            """
            params = (param_value, param_value, param_value, param_value, param_value, param_value)
            cursor.execute(sql_query, params)
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao executar a busca no JSON: {ex}")
            return []
        finally:
            cursor.close()

    def find_by_origem(self, origem: str) -> List[Dict[str, Any]]:
        """Busca registros por uma ORIGEM específica."""
        if not origem: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        query = "SELECT TOP (1000) * FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)"
        return self._execute_query(query, "ORIGEM = ?", (origem,))
        
    def get_new_entries_per_minute(self) -> List[Dict[str, Any]]:
        """
        Calcula o número de novos NR_CONTROLE por minuto nas últimas 24 horas para a seção de estatísticas.
        """
        cursor = self._connection.cursor()
        try:
            sql_query = """
                WITH PrimeirasOcorrencias AS (
                    SELECT
                        [DATAHORA],
                        [NR_CONTROLE],
                        ROW_NUMBER() OVER(PARTITION BY [NR_CONTROLE] ORDER BY [DATAHORA] ASC) AS OrdemAparicao
                    FROM
                        [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)
                    WHERE [DATAHORA] >= DATEADD(day, -1, GETDATE())
                )
                SELECT
                    FORMAT([DATAHORA], 'yyyy-MM-dd HH:mm') AS [MinutoFormatado],
                    COUNT(*) AS [NovosNrControlePorMinuto]
                FROM
                    PrimeirasOcorrencias
                WHERE
                    OrdemAparicao = 1
                GROUP BY
                    FORMAT([DATAHORA], 'yyyy-MM-dd HH:mm')
                ORDER BY
                    [MinutoFormatado] ASC;
            """
            cursor.execute(sql_query)
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao buscar estatísticas: {ex}")
            return []
        finally:
            cursor.close()

    def get_transaction_summary(self, nr_controle: str) -> List[Dict[str, Any]]:
        """
        Calcula o tempo total e a quantidade de etapas para um único NR_CONTROLE.
        """
        if not nr_controle: return []
        cursor = self._connection.cursor()
        try:
            sql_query = """
                WITH OperationType_CTE AS (
                    SELECT
                        NR_CONTROLE,
                        MAX(
                            CASE
                                WHEN USUARIO = 'envia_pix_prod' OR DESCRICAO LIKE '%DÉBITO%' THEN 'OUT'
                                WHEN USUARIO = 'recebe_pix_prod' OR DESCRICAO LIKE '%CRÉDITO%' THEN 'IN'
                                ELSE 'Indefinido'
                            END
                        ) AS Tipo_Operacao
                    FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)
                    WHERE NR_CONTROLE = ?
                    GROUP BY NR_CONTROLE
                ),
                TransactionAggregation_CTE AS (
                    SELECT
                        NR_CONTROLE,
                        MIN(DATAHORA) AS Primeira_Operacao,
                        MAX(DATAHORA) AS Ultima_Operacao,
                        DATEDIFF(MILLISECOND, MIN(DATAHORA), MAX(DATAHORA)) AS Intervalo_Total_MS,
                        COUNT(ID) AS Quantidade_Etapas
                    FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK)
                    WHERE NR_CONTROLE = ?
                    GROUP BY NR_CONTROLE
                )
                SELECT
                    agg.NR_CONTROLE,
                    ISNULL(ot.Tipo_Operacao, 'Indefinido') AS Tipo_Operacao,
                    agg.Primeira_Operacao,
                    agg.Ultima_Operacao,
                    agg.Intervalo_Total_MS,
                    agg.Quantidade_Etapas
                FROM TransactionAggregation_CTE agg
                LEFT JOIN OperationType_CTE ot ON agg.NR_CONTROLE = ot.NR_CONTROLE;
            """
            cursor.execute(sql_query, (nr_controle, nr_controle))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao buscar o sumário da transação: {ex}")
            return []
        finally:
            cursor.close()

    def get_performance_summary_data(self, mode: str) -> List[Dict[str, Any]]:
        """
        Busca dados agregados de transações para análise de performance.
        
        Args:
            mode (str): O modo de busca ('24h' ou '100k').
        """
        # Define a subquery de origem dos dados com base no modo
        source_data_subquery = ""
        if mode == '24h':
            source_data_subquery = "[indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK) WHERE [DATAHORA] >= DATEADD(day, -1, GETDATE())"
        elif mode == '100k':
            source_data_subquery = "(SELECT TOP 100000 * FROM [indigo_pix].[dbo].[TIXLOG] WITH (NOLOCK) ORDER BY ID DESC) AS RecentLogs"
        else:
            return []

        cursor = self._connection.cursor()
        try:
            sql_query = f"""
                WITH OperationType_CTE AS (
                    SELECT
                        NR_CONTROLE,
                        MAX(
                            CASE
                                WHEN USUARIO = 'envia_pix_prod' OR DESCRICAO LIKE '%DÉBITO%' THEN 'OUT'
                                WHEN USUARIO = 'recebe_pix_prod' OR DESCRICAO LIKE '%CRÉDITO%' THEN 'IN'
                                ELSE 'Indefinido'
                            END
                        ) AS Tipo_Operacao
                    FROM {source_data_subquery}
                    GROUP BY NR_CONTROLE
                ),
                TransactionAggregation_CTE AS (
                    SELECT
                        NR_CONTROLE,
                        DATEDIFF(MILLISECOND, MIN(DATAHORA), MAX(DATAHORA)) AS Intervalo_Total_MS
                    FROM {source_data_subquery}
                    GROUP BY NR_CONTROLE
                )
                SELECT
                    agg.NR_CONTROLE,
                    ISNULL(ot.Tipo_Operacao, 'Indefinido') AS Tipo_Operacao,
                    agg.Intervalo_Total_MS
                FROM TransactionAggregation_CTE agg
                LEFT JOIN OperationType_CTE ot ON agg.NR_CONTROLE = ot.NR_CONTROLE;
            """
            cursor.execute(sql_query)
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao buscar dados de performance: {ex}")
            return []
        finally:
            cursor.close()

# --- Repositório para a tabela MCLOG (CAD) ---
class MclogRepository:
    """
    Gerencia todas as interações com a tabela [indigo_cad].[dbo].[MCLOG].
    Focada na busca de informações gerais e estatísticas de operações.
    """
    def __init__(self, connection):
        # Inicializa o repositório com uma conexão de banco de dados ativa.
        self._connection = connection

    def _format_results(self, cursor) -> List[Dict[str, Any]]:
        """Função auxiliar para formatar resultados do banco de dados."""
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def find_by_outras_info(self, search_term: str) -> List[Dict[str, Any]]:
        """Busca um termo genérico na coluna OUTRAS_INFO."""
        if not search_term: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        cursor = self._connection.cursor()
        try:
            param_value = f"%{search_term}%"
            sql_query = "SELECT TOP (1000) * FROM [indigo_cad].[dbo].[MCLOG] WITH (NOLOCK) WHERE OUTRAS_INFO LIKE ? ORDER BY ID DESC"
            cursor.execute(sql_query, (param_value,))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao executar a busca em MCLOG: {ex}")
            return []
        finally:
            cursor.close()

    def get_operations_per_minute(self, hours_ago: int = 24) -> List[Dict[str, Any]]:
        """
        Calcula o número de operações por função e por minuto na tabela MCLOG em um período customizável.
        
        Args:
            hours_ago (int): O número de horas para olhar para trás a partir do momento atual.
        """
        cursor = self._connection.cursor()
        try:
            sql_query = """
                SELECT
                    FORMAT(DATAHORA, 'yyyy-MM-dd HH:mm') AS Minuto,
                    FUNCAO,
                    COUNT(*) AS NumeroDeOperacoes
                FROM
                    [indigo_cad].[dbo].MCLOG WITH (NOLOCK)
                WHERE DATAHORA >= DATEADD(hour, -?, GETDATE())
                GROUP BY
                    FORMAT(DATAHORA, 'yyyy-MM-dd HH:mm'),
                    FUNCAO
                ORDER BY
                    Minuto ASC,
                    FUNCAO;
            """
            cursor.execute(sql_query, (hours_ago,))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao buscar estatísticas da MCLOG: {ex}")
            return []
        finally:
            cursor.close()

    def get_latest_errors(self) -> List[Dict[str, Any]]:
        """
        Busca os últimos 1000 erros (IAE = 'E') da MCLOG nas últimas 24 horas.
        """
        cursor = self._connection.cursor()
        try:
            # A query já seleciona apenas as colunas desejadas para otimização.
            sql_query = """
                SELECT TOP (1000)
                    [ID], [USUARIO], [DATAHORA], [FUNCAO], [IAE], [OUTRAS_INFO], [CODIGO_CLIENTE]
                FROM
                    [indigo_cad].[dbo].MCLOG WITH (NOLOCK)
                WHERE
                    IAE = 'E' AND DATAHORA >= DATEADD(day, -1, GETDATE())
                ORDER BY
                    ID DESC;
            """
            cursor.execute(sql_query)
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao buscar os últimos erros da MCLOG: {ex}")
            return []
        finally:
            cursor.close()

# --- Repositório para a tabela MIX100 ---
class Mix100Repository:
    """
    Gerencia todas as interações com a tabela [indigo_pix].[dbo].[MIX100].
    Focada em buscas relacionadas a transações PIX.
    """
    def __init__(self, connection):
        # Inicializa o repositório com uma conexão de banco de dados ativa.
        self._connection = connection

    def _format_results(self, cursor) -> List[Dict[str, Any]]:
        """Função auxiliar para formatar resultados do banco de dados."""
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def find_by_nr_controle(self, nr_controle: str) -> List[Dict[str, Any]]:
        """Busca transações por um NR_CONTROLE específico."""
        if not nr_controle: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        cursor = self._connection.cursor()
        try:
            sql_query = "SELECT TOP (500) * FROM [indigo_pix].[dbo].[MIX100] WITH (NOLOCK) WHERE NR_CONTROLE = ? ORDER BY id DESC"
            cursor.execute(sql_query, (nr_controle,))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao executar a busca em MIX100: {ex}")
            return []
        finally:
            cursor.close()
            
    def find_by_endtoendiddevolucao(self, endtoendid: str) -> List[Dict[str, Any]]:
        """Busca transações de devolução por seu EndToEndId específico."""
        if not endtoendid: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        cursor = self._connection.cursor()
        try:
            sql_query = "SELECT TOP (500) * FROM [indigo_pix].[dbo].[MIX100] WITH (NOLOCK) WHERE ENDTOENDIDDEVOLUCAO = ? ORDER BY id DESC"
            cursor.execute(sql_query, (endtoendid,))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao executar a busca por EndToEndId Devolução: {ex}")
            return []
        finally:
            cursor.close()

# --- Repositório para a tabela MCLOG (CCT) ---
class MclogCctRepository:
    """
    Gerencia interações com a tabela [indigo_cct].[dbo].[MCLOG].
    Focada em buscas relacionadas a logs de transações KYT.
    """
    def __init__(self, connection):
        # Inicializa o repositório com uma conexão de banco de dados ativa.
        self._connection = connection

    def _format_results(self, cursor) -> List[Dict[str, Any]]:
        """Função auxiliar para formatar resultados do banco de dados."""
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def find_by_kyt_id(self, kyt_id: str) -> List[Dict[str, Any]]:
        """Busca logs por um ID de transação KYT na coluna OUTRAS_INFO."""
        if not kyt_id: return [] # Bifurcação: evita busca desnecessária se o input for vazio.
        cursor = self._connection.cursor()
        try:
            param_value = f"%{kyt_id}%"
            sql_query = "SELECT TOP (5000) * FROM [indigo_cct].[dbo].[MCLOG] WITH (NOLOCK) WHERE OUTRAS_INFO LIKE ? ORDER BY id DESC"
            cursor.execute(sql_query, (param_value,))
            return self._format_results(cursor)
        except pyodbc.Error as ex:
            st.error(f"Erro ao executar a busca por ID KYT: {ex}")
            return []
        finally:
            cursor.close()

# --- Funções e Interface da Aplicação Streamlit ---

def display_json_or_text(content: str):
    """
    Renderiza um conteúdo como JSON se for válido, caso contrário, exibe como texto simples.
    A função foi aprimorada para extrair JSON de strings de log (ex: 'prefixo = {...}').
    """
    if not content or not isinstance(content, str):
        # Bifurcação: Trata casos de conteúdo nulo ou que não é string.
        st.info("Conteúdo vazio ou não é texto.")
        return
    
    json_part = content
    # Bifurcação: Verifica se a string parece conter um JSON aninhado.
    if '{' in content:
        # Encontra o início do JSON e extrai a substring a partir dele.
        json_start_index = content.find('{')
        json_part = content[json_start_index:]

    try:
        # Bifurcação: Tenta o parse da parte extraída da string.
        parsed_json = json.loads(json_part)
        st.json(parsed_json, expanded=True) # Exibe expandido por padrão para clareza
    except json.JSONDecodeError:
        # Bifurcação: Se o parse falhar, exibe o conteúdo original como texto.
        st.code(content, language=None)

# --- LÓGICA DE GERENCIAMENTO DE CONEXÃO RESILIENTE ---

def init_connection():
    """
    Cria a conexão com o banco de dados e inicializa os repositórios.
    Armazena o objeto de conexão e os repositórios no st.session_state.
    """
    try:
        connection = pyodbc.connect(st.secrets["database"]["connection_string"])
        st.session_state.db_connection = connection
        st.session_state.repos = {
            "tixlog": TixlogRepository(connection),
            "mclog": MclogRepository(connection),
            "mix100": Mix100Repository(connection),
            "mclog_cct": MclogCctRepository(connection)
        }
        return st.session_state.repos
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        st.warning("Verifique se o arquivo `.streamlit/secrets.toml` está configurado corretamente.")
        # Limpa o estado para forçar uma nova tentativa na próxima interação.
        st.session_state.db_connection = None
        st.session_state.repos = None
        return None

def check_connection():
    """
    Verifica se a conexão com o banco de dados está ativa. Se não estiver,
    tenta reconectar. Retorna os repositórios se a conexão estiver OK.
    """
    # Bifurcação: Se não houver conexão no estado da sessão, cria uma nova.
    if "repos" not in st.session_state or st.session_state.repos is None:
        return init_connection()
    
    cursor = None
    try:
        # Tenta executar uma query simples e rápida para validar a conexão, usando um cursor temporário.
        cursor = st.session_state.db_connection.cursor()
        cursor.execute("SELECT 1")
        # Se a query funcionar, a conexão está boa e podemos retornar os repositórios existentes.
        return st.session_state.repos
    except (pyodbc.OperationalError, pyodbc.ProgrammingError) as e:
        # Bifurcação: Se a query falhar, a conexão caiu.
        st.warning("Conexão com o banco de dados perdida. Tentando reconectar...")
        # Tenta criar uma nova conexão.
        return init_connection()
    finally:
        # Garante que o cursor de verificação seja sempre fechado.
        if cursor:
            cursor.close()

# --- Configuração da Página Principal ---
st.set_page_config(layout="wide", page_title="Visualizador de Logs e Estatísticas")
st.title("🔎 Visualizador de Logs e Estatísticas")

# Garante que temos uma conexão válida no início de cada execução.
repos = check_connection()

# --- Navegação Principal ---
st.sidebar.header("Navegação")
app_mode = st.sidebar.radio(
    "Escolha a seção:",
    ["Busca 360º", "Estatísticas"]
)

# --- LÓGICA DA PÁGINA DE BUSCA ---
if app_mode == "Busca 360º":
    # Bifurcação: Renderiza a página de busca se selecionada.
    st.header("Busca de Logs (TIXLOG, MCLOG, MIX100)")
    st.markdown("Use o menu na barra lateral para selecionar o tipo de busca.")
    
    st.sidebar.header("Opções de Busca")
    search_type = st.sidebar.selectbox(
        "Selecione o tipo de filtro:",
        [
            "MIX100: Por NR_CONTROLE (Busca 360º)",
            "MIX100: Por EndToEndId Devolução",
            "MCLOG CCT: Por ID da Transação KYT", # Nova opção
            "TIXLOG: Por NR_CONTROLE",
            "TIXLOG: Por IDREQJDPI",
            "TIXLOG: Por Lista de NR_CONTROLE (IN)",
            "TIXLOG: Por Conteúdo no JSON (LIKE)",
            "TIXLOG: Por Origem",
            "MCLOG CAD: Busca Geral em OUTRAS_INFO",
        ],
        key="search_type"
    )

    input_container = st.container()
    
    # Placeholders para os resultados
    summary_placeholder = st.empty()
    results_placeholder_1 = st.empty()
    results_placeholder_2 = st.empty()
    results_placeholder_3 = st.empty()

    with input_container:
        # Bifurcação: Renderiza o campo de input apropriado para o tipo de busca.
        if search_type == "TIXLOG: Por Lista de NR_CONTROLE (IN)":
             input_value = st.text_area("Digite os NR_CONTROLES (um por linha):", key="list_input")
        else:
             label_map = {
                 "MIX100: Por NR_CONTROLE (Busca 360º)": "Digite o NR_CONTROLE:",
                 "MIX100: Por EndToEndId Devolução": "Digite o EndToEndId da Devolução:",
                 "MCLOG CCT: Por ID da Transação KYT": "Digite o ID da Transação KYT:",
                 "TIXLOG: Por NR_CONTROLE": "Digite o NR_CONTROLE:",
                 "TIXLOG: Por IDREQJDPI": "Digite o IDREQJDPI:",
                 "TIXLOG: Por Conteúdo no JSON (LIKE)": "Digite o texto para buscar nos JSONs:",
                 "TIXLOG: Por Origem": "Digite a Origem:",
                 "MCLOG CAD: Busca Geral em OUTRAS_INFO": "Digite o termo de busca (Conta, Documento, Chave PIX, etc.):"
             }
             label = label_map.get(search_type, "Digite o valor:")
             key = search_type.replace(":", "_").replace(" ", "_").lower()
             input_value = st.text_input(label, key=key)
        
        search_clicked = st.button("Buscar", key="search_button")

    def display_mix100_details(df):
        """Exibe métricas de status e legenda para resultados da MIX100."""
        # Bifurcação: Só exibe se a coluna de status existir e houver dados.
        if 'STATUS_MENSAGEM' in df.columns and not df.empty:
            st.write("---")
            latest_status = df['STATUS_MENSAGEM'].iloc[0]
            status_map = {'D': 'Devolvido', 'A': 'Aguardando', 'L': 'Liquidado', 'V': 'Valor Vazio/Desconhecido', 'E': 'Erro'}
            status_description = status_map.get(latest_status, f"Status Desconhecido ({latest_status})")
            
            cols = st.columns(2)
            with cols[0]: st.metric(label="Último Status da Mensagem", value=latest_status)
            with cols[1]: st.metric(label="Significado", value=status_description)
            st.caption("Legenda: D = Devolvido, A = Aguardando, L = Liquidado, E = Erro, V = Valor Vazio/Desconhecido")
        else:
            # Bifurcação: Exibe apenas a legenda se não houver dados.
            st.caption("Legenda Status: D = Devolvido, A = Aguardando, L = Liquidado, E = Erro, V = Valor Vazio/Desconhecido")

    def display_tixlog_details(df):
        """Exibe os conteúdos JSON de forma expansível para resultados da TIXLOG."""
        # Bifurcação: Só exibe a seção se a coluna JSON existir.
        if 'JSON' in df.columns:
            st.subheader("Detalhes dos JSONs (TIXLOG)")
            for _, row in df.iterrows():
                expander_title = f"ID: {row['ID']} | NR_CONTROLE: {row.get('NR_CONTROLE', 'N/A')} | ORIGEM: {row.get('ORIGEM', 'N/A')}"
                with st.expander(expander_title):
                    st.write("**JSON Enviado:**"); display_json_or_text(row.get('JSON'))
                    st.write("**JSON de Retorno:**"); display_json_or_text(row.get('JSON_RETORNO'))

    def display_mclog_cct_details(df):
        """
        Busca pela decisão final nos logs da MCLOG CCT, extrai o status e o horário,
        e exibe essas informações em destaque, além do JSON completo.
        """
        # Bifurcação: Procede apenas se a coluna OUTRAS_INFO existir e houver dados.
        if 'OUTRAS_INFO' in df.columns and not df.empty:
            decision_keywords = ['aprovado', 'rejeitado', 'approved', 'rejected', '"ALLOW"', '"DENY"']
            decision_row = None

            # Itera sobre os resultados para encontrar a linha com a decisão.
            for index, row in df.iterrows():
                outras_info_str = row.get('OUTRAS_INFO')
                if isinstance(outras_info_str, str) and any(keyword.lower() in outras_info_str.lower() for keyword in decision_keywords):
                    decision_row = row
                    break  # Para no primeiro resultado encontrado.

            # Bifurcação: Se uma linha com a decisão foi encontrada, exibe os detalhes.
            if decision_row is not None:
                st.subheader("Decisão Final (KYT)")
                decision_info = decision_row.get('OUTRAS_INFO')
                decision_time = decision_row.get('DATAHORA')
                action = "N/A"
                
                # Tenta extrair a ação de dentro do JSON
                if isinstance(decision_info, str) and '{' in decision_info:
                    try:
                        json_start_index = decision_info.find('{')
                        json_part = decision_info[json_start_index:]
                        parsed_json = json.loads(json_part)
                        action = parsed_json.get('action', 'N/A')
                    except json.JSONDecodeError:
                        action = "Erro no JSON" # Indica que o JSON está mal formatado

                # Exibe as métricas de status e horário
                col1, col2 = st.columns(2)
                col1.metric("Status da Transação", action)
                col2.metric("Horário da Decisão", f"{decision_time:%Y-%m-%d %H:%M:%S}" if decision_time else "N/A")
                
                # Exibe o JSON completo em um expander
                with st.expander("Ver JSON completo da decisão"):
                    display_json_or_text(decision_info)

    def display_results(placeholder, title, results, details_func=None):
        """
        Função genérica para renderizar um DataFrame e seus detalhes em um placeholder.
        Foi refatorada para ser mais flexível.
        """
        with placeholder.container():
            # Bifurcação: Só renderiza se houver resultados.
            if results:
                st.subheader(title)
                df = pd.DataFrame(results)
                
                # Passo 1: Exibe detalhes que devem aparecer ACIMA da tabela (ex: status da MIX100 ou decisão do KYT).
                if details_func in [display_mix100_details, display_mclog_cct_details]:
                    details_func(df)
                
                # Passo 2: Exibe a tabela principal, suprimindo colunas se necessário.
                cols_to_show = df.columns
                if details_func == display_tixlog_details:
                    cols_to_show = [col for col in df.columns if col not in ['JSON', 'JSON_RETORNO']]
                st.dataframe(df[cols_to_show])

                # Passo 3: Exibe detalhes que devem aparecer ABAIXO da tabela (ex: JSONs da TIXLOG).
                if details_func == display_tixlog_details:
                    details_func(df)

    # Ação principal: executada somente quando o botão de busca é clicado.
    if search_clicked and repos:
        summary_placeholder.empty(); results_placeholder_1.empty(); results_placeholder_2.empty(); results_placeholder_3.empty()
        
        # Bifurcação: Trata a busca 360º e a busca por NR_CONTROLE na TIXLOG, que exibem o sumário.
        if search_type == "MIX100: Por NR_CONTROLE (Busca 360º)" or search_type == "TIXLOG: Por NR_CONTROLE":
            with st.spinner("Analisando tempo da transação..."):
                summary_data = repos["tixlog"].get_transaction_summary(input_value)
            with summary_placeholder.container():
                if summary_data:
                    st.subheader("Sumário da Transação (TIXLOG)")
                    summary = summary_data[0]
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Tipo de Operação", summary.get('Tipo_Operacao', 'N/A'))
                    col2.metric("Tempo Total (ms)", f"{summary.get('Intervalo_Total_MS', 0):,}")
                    col3.metric("Quantidade de Etapas", summary.get('Quantidade_Etapas', 0))
                    st.divider()

        if search_type == "MIX100: Por NR_CONTROLE (Busca 360º)":
            with st.spinner("Buscando em MIX100..."): mix100_results = repos["mix100"].find_by_nr_controle(input_value)
            display_results(results_placeholder_1, "Resultados em MIX100", mix100_results, display_mix100_details)
            with st.spinner("Buscando em TIXLOG..."): tixlog_results = repos["tixlog"].find_by_nr_controle(input_value)
            display_results(results_placeholder_2, "Resultados Complementares em TIXLOG", tixlog_results, display_tixlog_details)
            with st.spinner("Buscando em MCLOG CAD..."): mclog_results = repos["mclog"].find_by_outras_info(input_value)
            display_results(results_placeholder_3, "Resultados Complementares em MCLOG CAD", mclog_results)
            if not mix100_results and not tixlog_results and not mclog_results: results_placeholder_1.info("Nenhum resultado encontrado.")
        
        elif search_type == "MIX100: Por EndToEndId Devolução":
            with st.spinner("Buscando em MIX100..."): mix100_results = repos["mix100"].find_by_endtoendiddevolucao(input_value)
            display_results(results_placeholder_1, "Resultados em MIX100", mix100_results, display_mix100_details)
            if not mix100_results: results_placeholder_1.info("Nenhum resultado encontrado.")

        elif search_type == "MCLOG CCT: Por ID da Transação KYT":
            with st.spinner("Buscando em MCLOG CCT..."):
                kyt_results = repos["mclog_cct"].find_by_kyt_id(input_value)
            # Passa a nova função de detalhes para a exibição dos resultados.
            display_results(results_placeholder_1, "Resultados em MCLOG CCT", kyt_results, display_mclog_cct_details)
            if not kyt_results:
                results_placeholder_1.info("Nenhum resultado encontrado.")

        elif search_type.startswith("TIXLOG"):
            primary_results, complementary_results = [], []
            with st.spinner(f"Buscando em TIXLOG..."):
                if search_type == "TIXLOG: Por NR_CONTROLE": primary_results = repos["tixlog"].find_by_nr_controle(input_value)
                elif search_type == "TIXLOG: Por IDREQJDPI": primary_results = repos["tixlog"].find_by_idreqjdpi(input_value)
                elif search_type == "TIXLOG: Por Lista de NR_CONTROLE (IN)":
                    nr_list = [line.strip() for line in input_value.split('\n') if line.strip()]
                    if nr_list: primary_results = repos["tixlog"].find_by_nr_controle_in(nr_list)
                elif search_type == "TIXLOG: Por Conteúdo no JSON (LIKE)": primary_results = repos["tixlog"].find_by_json_content(input_value)
                elif search_type == "TIXLOG: Por Origem": primary_results = repos["tixlog"].find_by_origem(input_value)
            display_results(results_placeholder_1, f"Resultados Principais em TIXLOG", primary_results, display_tixlog_details)
            with st.spinner("Buscando complemento em MCLOG CAD..."):
                if search_type == "TIXLOG: Por Lista de NR_CONTROLE (IN)":
                    nr_list = [line.strip() for line in input_value.split('\n') if line.strip()]
                    if nr_list:
                        temp_complementary = [item for sublist in [repos["mclog"].find_by_outras_info(item) for item in nr_list] for item in sublist]
                        if temp_complementary: complementary_results = pd.DataFrame(temp_complementary).drop_duplicates().to_dict('records')
                else: complementary_results = repos["mclog"].find_by_outras_info(input_value)
            display_results(results_placeholder_2, "Resultados Complementares em MCLOG CAD", complementary_results)
            if not primary_results and not complementary_results: results_placeholder_1.info("Nenhum resultado encontrado.")

        elif search_type == "MCLOG CAD: Busca Geral em OUTRAS_INFO":
            with st.spinner("Buscando em MCLOG CAD..."): mclog_results = repos["mclog"].find_by_outras_info(input_value)
            display_results(results_placeholder_1, "Resultados em MCLOG CAD", mclog_results)
            if not mclog_results: results_placeholder_1.info("Nenhum resultado encontrado.")

# --- LÓGICA DA PÁGINA DE ESTATÍSTICAS ---
elif app_mode == "Estatísticas":
    # Bifurcação: Renderiza a página de estatísticas se selecionada.
    st.header("📈 Estatísticas de Logs e Performance")
    
    st.subheader("Novas Entradas na TIXLOG")
    if st.button("Gerar Gráfico de Entradas por Minuto (Últimas 24h)"):
        # Bifurcação: Lógica para o primeiro gráfico.
        if repos:
            with st.spinner("Calculando estatísticas da TIXLOG..."):
                stats_data = repos["tixlog"].get_new_entries_per_minute()
            
            if stats_data:
                df_stats = pd.DataFrame(stats_data)
                df_stats['MinutoFormatado'] = pd.to_datetime(df_stats['MinutoFormatado'])
                df_stats = df_stats.set_index('MinutoFormatado')
                
                st.write("Gráfico de Novas Entradas por Minuto (TIXLOG)")
                st.line_chart(df_stats['NovosNrControlePorMinuto'])
                
                with st.expander("Ver dados brutos"):
                    st.dataframe(df_stats)
            else:
                st.info("Nenhum dado estatístico encontrado na TIXLOG no período.")
        else:
            st.error("Conexão com o banco de dados não estabelecida.")

    st.divider()

    st.subheader("Operações por Função na MCLOG")
    
    # Controles de filtro para o gráfico da MCLOG
    col1_ops, col2_ops = st.columns([1, 3])
    with col1_ops:
        time_options = {"Última 1 hora": 1, "Últimas 6 horas": 6, "Últimas 24 horas": 24}
        selected_time_label = st.selectbox("Selecione o Período:", options=time_options.keys(), key="mclog_time")
        selected_hours = time_options[selected_time_label]

    filter_placeholder = st.empty()
    
    if st.button("Gerar Gráfico de Operações"):
        # Bifurcação: Lógica para o segundo gráfico e a tabela de erros.
        if repos:
            with st.spinner(f"Calculando estatísticas da MCLOG ({selected_time_label})..."):
                ops_data = repos["mclog"].get_operations_per_minute(hours_ago=selected_hours)
            
            if ops_data:
                df_ops = pd.DataFrame(ops_data)
                all_functions = sorted(df_ops['FUNCAO'].unique())
                with filter_placeholder:
                    selected_functions = st.multiselect("Filtre as Funções:", options=all_functions, default=all_functions)
                
                if selected_functions:
                    df_ops_filtered = df_ops[df_ops['FUNCAO'].isin(selected_functions)]
                    pivot_df = df_ops_filtered.pivot(index='Minuto', columns='FUNCAO', values='NumeroDeOperacoes').fillna(0)
                    st.write(f"Gráfico de Operações por Função por Minuto (MCLOG) - {selected_time_label}")
                    st.bar_chart(pivot_df)
                else:
                    st.warning("Selecione ao menos uma função para exibir o gráfico.")
                
                with st.expander("Ver dados brutos de operações"):
                    st.dataframe(df_ops)
            else:
                st.info(f"Nenhuma operação encontrada na MCLOG no período de {selected_time_label}.")

            st.write("---")
            with st.spinner("Buscando últimos erros na MCLOG..."):
                error_data = repos["mclog"].get_latest_errors()
            
            if error_data:
                st.subheader("Últimos Erros Registrados na MCLOG (IAE = 'E')")
                df_errors = pd.DataFrame(error_data)
                st.dataframe(df_errors)
            else:
                st.info("Nenhum erro (IAE = 'E') encontrado na MCLOG nas últimas 24 horas.")
        else:
            st.error("Conexão com o banco de dados não estabelecida.")

    st.divider()

    # Seção de Análise de Performance
    st.subheader("Análise de Performance de Transações (TIXLOG)")
    perf_mode_options = {"Últimas 24 horas": "24h", "Últimas 100.000 Transações": "100k"}
    selected_perf_mode_label = st.selectbox("Selecione o Conjunto de Dados:", options=perf_mode_options.keys())
    selected_perf_mode = perf_mode_options[selected_perf_mode_label]

    if st.button("Analisar Performance"):
        if repos:
            with st.spinner(f"Buscando e analisando dados de performance ({selected_perf_mode_label})..."):
                perf_data = repos["tixlog"].get_performance_summary_data(mode=selected_perf_mode)
            
            if perf_data:
                df_perf = pd.DataFrame(perf_data)
                df_in = df_perf[df_perf['Tipo_Operacao'] == 'IN']
                df_out = df_perf[df_perf['Tipo_Operacao'] == 'OUT']
                
                st.write("---")
                col_in, col_out = st.columns(2)

                with col_in:
                    st.subheader("Operações de Entrada (IN)")
                    if not df_in.empty:
                        st.metric("Tempo Médio (ms)", f"{df_in['Intervalo_Total_MS'].mean():.2f}")
                        st.metric("Mediana (ms)", f"{df_in['Intervalo_Total_MS'].median():.2f}")
                        st.metric("P95 (ms)", f"{df_in['Intervalo_Total_MS'].quantile(0.95):.2f}")
                        st.metric("Total de Transações", f"{len(df_in):,}")
                    else:
                        st.info("Nenhuma operação 'IN' encontrada no período.")

                with col_out:
                    st.subheader("Operações de Saída (OUT)")
                    if not df_out.empty:
                        st.metric("Tempo Médio (ms)", f"{df_out['Intervalo_Total_MS'].mean():.2f}")
                        st.metric("Mediana (ms)", f"{df_out['Intervalo_Total_MS'].median():.2f}")
                        st.metric("P95 (ms)", f"{df_out['Intervalo_Total_MS'].quantile(0.95):.2f}")
                        st.metric("Total de Transações", f"{len(df_out):,}")
                    else:
                        st.info("Nenhuma operação 'OUT' encontrada no período.")
                
                with st.expander("Ver dados brutos da análise"):
                    st.dataframe(df_perf)
            else:
                st.warning("Não foi possível obter dados para a análise de performance.")
        else:
            st.error("Conexão com o banco de dados não estabelecida.")
