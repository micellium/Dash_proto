# Visualizador de Logs e Estatísticas

Um aplicativo Streamlit para visualizar logs e estatísticas de um banco de dados.

## Descrição

Esta aplicação fornece uma interface de usuário para consultar e visualizar dados de logs de transações de um banco de dados. Ela permite que os usuários realizem buscas detalhadas em várias tabelas e visualizem estatísticas de performance em tempo real.

## Funcionalidades

- **Busca 360º**: Pesquise por um `NR_CONTROLE` em múltiplas tabelas (`MIX100`, `TIXLOG`, `MCLOG CAD`).
- **Buscas Específicas**:
    - `MIX100`: Por `EndToEndId Devolução`.
    - `MCLOG CCT`: Por `ID da Transação KYT`.
    - `TIXLOG`: Por `NR_CONTROLE`, `IDREQJDPI`, lista de `NR_CONTROLE`, conteúdo em `JSON`, e `Origem`.
    - `MCLOG CAD`: Busca geral em `OUTRAS_INFO`.
- **Página de Estatísticas**:
    - Gráfico de novas entradas por minuto na `TIXLOG`.
    - Gráfico de operações por função na `MCLOG`.
    - Visualização dos últimos erros registrados na `MCLOG`.
    - Análise de performance de transações na `TIXLOG`.

## Instalação

1. **Clone o repositório:**
   ```bash
   git clone <url-do-repositorio>
   cd <diretorio-do-projeto>
   ```

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure as credenciais do banco de dados:**
   - Crie uma pasta `.streamlit` na raiz do projeto.
   - Dentro dela, crie um arquivo chamado `secrets.toml`.
   - Adicione a string de conexão do seu banco de dados ao arquivo, como no exemplo abaixo:
     ```toml
     [database]
     connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=seu_servidor;DATABASE=seu_banco;UID=seu_usuario;PWD=sua_senha"
     ```

## Uso

Para iniciar a aplicação, execute o seguinte comando no seu terminal:

```bash
streamlit run dash_proto.py
```
