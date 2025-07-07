#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

void inserirTupla(PGconn *conn);
void removerTupla(PGconn *conn);
void listarTuplas(PGconn *conn);
void consultaJoin(PGconn *conn);
void consultaSubAgregada(PGconn *conn);
void criarTabelas(PGconn *conn);
int lerInteiro(const char *prompt);
void lerString(const char *prompt, char *buffer, int tamanho);
void lerData(const char *prompt, char *buffer);
void lerDecimal(const char *prompt, double *valor);

// Funções auxiliares para libpq
void checkExecResult(PGresult *res, PGconn *conn, const char *action);
void clearResult(PGresult *res);

int main() {
    PGconn *conn;

    // ADAPTE A SENHA E AS INFORMAÇÕES DO SEU BANCO DE DADOS POSTGRESQL!
    const char *conninfo = "dbname=Academia user=postgres password=udesc host=localhost port=5432";

    conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Erro ao conectar ao banco de dados: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    } else {
        printf("Conexao ao banco de dados PostgreSQL estabelecida com sucesso!\n");
    }

    criarTabelas(conn);

    int opcao;
    do {
        printf("\n=== MENU PRINCIPAL ===\n");
        printf("1 - Inserir nova tupla\n");
        printf("2 - Remover tupla\n");
        printf("3 - Listar todas as tuplas\n");
        printf("4 - Consulta com juncao (Membros com mais de um email)\n");
        printf("5 - Consulta com subconsulta e agregacao (Membros com total de pagamentos acima da media)\n");
        printf("0 - Sair\n");
        printf("Escolha uma opcao: ");
        scanf("%d", &opcao);
        getchar();

        switch (opcao) {
            case 1: inserirTupla(conn); break;
            case 2: removerTupla(conn); break;
            case 3: listarTuplas(conn); break;
            case 4: consultaJoin(conn); break;
            case 5: consultaSubAgregada(conn); break;
            case 0: printf("Encerrando...\n"); break;
            default: printf("Opcao invalida!\n");
        }
    } while (opcao != 0);

    PQfinish(conn);
    return 0;
}

// Funções Auxiliares da libpq
void checkExecResult(PGresult *res, PGconn *conn, const char *action) {
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Erro ao %s: %s\n", action, PQerrorMessage(conn));
        clearResult(res);
    }
}

void clearResult(PGresult *res) {
    if (res != NULL) {
        PQclear(res);
    }
}

void inserirTupla(PGconn *conn) {
    printf("\n=== INSERIR TUPLA ===\n");
    printf("1 - Membros\n");
    printf("2 - Emails\n");
    printf("3 - Frequencias\n");
    printf("4 - Planos\n");
    printf("5 - Assinatura\n");
    printf("6 - Pagamentos\n");
    printf("Escolha a tabela: ");

    int tabela;
    scanf("%d", &tabela);
    getchar();

    char sql[1000];
    PGresult *res;

    switch (tabela) {
        case 1: { // Membros
            char nome[100], cpf[13], telefone[20], endereco[200], dtNasc_input[20];
            char dtNasc_pg[13];
            fflush(stdin);
            lerString("Nome: ", nome, sizeof(nome));
            fflush(stdin);
            lerString("CPF (somente numeros): ", cpf, sizeof(cpf));
            fflush(stdin);
            lerData("Data de nascimento (DD/MM/AAAA): ", dtNasc_input);

            int dia, mes, ano;
            char temp_date_formatted[11];

            if (sscanf(dtNasc_input, "%d/%d/%d", &dia, &mes, &ano) == 3) {
                // Formata a data 'YYYY-MM-DD' para o buffer temporário
                snprintf(temp_date_formatted, sizeof(temp_date_formatted), "%04d-%02d-%02d", ano, mes, dia);
                // Envolve a data formatada em aspas simples para o SQL
                snprintf(dtNasc_pg, sizeof(dtNasc_pg), "'%s'", temp_date_formatted);
            } else {
                printf("Formato de data invalido. Usando data padrao (NULL).\n");
                strcpy(dtNasc_pg, "NULL");
            }
            fflush(stdin);
            lerString("Telefone: ", telefone, sizeof(telefone));
            fflush(stdin);
            lerString("Endereco: ", endereco, sizeof(endereco));

            snprintf(sql, sizeof(sql), "INSERT INTO Membros (Nome, CPF, dtnasc, Telefone, Endereco) "
                                       "VALUES ('%s', '%s', %s, '%s', '%s')", nome, cpf, dtNasc_pg, telefone, endereco);
            break;
        }
        case 2: { // Emails
            int idMembro;
            char email[100];

            idMembro = lerInteiro("ID do Membro: ");
            lerString("Email: ", email, sizeof(email));

            snprintf(sql, sizeof(sql), "INSERT INTO Emails (idMembro, Email) VALUES (%d, '%s')", idMembro, email);
            break;
        }
        case 3: { // Frequencias
            int idMembro;
            char entrada_input[30], saida_input[30];
            char dtHoraEntrada_pg[22], dtHoraSaida_pg[22];

            idMembro = lerInteiro("ID do Membro: ");
            lerData("Data/Hora Entrada (DD/MM/AAAA HH:MM): ", entrada_input);
            int dia_e, mes_e, ano_e, hora_e, min_e;
            char temp_dtHoraEntrada_formatted[20];

            if (sscanf(entrada_input, "%d/%d/%d %d:%d", &dia_e, &mes_e, &ano_e, &hora_e, &min_e) == 5) {
                snprintf(temp_dtHoraEntrada_formatted, sizeof(temp_dtHoraEntrada_formatted), "%04d-%02d-%02d %02d:%02d:00", ano_e, mes_e, dia_e, hora_e, min_e);
                snprintf(dtHoraEntrada_pg, sizeof(dtHoraEntrada_pg), "'%s'", temp_dtHoraEntrada_formatted);
            } else {
                printf("Formato de data/hora de entrada invalido. Usando data/hora padrao (NULL).\n");
                strcpy(dtHoraEntrada_pg, "NULL");
            }

            lerData("Data/Hora Saida (DD/MM/AAAA HH:MM): ", saida_input);
            int dia_s, mes_s, ano_s, hora_s, min_s;
            char temp_dtHoraSaida_formatted[20];

            if (sscanf(saida_input, "%d/%d/%d %d:%d", &dia_s, &mes_s, &ano_s, &hora_s, &min_s) == 5) {
                snprintf(temp_dtHoraSaida_formatted, sizeof(temp_dtHoraSaida_formatted), "%04d-%02d-%02d %02d:%02d:00", ano_s, mes_s, dia_s, hora_s, min_s);
                snprintf(dtHoraSaida_pg, sizeof(dtHoraSaida_pg), "'%s'", temp_dtHoraSaida_formatted);
            } else {
                printf("Formato de data/hora de saida invalido. Usando data/hora padrao (NULL).\n");
                strcpy(dtHoraSaida_pg, "NULL");
            }

            snprintf(sql, sizeof(sql), "INSERT INTO Frequencias (idMembro, dtHoraEntrada, dtHoraSaida) "
                                       "VALUES (%d, %s, %s)", idMembro, dtHoraEntrada_pg, dtHoraSaida_pg);
            break;
        }
        case 4: { // Planos
            int duracao;
            double preco;

            duracao = lerInteiro("Duracao (meses): ");
            lerDecimal("Preco: ", &preco);

            snprintf(sql, sizeof(sql), "INSERT INTO Planos (Duracao, Preco) VALUES (%d, %.2f)", duracao, preco);
            break;
        }
        case 5: { // Assinatura
            int idMembro, idPlano;
            char dtInicio_input[20], dtExpira_input[20];
            char dtInicio_pg[13], dtExpira_pg[13];

            idMembro = lerInteiro("ID do Membro: ");
            idPlano = lerInteiro("ID do Plano: ");

            lerData("Data de Inicio (DD/MM/AAAA): ", dtInicio_input);
            int dia_i, mes_i, ano_i;
            char temp_dtInicio_formatted[11];

            if (sscanf(dtInicio_input, "%d/%d/%d", &dia_i, &mes_i, &ano_i) == 3) {
                snprintf(temp_dtInicio_formatted, sizeof(temp_dtInicio_formatted), "%04d-%02d-%02d", ano_i, mes_i, dia_i);
                snprintf(dtInicio_pg, sizeof(dtInicio_pg), "'%s'", temp_dtInicio_formatted);
            } else {
                printf("Formato de data de inicio invalido. Usando data padrao (NULL).\n");
                strcpy(dtInicio_pg, "NULL");
            }

            lerData("Data de Expiracao (DD/MM/AAAA): ", dtExpira_input);
            int dia_e, mes_e, ano_e;
            char temp_dtExpira_formatted[11];

            if (sscanf(dtExpira_input, "%d/%d/%d", &dia_e, &mes_e, &ano_e) == 3) {
                snprintf(temp_dtExpira_formatted, sizeof(temp_dtExpira_formatted), "%04d-%02d-%02d", ano_e, mes_e, dia_e);
                snprintf(dtExpira_pg, sizeof(dtExpira_pg), "'%s'", temp_dtExpira_formatted);
            } else {
                printf("Formato de data de expiracao invalido. Usando data padrao (NULL).\n");
                strcpy(dtExpira_pg, "NULL");
            }

            snprintf(sql, sizeof(sql), "INSERT INTO Assinatura (idMembro, idPlano, dtInicio, dtExpira) "
                                       "VALUES (%d, %d, %s, %s)", idMembro, idPlano, dtInicio_pg, dtExpira_pg);
            break;
        }
        case 6: { // Pagamentos
            int idMembro, idPlano;
            char metodo[50], dtPagamento_input[20], status[10];
            char dtPagamento_pg[13];
            double valor;

            lerString("Metodo de pagamento (cartao, boleto, etc.): ", metodo, sizeof(metodo));
            lerDecimal("Valor: ", &valor);
            lerData("Data de Pagamento (DD/MM/AAAA): ", dtPagamento_input);
            int dia_p, mes_p, ano_p;
            char temp_dtPagamento_formatted[11];

            if (sscanf(dtPagamento_input, "%d/%d/%d", &dia_p, &mes_p, &ano_p) == 3) {
                snprintf(temp_dtPagamento_formatted, sizeof(temp_dtPagamento_formatted), "%04d-%02d-%02d", ano_p, mes_p, dia_p);
                snprintf(dtPagamento_pg, sizeof(dtPagamento_pg), "'%s'", temp_dtPagamento_formatted);
            } else {
                printf("Formato de data de pagamento invalido. Usando data padrao (NULL).\n");
                strcpy(dtPagamento_pg, "NULL");
            }

            lerString("Status (pago ou pendente): ", status, sizeof(status));
            idMembro = lerInteiro("ID do Membro: ");
            idPlano = lerInteiro("ID do Plano: ");

            snprintf(sql, sizeof(sql), "INSERT INTO Pagamentos (Metodo, Valor, dtPagamento, Status, idMembro, idPlano) "
                                       "VALUES ('%s', %.2f, %s, '%s', %d, %d)",
                                       metodo, valor, dtPagamento_pg, status, idMembro, idPlano);
            break;
        }
        default:
            printf("Tabela invalida!\n");
            return;
    }

    res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Erro ao inserir: %s\n", PQerrorMessage(conn));
    } else {
        printf("Dados inseridos com sucesso!\n");
    }
    clearResult(res);
}

// Funções de Leitura de Entrada
int lerInteiro(const char *prompt) {
    int valor;
    printf("%s", prompt);
    scanf("%d", &valor);
    getchar();
    return valor;
}

void lerString(const char *prompt, char *buffer, int tamanho) {
    printf("%s", prompt);
    fgets(buffer, tamanho, stdin);
    buffer[strcspn(buffer, "\n")] = '\0'; // Remove o '\n'
}

void lerData(const char *prompt, char *buffer) {
    printf("%s", prompt);
    fgets(buffer, 20, stdin);
    buffer[strcspn(buffer, "\n")] = '\0';
}

void lerDecimal(const char *prompt, double *valor) {
    printf("%s", prompt);
    scanf("%lf", valor);
    getchar();
}

// Funções de Manipulação de Tabelas e Consultas
void criarTabelas(PGconn *conn) {
    PGresult *res;

    const char *sql_membros =
        "CREATE TABLE IF NOT EXISTS Membros ("
        "   idMembro SERIAL PRIMARY KEY,"
        "   Nome VARCHAR(100) NOT NULL,"
        "   CPF VARCHAR(11) UNIQUE NOT NULL,"
        "   dtnasc DATE,"
        "   Telefone VARCHAR(20),"
        "   Endereco VARCHAR(200)"
        ");";

    const char *sql_emails =
        "CREATE TABLE IF NOT EXISTS Emails ("
        "   codEmail SERIAL PRIMARY KEY,"
        "   Email VARCHAR(100) NOT NULL,"
        "   idMembro INTEGER NOT NULL,"
        "   FOREIGN KEY (idMembro) REFERENCES Membros(idMembro) ON DELETE CASCADE"
        ");";

    const char *sql_frequencias =
        "CREATE TABLE IF NOT EXISTS Frequencias ("
        "   idFrequencia SERIAL PRIMARY KEY,"
        "   dtHoraEntrada TIMESTAMP NOT NULL,"
        "   dtHoraSaida TIMESTAMP,"
        "   idMembro INTEGER NOT NULL,"
        "   FOREIGN KEY (idMembro) REFERENCES Membros(idMembro) ON DELETE CASCADE"
        ");";

    const char *sql_planos =
        "CREATE TABLE IF NOT EXISTS Planos ("
        "   idPlano SERIAL PRIMARY KEY,"
        "   Duracao INTEGER NOT NULL,"
        "   Preco DECIMAL(10,2) NOT NULL"
        ");";

    const char *sql_assinatura =
        "CREATE TABLE IF NOT EXISTS Assinatura ("
        "   idMembro INTEGER NOT NULL,"
        "   idPlano INTEGER NOT NULL,"
        "   dtInicio DATE NOT NULL,"
        "   dtExpira DATE,"
        "   PRIMARY KEY (idMembro, idPlano, dtInicio),"
        "   FOREIGN KEY (idMembro) REFERENCES Membros(idMembro) ON DELETE CASCADE,"
        "   FOREIGN KEY (idPlano) REFERENCES Planos(idPlano) ON DELETE CASCADE"
        ");";

    const char *sql_pagamentos =
        "CREATE TABLE IF NOT EXISTS Pagamentos ("
        "   idPagamento SERIAL PRIMARY KEY,"
        "   Metodo VARCHAR(50) NOT NULL,"
        "   Valor DECIMAL(10,2) NOT NULL,"
        "   dtPagamento DATE NOT NULL,"
        "   Status VARCHAR(10) NOT NULL,"
        "   idMembro INTEGER NOT NULL,"
        "   idPlano INTEGER NOT NULL"
        ");";

    res = PQexec(conn, sql_membros);
    checkExecResult(res, conn, "criar tabela Membros");
    clearResult(res);

    res = PQexec(conn, sql_emails);
    checkExecResult(res, conn, "criar tabela Emails");
    clearResult(res);

    res = PQexec(conn, sql_frequencias);
    checkExecResult(res, conn, "criar tabela Frequencias");
    clearResult(res);

    res = PQexec(conn, sql_planos);
    checkExecResult(res, conn, "criar tabela Planos");
    clearResult(res);

    res = PQexec(conn, sql_assinatura);
    checkExecResult(res, conn, "criar tabela Assinatura");
    clearResult(res);

    res = PQexec(conn, sql_pagamentos);
    checkExecResult(res, conn, "criar tabela Pagamentos");
    clearResult(res);

    printf("Tabelas criadas com sucesso (ou ja existiam)!\n");
}

void removerTupla(PGconn *conn) {
    printf("\n=== REMOVER TUPLA ===\n");
    printf("1 - Membros\n");
    printf("2 - Emails\n");
    printf("3 - Frequencias\n");
    printf("4 - Planos\n");
    printf("5 - Assinatura\n");
    printf("6 - Pagamentos\n");
    printf("Escolha a tabela: ");

    int tabela;
    scanf("%d", &tabela);
    getchar();

    char sql[1000];
    PGresult *res;
    int id;

    switch (tabela) {
        case 1:
            id = lerInteiro("ID do Membro a remover: ");
            snprintf(sql, sizeof(sql), "DELETE FROM Membros WHERE idMembro = %d", id);
            break;

        case 2:
            id = lerInteiro("ID do Email a remover: ");
            snprintf(sql, sizeof(sql), "DELETE FROM Emails WHERE codEmail = %d", id);
            break;

        case 3:
            id = lerInteiro("ID da Frequencia a remover: ");
            snprintf(sql, sizeof(sql), "DELETE FROM Frequencias WHERE idFrequencia = %d", id);
            break;

        case 4:
            id = lerInteiro("ID do Plano a remover: ");
            snprintf(sql, sizeof(sql), "DELETE FROM Planos WHERE idPlano = %d", id);
            break;

        case 5: {
            int idMembro, idPlano;
            char dtInicio_input[20];
            char dtInicio_pg[13];
            char temp_dtInicio_formatted[11];

            idMembro = lerInteiro("ID do Membro: ");
            idPlano = lerInteiro("ID do Plano: ");
            lerData("Data de Inicio da Assinatura (DD/MM/AAAA): ", dtInicio_input);

            int dia, mes, ano;
            if (sscanf(dtInicio_input, "%d/%d/%d", &dia, &mes, &ano) == 3) {
                snprintf(temp_dtInicio_formatted, sizeof(temp_dtInicio_formatted), "%04d-%02d-%02d", ano, mes, dia);
                snprintf(dtInicio_pg, sizeof(dtInicio_pg), "'%s'", temp_dtInicio_formatted);
            } else {
                printf("Formato de data invalido. Nao sera possivel remover sem data valida.\n");
                return;
            }

            snprintf(sql, sizeof(sql), "DELETE FROM Assinatura WHERE idMembro = %d AND idPlano = %d AND dtInicio = %s",
                     idMembro, idPlano, dtInicio_pg);
            break;
        }

        case 6:
            id = lerInteiro("ID do Pagamento a remover: ");
            snprintf(sql, sizeof(sql), "DELETE FROM Pagamentos WHERE idPagamento = %d", id);
            break;

        default:
            printf("Tabela invalida!\n");
            return;
    }

    res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Erro ao remover: %s\n", PQerrorMessage(conn));
    } else {
        int rows_affected = atoi(PQcmdTuples(res));
        if (rows_affected == 0) {
            printf("Aviso: Nenhum registro foi encontrado com os criterios informados.\n");
        } else {
            printf("%d registro(s) removido(s).\n", rows_affected);
        }
    }
    clearResult(res);
}

void listarTuplas(PGconn *conn) {
    printf("\n=== LISTAR TUPLAS ===\n");
    printf("1 - Membros\n");
    printf("2 - Emails\n");
    printf("3 - Frequencias\n");
    printf("4 - Planos\n");
    printf("5 - Assinatura\n");
    printf("6 - Pagamentos\n");
    printf("Escolha a tabela: ");

    int tabela;
    scanf("%d", &tabela);
    getchar();

    const char *sql;
    PGresult *res;
    int i, num_rows;

    switch (tabela) {
        case 1:
            sql = "SELECT idMembro, Nome, CPF, TO_CHAR(dtnasc, 'DD/MM/YYYY'), Telefone, Endereco FROM Membros ORDER BY idMembro";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Membros");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== MEMBROS ===\n");
            printf("%-5s %-20s %-15s %-12s %-15s %-30s\n",
                   "ID", "Nome", "CPF", "Nascimento", "Telefone", "Endereco");
            printf("----------------------------------------------------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-5s %-20s %-15s %-12s %-15s %-30s\n",
                       PQgetvalue(res, i, 0), //(resultadoBusca, nºlinha, nºcoluna)
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2),
                       PQgetvalue(res, i, 3),
                       PQgetvalue(res, i, 4),
                       PQgetvalue(res, i, 5));
            }
            break;

        case 2:
            sql = "SELECT codEmail, Email, idMembro FROM Emails ORDER BY codEmail";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Emails");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== EMAILS ===\n");
            printf("%-8s %-30s %-10s\n", "ID", "Email", "ID Membro");
            printf("------------------------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-8s %-30s %-10s\n",
                       PQgetvalue(res, i, 0),
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2));
            }
            break;

        case 3:
            sql = "SELECT idFrequencia, TO_CHAR(dtHoraEntrada, 'DD/MM/YYYY HH24:MI:SS'), TO_CHAR(dtHoraSaida, 'DD/MM/YYYY HH24:MI:SS'), idMembro FROM Frequencias ORDER BY idFrequencia";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Frequencias");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== FREQUENCIAS ===\n");
            printf("%-5s %-20s %-20s %-10s\n", "ID", "Entrada", "Saida", "ID Membro");
            printf("--------------------------------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-5s %-20s %-20s %-10s\n",
                       PQgetvalue(res, i, 0),
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2),
                       PQgetvalue(res, i, 3));
            }
            break;

        case 4:
            sql = "SELECT idPlano, Duracao, Preco FROM Planos ORDER BY idPlano";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Planos");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== PLANOS ===\n");
            printf("%-5s %-15s %-10s\n", "ID", "Duracao", "Preco");
            printf("----------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-5s %-15s R$ %-8s\n",
                       PQgetvalue(res, i, 0),
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2));
            }
            break;

        case 5:
            sql = "SELECT idMembro, idPlano, TO_CHAR(dtInicio, 'DD/MM/YYYY'), TO_CHAR(dtExpira, 'DD/MM/YYYY') FROM Assinatura ORDER BY idMembro, idPlano, dtInicio";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Assinaturas");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== ASSINATURAS ===\n");
            printf("%-10s %-10s %-12s %-12s\n", "ID Membro", "ID Plano", "Inicio", "Expira");
            printf("------------------------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-10s %-10s %-12s %-12s\n",
                       PQgetvalue(res, i, 0),
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2),
                       PQgetvalue(res, i, 3));
            }
            break;

        case 6:
            sql = "SELECT idPagamento, Metodo, Valor, TO_CHAR(dtPagamento, 'DD/MM/YYYY'), Status, idMembro, idPlano FROM Pagamentos ORDER BY idPagamento";
            res = PQexec(conn, sql);
            checkExecResult(res, conn, "listar Pagamentos");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                clearResult(res);
                return;
            }

            printf("\n=== PAGAMENTOS ===\n");
            printf("%-5s %-15s %-10s %-12s %-10s %-10s %-10s\n",
                   "ID", "Metodo", "Valor", "Data", "Status", "ID Membro", "ID Plano");
            printf("----------------------------------------------------------------------------\n");

            num_rows = PQntuples(res);
            for (i = 0; i < num_rows; i++) {
                printf("%-5s %-15s R$ %-7s %-12s %-10s %-10s %-10s\n",
                       PQgetvalue(res, i, 0),
                       PQgetvalue(res, i, 1),
                       PQgetvalue(res, i, 2),
                       PQgetvalue(res, i, 3),
                       PQgetvalue(res, i, 4),
                       PQgetvalue(res, i, 5),
                       PQgetvalue(res, i, 6));
            }
            break;

        default:
            printf("Tabela invalida!\n");
            clearResult(res);
            return;
    }

    printf("\nTotal de registros: %d\n", num_rows);
    clearResult(res);
}

void consultaJoin(PGconn *conn) {
    printf("\n=== CONSULTA COM JUNCAO ===\n");
    printf("Consultando membros com mais de um email cadastrado\n");
    const char *sql;
    PGresult *res;
    int i, num_rows;

    sql = "SELECT M.idMembro, M.Nome, COUNT(E.codEmail) AS total_emails "
          "FROM Membros M "
          "JOIN Emails E ON M.idMembro = E.idMembro "
          "GROUP BY M.idMembro, M.Nome "
          "HAVING COUNT(E.codEmail) > 1 "
          "ORDER BY total_emails DESC";

    res = PQexec(conn, sql);
    checkExecResult(res, conn, "executar consulta de juncao");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        clearResult(res);
        return;
    }

    printf("\n=== MEMBROS COM MAIS DE UM EMAIL ===\n");
    printf("%-8s %-30s %-12s\n", "ID", "Nome", "Total Emails");
    printf("--------------------------------------------\n");

    num_rows = PQntuples(res);
    for (i = 0; i < num_rows; i++) {
        printf("%-8s %-30s %-12s\n",
               PQgetvalue(res, i, 0),
               PQgetvalue(res, i, 1),
               PQgetvalue(res, i, 2));
    }
    clearResult(res);
}

void consultaSubAgregada(PGconn *conn) {
    printf("\n=== CONSULTA COM SUBCONSULTA E AGREGACAO ===\n");
    printf("Membros com total de pagamentos acima da media\n");

    const char *sql;
    PGresult *res;
    int i, num_rows;

    sql = "SELECT M.idMembro, M.Nome, SUM(P.Valor) AS total_pagamentos "
          "FROM Membros M "
          "JOIN Pagamentos P ON M.idMembro = P.idMembro "
          "GROUP BY M.idMembro, M.Nome "
          "HAVING SUM(P.Valor) > (SELECT AVG(Valor) FROM Pagamentos)";

    res = PQexec(conn, sql);
    checkExecResult(res, conn, "executar consulta subAgregada");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        clearResult(res);
        return;
    }

    printf("\n=== MEMBROS COM PAGAMENTOS ACIMA DA MEDIA ===\n");
    printf("%-8s %-30s %-15s\n", "ID", "Nome", "Total Pagamentos");
    printf("--------------------------------------------\n");

    num_rows = PQntuples(res);
    for (i = 0; i < num_rows; i++) {
        printf("%-8s %-30s R$ %-12s\n",
               PQgetvalue(res, i, 0),
               PQgetvalue(res, i, 1),
               PQgetvalue(res, i, 2));
    }
    clearResult(res);
}
