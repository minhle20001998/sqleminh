package sql.parser;

import sql.schema.Column;
import sql.schema.Schema;
import sql.schema.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SqlParser {
    private static final Set<String> RESERVED_KEYWORDS = Set.of(
            "CREATE",
            "TABLE",
            "INSERT",
            "INTO",
            "VALUES",
            "SELECT",
            "FROM",
            "WHERE",
            "NULL",
            "NOT",
            "INT",
            "TEXT"
    );

    private Tokenizer tokenizer;

    public SqlStatement parse(String sql) {
        this.tokenizer = new Tokenizer(sql);

        SqlStatement statement;

        if (matchKeyword("CREATE")) {
            statement = parseCreateTable();
        } else if (matchKeyword("INSERT")) {
            statement = parseInsert();
        } else if (matchKeyword("SELECT")) {
            statement = parseSelect();
        } else {
            throw error("Expected CREATE, INSERT, or SELECT");
        }

        matchSymbol(";");
        expect(TokenType.EOF, "Expected end of SQL");

        return statement;
    }

    private CreateTableStatement parseCreateTable() {
        expectKeyword("TABLE");
        String tableName = expectIdentifier("Expected table name");
        expectSymbol("(");

        List<Column> columns = new ArrayList<>();

        do {
            String columnName = expectIdentifier("Expected column name");
            SqlType type = SqlType.fromName(expectTypeName("Expected column type"));
            boolean nullable = true;

            if (matchKeyword("NOT")) {
                expectKeyword("NULL");
                nullable = false;
            } else if (matchKeyword("NULL")) {
                nullable = true;
            }

            columns.add(new Column(columnName, type, nullable));
        } while (matchSymbol(","));

        expectSymbol(")");

        return new CreateTableStatement(tableName, new Schema(columns));
    }

    private InsertStatement parseInsert() {
        expectKeyword("INTO");
        String tableName = expectIdentifier("Expected table name");
        List<String> columnNames = new ArrayList<>();

        if (matchSymbol("(")) {
            do {
                columnNames.add(expectIdentifier("Expected insert column name"));
            } while (matchSymbol(","));

            expectSymbol(")");
        }

        expectKeyword("VALUES");
        expectSymbol("(");

        List<LiteralValue> values = new ArrayList<>();

        do {
            values.add(parseLiteral());
        } while (matchSymbol(","));

        expectSymbol(")");

        return new InsertStatement(tableName, columnNames, values);
    }

    private SelectStatement parseSelect() {
        boolean selectAll = false;
        List<String> columnNames = new ArrayList<>();

        if (matchSymbol("*")) {
            selectAll = true;
        } else {
            do {
                columnNames.add(expectIdentifier("Expected selected column name"));
            } while (matchSymbol(","));
        }

        expectKeyword("FROM");
        String tableName = expectIdentifier("Expected table name");

        ComparisonExpression where = null;
        if (matchKeyword("WHERE")) {
            String columnName = expectIdentifier("Expected WHERE column name");
            expectSymbol("=");
            where = new ComparisonExpression(columnName, ComparisonOperator.EQUALS, parseLiteral());
        }

        return new SelectStatement(selectAll, columnNames, tableName, where);
    }

    private LiteralValue parseLiteral() {
        Token token = tokenizer.peek();

        if (token.type == TokenType.NUMBER) {
            tokenizer.next();
            return LiteralValue.intLiteral(Integer.parseInt(token.text));
        }

        if (token.type == TokenType.STRING) {
            tokenizer.next();
            return LiteralValue.textLiteral(token.text);
        }

        if (matchKeyword("NULL")) {
            return LiteralValue.nullLiteral();
        }

        throw error("Expected literal value");
    }

    private boolean matchKeyword(String keyword) {
        Token token = tokenizer.peek();

        if (token.type == TokenType.IDENTIFIER && token.text.equalsIgnoreCase(keyword)) {
            tokenizer.next();
            return true;
        }

        return false;
    }

    private void expectKeyword(String keyword) {
        if (!matchKeyword(keyword)) {
            throw error("Expected keyword " + keyword);
        }
    }

    private boolean matchSymbol(String symbol) {
        Token token = tokenizer.peek();

        if (token.type == TokenType.SYMBOL && token.text.equals(symbol)) {
            tokenizer.next();
            return true;
        }

        return false;
    }

    private void expectSymbol(String symbol) {
        if (!matchSymbol(symbol)) {
            throw error("Expected symbol " + symbol);
        }
    }

    private String expectIdentifier(String message) {
        Token token = tokenizer.next();

        if (token.type != TokenType.IDENTIFIER) {
            throw error(message);
        }

        if (isReservedKeyword(token.text)) {
            throw new IllegalArgumentException(
                    "Reserved keyword cannot be used as identifier: " + token.text +
                            " at position " + token.position
            );
        }

        return token.text;
    }

    private String expectTypeName(String message) {
        Token token = tokenizer.next();

        if (token.type != TokenType.IDENTIFIER) {
            throw error(message);
        }

        return token.text;
    }

    private void expect(TokenType tokenType, String message) {
        Token token = tokenizer.next();

        if (token.type != tokenType) {
            throw error(message);
        }
    }

    private IllegalArgumentException error(String message) {
        Token token = tokenizer.peek();
        return new IllegalArgumentException(message + " near '" + token.text + "' at position " + token.position);
    }

    private boolean isReservedKeyword(String text) {
        return RESERVED_KEYWORDS.contains(text.toUpperCase());
    }

    private enum TokenType {
        IDENTIFIER,
        NUMBER,
        STRING,
        SYMBOL,
        EOF
    }

    private static class Token {
        private final TokenType type;
        private final String text;
        private final int position;

        private Token(TokenType type, String text, int position) {
            this.type = type;
            this.text = text;
            this.position = position;
        }
    }

    private static class Tokenizer {
        private final String sql;
        private int position;
        private Token current;

        private Tokenizer(String sql) {
            if (sql == null) {
                throw new IllegalArgumentException("SQL cannot be null");
            }

            this.sql = sql;
            this.position = 0;
            this.current = readNextToken();
        }

        private Token peek() {
            return current;
        }

        private Token next() {
            Token token = current;
            current = readNextToken();
            return token;
        }

        private Token readNextToken() {
            skipWhitespace();

            if (position >= sql.length()) {
                return new Token(TokenType.EOF, "<eof>", position);
            }

            char ch = sql.charAt(position);
            int start = position;

            if (Character.isLetter(ch) || ch == '_') {
                position++;

                while (position < sql.length()) {
                    char next = sql.charAt(position);

                    if (!Character.isLetterOrDigit(next) && next != '_') {
                        break;
                    }

                    position++;
                }

                return new Token(TokenType.IDENTIFIER, sql.substring(start, position), start);
            }

            if (Character.isDigit(ch)) {
                position++;

                while (position < sql.length() && Character.isDigit(sql.charAt(position))) {
                    position++;
                }

                return new Token(TokenType.NUMBER, sql.substring(start, position), start);
            }

            if (ch == '\'') {
                return readStringToken();
            }

            if ("(),;*=".indexOf(ch) >= 0) {
                position++;
                return new Token(TokenType.SYMBOL, String.valueOf(ch), start);
            }

            throw new IllegalArgumentException("Unexpected character '" + ch + "' at position " + position);
        }

        private Token readStringToken() {
            int start = position;
            position++;
            StringBuilder builder = new StringBuilder();

            while (position < sql.length()) {
                char ch = sql.charAt(position);

                if (ch == '\'') {
                    if (position + 1 < sql.length() && sql.charAt(position + 1) == '\'') {
                        builder.append('\'');
                        position += 2;
                        continue;
                    }

                    position++;
                    return new Token(TokenType.STRING, builder.toString(), start);
                }

                builder.append(ch);
                position++;
            }

            throw new IllegalArgumentException("Unterminated string literal at position " + start);
        }

        private void skipWhitespace() {
            while (position < sql.length() && Character.isWhitespace(sql.charAt(position))) {
                position++;
            }
        }
    }
}
