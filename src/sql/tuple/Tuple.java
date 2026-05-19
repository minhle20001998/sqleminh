package sql.tuple;

import sql.schema.Column;
import sql.schema.Schema;
import sql.schema.SqlType;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Tuple {
    private final Schema schema;
    private final List<Value> values;

    public Tuple(Schema schema, List<Value> values) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");

        if (values == null) {
            throw new IllegalArgumentException("Values cannot be null");
        }

        if (schema.size() != values.size()) {
            throw new IllegalArgumentException(
                    "Value count does not match schema. expected=" + schema.size() +
                            ", actual=" + values.size()
            );
        }

        List<Value> copiedValues = new ArrayList<>();

        for (int i = 0; i < values.size(); i++) {
            Value value = values.get(i);
            Column column = schema.getColumn(i);

            if (value == null) {
                throw new IllegalArgumentException("Value cannot be null. Use Value.nullValue(type)");
            }

            if (value.getType() != column.getType()) {
                throw new IllegalArgumentException(
                        "Value type does not match column " + column.getName() +
                                ". expected=" + column.getType() +
                                ", actual=" + value.getType()
                );
            }

            if (value.isNull() && !column.isNullable()) {
                throw new IllegalArgumentException("Column cannot be NULL: " + column.getName());
            }

            copiedValues.add(value);
        }

        this.values = Collections.unmodifiableList(copiedValues);
    }

    public Schema getSchema() {
        return schema;
    }

    public List<Value> getValues() {
        return values;
    }

    public Value getValue(int index) {
        return values.get(index);
    }

    public Value getValue(String columnName) {
        return values.get(schema.getColumnIndex(columnName));
    }

    public byte[] serialize() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] nullBitmap = new byte[nullBitmapSize(schema.size())];

        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).isNull()) {
                setNullBit(nullBitmap, i);
            }
        }

        writeBytes(output, nullBitmap);

        for (int i = 0; i < values.size(); i++) {
            Value value = values.get(i);

            if (value.isNull()) {
                continue;
            }

            writeValue(output, value);
        }

        return output.toByteArray();
    }

    public static Tuple deserialize(Schema schema, byte[] bytes) {
        Objects.requireNonNull(schema, "Schema cannot be null");

        if (bytes == null) {
            throw new IllegalArgumentException("Tuple bytes cannot be null");
        }

        int nullBitmapSize = nullBitmapSize(schema.size());

        if (bytes.length < nullBitmapSize) {
            throw new IllegalArgumentException("Tuple bytes are smaller than null bitmap");
        }

        byte[] nullBitmap = new byte[nullBitmapSize];
        System.arraycopy(bytes, 0, nullBitmap, 0, nullBitmapSize);

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(nullBitmapSize);

        List<Value> values = new ArrayList<>();

        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.getColumn(i);

            if (isNullBitSet(nullBitmap, i)) {
                values.add(Value.nullValue(column.getType()));
                continue;
            }

            values.add(readValue(buffer, column.getType()));
        }

        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Tuple bytes contain trailing data: " + buffer.remaining());
        }

        return new Tuple(schema, values);
    }

    private static void writeValue(ByteArrayOutputStream output, Value value) {
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        if (value.getType() == SqlType.INT) {
            writeBytes(output, intBuffer.putInt(value.asInt()).array());
            return;
        }

        if (value.getType() == SqlType.TEXT) {
            byte[] textBytes = value.asText().getBytes(StandardCharsets.UTF_8);
            writeBytes(output, intBuffer.putInt(textBytes.length).array());
            writeBytes(output, textBytes);
            return;
        }

        throw new IllegalArgumentException("Unsupported value type: " + value.getType());
    }

    private static Value readValue(ByteBuffer buffer, SqlType type) {
        if (type == SqlType.INT) {
            requireRemaining(buffer, Integer.BYTES, "INT");
            return Value.intValue(buffer.getInt());
        }

        if (type == SqlType.TEXT) {
            requireRemaining(buffer, Integer.BYTES, "TEXT length");
            int length = buffer.getInt();

            if (length < 0) {
                throw new IllegalArgumentException("TEXT length cannot be negative");
            }

            requireRemaining(buffer, length, "TEXT bytes");
            byte[] textBytes = new byte[length];
            buffer.get(textBytes);

            return Value.textValue(new String(textBytes, StandardCharsets.UTF_8));
        }

        throw new IllegalArgumentException("Unsupported SQL type: " + type);
    }

    private static void requireRemaining(ByteBuffer buffer, int size, String label) {
        if (buffer.remaining() < size) {
            throw new IllegalArgumentException("Not enough bytes to read " + label);
        }
    }

    private static void writeBytes(ByteArrayOutputStream output, byte[] bytes) {
        output.write(bytes, 0, bytes.length);
    }

    private static int nullBitmapSize(int columnCount) {
        return (columnCount + 7) / 8;
    }

    private static void setNullBit(byte[] nullBitmap, int columnIndex) {
        int byteIndex = columnIndex / 8;
        int bitIndex = columnIndex % 8;
        nullBitmap[byteIndex] = (byte) (nullBitmap[byteIndex] | (1 << bitIndex));
    }

    private static boolean isNullBitSet(byte[] nullBitmap, int columnIndex) {
        int byteIndex = columnIndex / 8;
        int bitIndex = columnIndex % 8;
        return (nullBitmap[byteIndex] & (1 << bitIndex)) != 0;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "schema=" + schema +
                ", values=" + values +
                '}';
    }
}
