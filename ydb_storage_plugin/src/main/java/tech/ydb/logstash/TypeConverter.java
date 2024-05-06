package tech.ydb.logstash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TypeConverter {

    public static PrimitiveValue toValue(Object data, PrimitiveType type) {
        if (data == null) {
            return null;
        }
        if (data instanceof Boolean) {
            return booleanToValue((Boolean) data, type);
        }
        if (data instanceof Byte) {
            return byteToValue((Byte) data, type);
        }
        if (data instanceof Short) {
            return shortToValue((Short) data, type);
        }
        if (data instanceof Integer) {
            return integerToValue((Integer) data, type);
        }
        if (data instanceof Long) {
            return longToValue((Long) data, type);
        }
        if (data instanceof Float) {
            return floatToValue((Float) data, type);
        }
        if (data instanceof Double) {
            return doubleToValue((Double) data, type);
        }
        if (data instanceof String) {
            return stringToValue((String) data, type);
        }
        if (data instanceof byte[]) {
            return bytesToValue((byte[]) data, type);
        }
        if (data instanceof Instant) {
            return instantToValue((Instant) data, type);
        }
        if (data instanceof UUID) {
            return uuidToValue((UUID) data, type);
        }

        return null;
    }

    private static PrimitiveValue booleanToValue(Boolean data, PrimitiveType type) {
        switch (type) {
            case Bool: return PrimitiveValue.newBool(data);
            case Text: return PrimitiveValue.newText(data.toString());
            default:
                return null;
        }
    }

    private static PrimitiveValue byteToValue(Byte data, PrimitiveType type) {
        switch (type) {
            case Int8: return PrimitiveValue.newInt8(data);
            case Int16: return PrimitiveValue.newInt16(data);
            case Int32: return PrimitiveValue.newInt32(data);
            case Int64: return PrimitiveValue.newInt64(data);

            case Uint8: return PrimitiveValue.newUint8(data);
            case Uint16: return PrimitiveValue.newUint16(data);
            case Uint32: return PrimitiveValue.newUint32(data);
            case Uint64: return PrimitiveValue.newUint64(data);

            case Text: return PrimitiveValue.newText(String.valueOf(data));

            case Float: return PrimitiveValue.newFloat(data);
            case Double: return PrimitiveValue.newDouble(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue shortToValue(Short data, PrimitiveType type) {
        switch (type) {
            case Int8: return PrimitiveValue.newInt8(data.byteValue());
            case Int16: return PrimitiveValue.newInt16(data);
            case Int32: return PrimitiveValue.newInt32(data);
            case Int64: return PrimitiveValue.newInt64(data);

            case Uint8: return PrimitiveValue.newUint8(data);
            case Uint16: return PrimitiveValue.newUint16(data);
            case Uint32: return PrimitiveValue.newUint32(data);
            case Uint64: return PrimitiveValue.newUint64(data);

            case Text: return PrimitiveValue.newText(String.valueOf(data));

            case Float: return PrimitiveValue.newFloat(data);
            case Double: return PrimitiveValue.newDouble(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue integerToValue(Integer data, PrimitiveType type) {
        switch (type) {
            case Int8: return PrimitiveValue.newInt8(data.byteValue());
            case Int16: return PrimitiveValue.newInt16(data.shortValue());
            case Int32: return PrimitiveValue.newInt32(data);
            case Int64: return PrimitiveValue.newInt64(data);

            case Uint8: return PrimitiveValue.newUint8(data);
            case Uint16: return PrimitiveValue.newUint16(data);
            case Uint32: return PrimitiveValue.newUint32(data);
            case Uint64: return PrimitiveValue.newUint64(data);

            case Text: return PrimitiveValue.newText(String.valueOf(data));

            case Float: return PrimitiveValue.newFloat(data);
            case Double: return PrimitiveValue.newDouble(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue longToValue(Long data, PrimitiveType type) {
        switch (type) {
            case Int8: return PrimitiveValue.newInt8(data.byteValue());
            case Int16: return PrimitiveValue.newInt16(data.shortValue());
            case Int32: return PrimitiveValue.newInt32(data.intValue());
            case Int64: return PrimitiveValue.newInt64(data);

            case Uint8: return PrimitiveValue.newUint8(data.intValue());
            case Uint16: return PrimitiveValue.newUint16(data.intValue());
            case Uint32: return PrimitiveValue.newUint32(data);
            case Uint64: return PrimitiveValue.newUint64(data);

            case Text: return PrimitiveValue.newText(String.valueOf(data));

            case Float: return PrimitiveValue.newFloat(data);
            case Double: return PrimitiveValue.newDouble(data);

            case Date: return PrimitiveValue.newDate(Instant.ofEpochMilli(data));
            case Datetime: return PrimitiveValue.newDatetime(Instant.ofEpochMilli(data));
            case Timestamp: return PrimitiveValue.newTimestamp(Instant.ofEpochMilli(data));
            default:
                return null;
        }
    }

    private static PrimitiveValue floatToValue(Float data, PrimitiveType type) {
        switch (type) {
            case Text: return PrimitiveValue.newText(String.valueOf(data));
            case Float: return PrimitiveValue.newFloat(data);
            case Double: return PrimitiveValue.newDouble(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue doubleToValue(Double data, PrimitiveType type) {
        switch (type) {
            case Text: return PrimitiveValue.newText(String.valueOf(data));
            case Float: return PrimitiveValue.newFloat(data.floatValue());
            case Double: return PrimitiveValue.newDouble(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue stringToValue(String data, PrimitiveType type) {
        switch (type) {
            case Text: return PrimitiveValue.newText(data);
            case Bytes: return PrimitiveValue.newBytes(data.getBytes(StandardCharsets.UTF_8));
            default:
                return null;
        }
    }

    private static PrimitiveValue bytesToValue(byte[] data, PrimitiveType type) {
        switch (type) {
            case Bytes: return PrimitiveValue.newBytes(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue instantToValue(Instant data, PrimitiveType type) {
        switch (type) {
            case Date: return PrimitiveValue.newDate(data);
            case Datetime: return PrimitiveValue.newDatetime(data);
            case Timestamp: return PrimitiveValue.newTimestamp(data);
            default:
                return null;
        }
    }

    private static PrimitiveValue uuidToValue(UUID data, PrimitiveType type) {
        switch (type) {
            case Text: return PrimitiveValue.newText(data.toString());
            case Bytes: {
                ByteBuffer bb = ByteBuffer.allocate(16);
                bb.putLong(data.getMostSignificantBits());
                bb.putLong(data.getLeastSignificantBits());
                return PrimitiveValue.newBytes(bb.array());
            }
            case Uuid: return PrimitiveValue.newUuid(data);
            default:
                return null;
        }
    }
}
