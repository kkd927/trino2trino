/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.trino;

import io.airlift.slice.Slices;
import io.airlift.stats.TDigest;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

import java.sql.Types;
import java.time.LocalTime;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.math.RoundingMode.UNNECESSARY;

final class TrinoReadMappingFactory
{
    private final TypeManager typeManager;
    private final Function<JdbcTypeHandle, Optional<ColumnMapping>> unsupportedTypeMapping;

    TrinoReadMappingFactory(TypeManager typeManager, Function<JdbcTypeHandle, Optional<ColumnMapping>> unsupportedTypeMapping)
    {
        this.typeManager = typeManager;
        this.unsupportedTypeMapping = unsupportedTypeMapping;
    }

    Optional<ColumnMapping> createColumnMapping(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.jdbcTypeName().orElse("");
        String normalizedTypeName = TrinoJdbcTypeHandleResolver.normalizedTypeName(typeName);
        Type logicalType = typeName.isEmpty() ? null : TrinoTypeNameParser.parseTypeName(typeName, typeManager);
        Optional<ColumnMapping> transportMapping = transportFallbackColumnMapping(logicalType);

        switch (typeHandle.jdbcType()) {
            case Types.BIT, Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.REAL:
                return Optional.of(realColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL, Types.NUMERIC:
                return Optional.of(decimalColumnMapping(decimalType(typeHandle, logicalType), UNNECESSARY));
            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle, logicalType));
            case Types.VARCHAR, Types.LONGVARCHAR:
                return Optional.of(varcharColumnMapping(varcharType(typeHandle, logicalType)));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle, typeName, logicalType));
            case Types.TIMESTAMP:
                return timestampReadMapping(typeHandle, typeName, normalizedTypeName, logicalType);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return timestampWithTimeZoneColumnMapping(typeHandle, typeName, logicalType);
            case Types.ARRAY:
                return transportMapping.isPresent() ? transportMapping : toArrayMapping(session, typeHandle, typeName);
            case Types.JAVA_OBJECT:
                return transportMapping.isPresent() ? transportMapping : toComplexTypeMapping(session, typeHandle, typeName, normalizedTypeName);
            case Types.TIME_WITH_TIMEZONE:
                return timeWithTimeZoneColumnMapping(typeHandle, typeName, logicalType);
            default:
                break;
        }

        return transportMapping.isPresent() ? transportMapping : fallbackToVarchar(session, typeHandle);
    }

    private ColumnMapping timeColumnMapping(JdbcTypeHandle typeHandle, String typeName, Type logicalType)
    {
        int timePrecision = extractTemporalPrecision(typeHandle, typeName, 3);
        Type resolvedType = logicalType == null ? createTimeType(timePrecision) : logicalType;
        if (resolvedType instanceof TimeType timeType && timeType.getPrecision() > 9) {
            return varcharTransportColumnMapping(timeType);
        }
        TimeType timeType = createTimeType(timePrecision);
        return ColumnMapping.longMapping(
                timeType,
                (rs, idx) -> TemporalTransportCodec.parseTimeToPicos(rs.getString(idx)),
                (stmt, idx, picosOfDay) -> stmt.setObject(idx, "TIME '" + LocalTime.ofNanoOfDay(picosOfDay / 1_000L) + "'"),
                FULL_PUSHDOWN);
    }

    private Optional<ColumnMapping> timestampReadMapping(JdbcTypeHandle typeHandle, String typeName, String normalizedTypeName, Type logicalType)
    {
        int timestampPrecision = extractTemporalPrecision(typeHandle, typeName, 3);
        if (normalizedTypeName.startsWith("timestamp") && normalizedTypeName.contains("with time zone")) {
            Type resolvedType = logicalType == null ? createTimestampWithTimeZoneType(timestampPrecision) : logicalType;
            if (resolvedType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.getPrecision() > 9) {
                return Optional.of(varcharTransportColumnMapping(timestampWithTimeZoneType));
            }
            return toTimestampWithTimeZoneMapping(timestampPrecision);
        }

        Type resolvedType = logicalType == null ? createTimestampType(timestampPrecision) : logicalType;
        if (resolvedType instanceof TimestampType timestampType && timestampType.getPrecision() > 9) {
            return Optional.of(varcharTransportColumnMapping(timestampType));
        }
        return Optional.of(timestampColumnMapping(createTimestampType(timestampPrecision)));
    }

    private Optional<ColumnMapping> timestampWithTimeZoneColumnMapping(JdbcTypeHandle typeHandle, String typeName, Type logicalType)
    {
        int precision = extractTemporalPrecision(typeHandle, typeName, 3);
        Type resolvedType = logicalType == null ? createTimestampWithTimeZoneType(precision) : logicalType;
        if (resolvedType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.getPrecision() > 9) {
            return Optional.of(varcharTransportColumnMapping(timestampWithTimeZoneType));
        }
        return toTimestampWithTimeZoneMapping(precision);
    }

    private Optional<ColumnMapping> timeWithTimeZoneColumnMapping(JdbcTypeHandle typeHandle, String typeName, Type logicalType)
    {
        int precision = extractTemporalPrecision(typeHandle, typeName, 3);
        Type resolvedType = logicalType == null ? createTimeWithTimeZoneType(precision) : logicalType;
        if (resolvedType instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return Optional.of(varcharTransportColumnMapping(timeWithTimeZoneType));
        }
        return Optional.empty();
    }

    private Optional<ColumnMapping> toTimestampWithTimeZoneMapping(int precision)
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return Optional.of(ColumnMapping.longMapping(
                    type,
                    (rs, idx) -> TemporalTransportCodec.parseShortTimestampWithTimeZone(rs.getString(idx)),
                    TemporalTransportCodec.shortTimestampWithTimeZoneTransportWriteFunction(type),
                    TrinoTypeClassifier.transportPredicatePushdownController(type)));
        }
        return Optional.of(ColumnMapping.objectMapping(
                type,
                ObjectReadFunction.of(LongTimestampWithTimeZone.class, (rs, idx) -> TemporalTransportCodec.parseLongTimestampWithTimeZone(rs.getString(idx))),
                TemporalTransportCodec.longTimestampWithTimeZoneTransportWriteFunction(type),
                TrinoTypeClassifier.transportPredicatePushdownController(type)));
    }

    private Optional<ColumnMapping> transportFallbackColumnMapping(Type logicalType)
    {
        if (logicalType == null) {
            return Optional.empty();
        }
        if (TrinoTypeClassifier.requiresVarbinaryTransport(logicalType)) {
            return Optional.of(varbinaryTransportColumnMapping(logicalType));
        }
        if (TrinoTypeClassifier.requiresJsonTransport(logicalType)) {
            return Optional.of(jsonTransportColumnMapping(logicalType));
        }
        if (TrinoTypeClassifier.requiresVarcharTransport(logicalType)) {
            return Optional.of(varcharTransportColumnMapping(logicalType));
        }
        return Optional.empty();
    }

    private ColumnMapping varcharTransportColumnMapping(Type logicalType)
    {
        PredicatePushdownController predicatePushdownController = TrinoTypeClassifier.transportPredicatePushdownController(logicalType);
        if (logicalType instanceof TimeType timeType) {
            return ColumnMapping.longMapping(
                    timeType,
                    (rs, idx) -> TemporalTransportCodec.parseTimeToPicos(rs.getString(idx)),
                    TemporalTransportCodec.timeTransportWriteFunction(timeType),
                    predicatePushdownController);
        }
        if (logicalType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return ColumnMapping.longMapping(
                        timestampType,
                        (rs, idx) -> TemporalTransportCodec.parseShortTimestamp(rs.getString(idx)),
                        TemporalTransportCodec.shortTimestampTransportWriteFunction(timestampType),
                        predicatePushdownController);
            }
            return ColumnMapping.objectMapping(
                    timestampType,
                    ObjectReadFunction.of(LongTimestamp.class, (rs, idx) -> TemporalTransportCodec.parseLongTimestamp(rs.getString(idx))),
                    TemporalTransportCodec.longTimestampTransportWriteFunction(timestampType),
                    predicatePushdownController);
        }
        if (logicalType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.isShort()) {
                return ColumnMapping.longMapping(
                        timestampWithTimeZoneType,
                        (rs, idx) -> TemporalTransportCodec.parseShortTimestampWithTimeZone(rs.getString(idx)),
                        TemporalTransportCodec.shortTimestampWithTimeZoneTransportWriteFunction(timestampWithTimeZoneType),
                        predicatePushdownController);
            }
            return ColumnMapping.objectMapping(
                    timestampWithTimeZoneType,
                    ObjectReadFunction.of(LongTimestampWithTimeZone.class, (rs, idx) -> TemporalTransportCodec.parseLongTimestampWithTimeZone(rs.getString(idx))),
                    TemporalTransportCodec.longTimestampWithTimeZoneTransportWriteFunction(timestampWithTimeZoneType),
                    predicatePushdownController);
        }
        if (logicalType instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            if (timeWithTimeZoneType.isShort()) {
                return ColumnMapping.longMapping(
                        timeWithTimeZoneType,
                        (rs, idx) -> TemporalTransportCodec.parseShortTimeWithTimeZone(rs.getString(idx)),
                        TemporalTransportCodec.shortTimeWithTimeZoneTransportWriteFunction(timeWithTimeZoneType),
                        predicatePushdownController);
            }
            return ColumnMapping.objectMapping(
                    timeWithTimeZoneType,
                    ObjectReadFunction.of(LongTimeWithTimeZone.class, (rs, idx) -> TemporalTransportCodec.parseLongTimeWithTimeZone(rs.getString(idx))),
                    TemporalTransportCodec.longTimeWithTimeZoneTransportWriteFunction(timeWithTimeZoneType),
                    predicatePushdownController);
        }
        if (TrinoTypeClassifier.isIntervalYearToMonthType(logicalType) || TrinoTypeClassifier.isIntervalDayToSecondType(logicalType)) {
            return ColumnMapping.longMapping(
                    logicalType,
                    (rs, idx) -> TemporalTransportCodec.parseIntervalValue(rs.getString(idx), logicalType),
                    TemporalTransportCodec.intervalTransportWriteFunction(logicalType),
                    predicatePushdownController);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported VARCHAR transport type: " + logicalType);
    }

    private ColumnMapping jsonTransportColumnMapping(Type logicalType)
    {
        if (logicalType instanceof ArrayType arrayType) {
            return ColumnMapping.objectMapping(
                    arrayType,
                    ObjectReadFunction.of(Block.class, (rs, idx) -> JsonTransportCodec.readJsonArray(rs, idx, arrayType)),
                    rejectingWriteFunction(Block.class),
                    DISABLE_PUSHDOWN);
        }
        if (logicalType instanceof MapType mapType) {
            return ColumnMapping.objectMapping(
                    mapType,
                    ObjectReadFunction.of(SqlMap.class, (rs, idx) -> JsonTransportCodec.readJsonMap(rs, idx, mapType)),
                    rejectingWriteFunction(SqlMap.class),
                    DISABLE_PUSHDOWN);
        }
        if (logicalType instanceof RowType rowType) {
            return ColumnMapping.objectMapping(
                    rowType,
                    ObjectReadFunction.of(SqlRow.class, (rs, idx) -> JsonTransportCodec.readJsonRow(rs, idx, rowType)),
                    rejectingWriteFunction(SqlRow.class),
                    DISABLE_PUSHDOWN);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported JSON transport type: " + logicalType);
    }

    private ColumnMapping varbinaryTransportColumnMapping(Type logicalType)
    {
        if (TrinoTypeClassifier.isSliceBackedSketchType(logicalType)) {
            return ColumnMapping.sliceMapping(
                    logicalType,
                    (rs, idx) -> {
                        byte[] bytes = rs.getBytes(idx);
                        return bytes == null ? null : Slices.wrappedBuffer(bytes);
                    },
                    (stmt, idx, value) -> {
                        throw unsupportedWriteException();
                    },
                    DISABLE_PUSHDOWN);
        }
        if (TrinoTypeClassifier.isTDigestType(logicalType)) {
            return ColumnMapping.objectMapping(
                    logicalType,
                    ObjectReadFunction.of(TDigest.class, (rs, idx) -> {
                        byte[] bytes = rs.getBytes(idx);
                        return bytes == null ? null : TDigest.deserialize(Slices.wrappedBuffer(bytes));
                    }),
                    rejectingWriteFunction(TDigest.class),
                    DISABLE_PUSHDOWN);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported VARBINARY transport type: " + logicalType);
    }

    private Optional<ColumnMapping> toArrayMapping(ConnectorSession session, JdbcTypeHandle typeHandle, String typeName)
    {
        Type parsedType = TrinoTypeNameParser.parseTypeName(typeName, typeManager);
        if (!(parsedType instanceof ArrayType arrayType)) {
            return fallbackToVarchar(session, typeHandle);
        }
        if (!TrinoTypeClassifier.supportsComplexReadType(arrayType.getElementType())) {
            return fallbackToVarchar(session, typeHandle);
        }
        return Optional.of(ColumnMapping.objectMapping(
                arrayType,
                ObjectReadFunction.of(Block.class, (rs, idx) -> JdbcComplexValueCodec.readArray(rs, idx, arrayType.getElementType())),
                ObjectWriteFunction.of(Block.class, (stmt, idx, block) -> stmt.setObject(idx, JdbcComplexValueCodec.toJdbcValue(block, arrayType))),
                DISABLE_PUSHDOWN));
    }

    private Optional<ColumnMapping> toComplexTypeMapping(ConnectorSession session, JdbcTypeHandle typeHandle, String typeName, String normalizedTypeName)
    {
        if (normalizedTypeName.startsWith("map(")) {
            return toMapMapping(session, typeHandle, typeName);
        }
        if (normalizedTypeName.startsWith("row(")) {
            return toRowMapping(session, typeHandle, typeName);
        }
        Optional<ColumnMapping> specialTypeMapping = toSpecialJavaObjectMapping(normalizedTypeName);
        if (specialTypeMapping.isPresent()) {
            return specialTypeMapping;
        }
        return fallbackToVarchar(session, typeHandle);
    }

    private Optional<ColumnMapping> toMapMapping(ConnectorSession session, JdbcTypeHandle typeHandle, String typeName)
    {
        Type parsedType = TrinoTypeNameParser.parseTypeName(typeName, typeManager);
        if (!(parsedType instanceof MapType mapType)) {
            return fallbackToVarchar(session, typeHandle);
        }
        if (!TrinoTypeClassifier.supportsComplexReadType(mapType.getKeyType()) || !TrinoTypeClassifier.supportsComplexReadType(mapType.getValueType())) {
            return fallbackToVarchar(session, typeHandle);
        }
        return Optional.of(ColumnMapping.objectMapping(
                mapType,
                ObjectReadFunction.of(SqlMap.class, (rs, idx) -> JdbcComplexValueCodec.readMap(rs, idx, mapType)),
                ObjectWriteFunction.of(SqlMap.class, (stmt, idx, sqlMap) -> stmt.setObject(idx, JdbcComplexValueCodec.toJdbcValue(sqlMap, mapType))),
                DISABLE_PUSHDOWN));
    }

    private Optional<ColumnMapping> toRowMapping(ConnectorSession session, JdbcTypeHandle typeHandle, String typeName)
    {
        Type parsedType = TrinoTypeNameParser.parseTypeName(typeName, typeManager);
        if (!(parsedType instanceof RowType rowType)) {
            return fallbackToVarchar(session, typeHandle);
        }
        if (rowType.getFields().stream().map(RowType.Field::getType).anyMatch(type -> !TrinoTypeClassifier.supportsComplexReadType(type))) {
            return fallbackToVarchar(session, typeHandle);
        }
        return Optional.of(ColumnMapping.objectMapping(
                rowType,
                ObjectReadFunction.of(SqlRow.class, (rs, idx) -> JdbcComplexValueCodec.readRow(rs, idx, rowType)),
                ObjectWriteFunction.of(SqlRow.class, (stmt, idx, sqlRow) -> stmt.setObject(idx, JdbcComplexValueCodec.toJdbcValue(sqlRow, rowType))),
                DISABLE_PUSHDOWN));
    }

    private Optional<ColumnMapping> toSpecialJavaObjectMapping(String normalizedTypeName)
    {
        return switch (normalizedTypeName) {
            case "uuid" -> Optional.of(ColumnMapping.sliceMapping(
                    UuidType.UUID,
                    (rs, idx) -> TrinoSpecialTypeCodec.uuidSlice(rs.getString(idx)),
                    (stmt, idx, value) -> stmt.setObject(idx, UuidType.trinoUuidToJavaUuid(value)),
                    DISABLE_PUSHDOWN));
            case "json" -> {
                Type jsonType = typeManager.fromSqlType("json");
                yield Optional.of(ColumnMapping.sliceMapping(
                        jsonType,
                        (rs, idx) -> TrinoSpecialTypeCodec.jsonSlice(rs.getString(idx)),
                        (stmt, idx, value) -> stmt.setString(idx, value.toStringUtf8()),
                        DISABLE_PUSHDOWN));
            }
            case "ipaddress" -> {
                Type ipAddressType = typeManager.fromSqlType("ipaddress");
                yield Optional.of(ColumnMapping.sliceMapping(
                        ipAddressType,
                        (rs, idx) -> TrinoSpecialTypeCodec.ipAddressSlice(rs.getString(idx)),
                        (stmt, idx, value) -> {
                            throw unsupportedWriteException();
                        },
                        DISABLE_PUSHDOWN));
            }
            default -> Optional.empty();
        };
    }

    private Optional<ColumnMapping> fallbackToVarchar(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return unsupportedTypeMapping.apply(typeHandle);
        }
        return Optional.empty();
    }

    private static <T> ObjectWriteFunction rejectingWriteFunction(Class<T> javaType)
    {
        return ObjectWriteFunction.of(javaType, (statement, index, value) -> {
            throw unsupportedWriteException();
        });
    }

    private static TrinoException unsupportedWriteException()
    {
        return new TrinoException(NOT_SUPPORTED, "This connector does not support writes");
    }

    private static int extractPrecision(String typeName, int defaultPrecision)
    {
        int open = typeName.indexOf('(');
        int close = typeName.indexOf(')');
        if (open >= 0 && close > open) {
            try {
                return Integer.parseInt(typeName.substring(open + 1, close).trim());
            }
            catch (NumberFormatException ignored) {
                return defaultPrecision;
            }
        }
        return defaultPrecision;
    }

    private static int extractTemporalPrecision(JdbcTypeHandle typeHandle, String typeName, int defaultPrecision)
    {
        int parsedPrecision = extractPrecision(typeName, -1);
        if (parsedPrecision >= 0) {
            return parsedPrecision;
        }
        return typeHandle.decimalDigits()
                .filter(precision -> precision >= 0)
                .orElse(defaultPrecision);
    }

    private static DecimalType decimalType(JdbcTypeHandle typeHandle, Type logicalType)
    {
        if (logicalType instanceof DecimalType decimalType) {
            return decimalType;
        }
        return createDecimalType(typeHandle.requiredColumnSize(), typeHandle.requiredDecimalDigits());
    }

    private static VarcharType varcharType(JdbcTypeHandle typeHandle, Type logicalType)
    {
        if (logicalType instanceof VarcharType varcharType) {
            return varcharType;
        }
        return varcharType(typeHandle.requiredColumnSize());
    }

    private static VarcharType varcharType(int varcharLength)
    {
        return varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
    }

    private static ColumnMapping charColumnMapping(JdbcTypeHandle typeHandle, Type logicalType)
    {
        if (logicalType instanceof CharType charType) {
            return charColumnMapping(charType);
        }
        int charLength = typeHandle.requiredColumnSize();
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(varcharType(charLength));
        }
        return charColumnMapping(createCharType(charLength));
    }

    private static ColumnMapping charColumnMapping(CharType charType)
    {
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                FULL_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(VarcharType varcharType)
    {
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }
}
