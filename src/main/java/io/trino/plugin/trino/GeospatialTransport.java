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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.HexFormat;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

final class GeospatialTransport
{
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private GeospatialTransport() {}

    static boolean isGeospatialType(Type type)
    {
        return type.getBaseName().equalsIgnoreCase("Geometry") || type.getBaseName().equalsIgnoreCase("SphericalGeography");
    }

    static boolean containsGeospatialType(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return containsGeospatialType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return containsGeospatialType(mapType.getKeyType()) || containsGeospatialType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(GeospatialTransport::containsGeospatialType);
        }
        return isGeospatialType(type);
    }

    static String ewkbExpression(String reference, Type type)
    {
        if (type.getBaseName().equalsIgnoreCase("Geometry")) {
            return "ST_AsEWKB(" + reference + ")";
        }
        if (type.getBaseName().equalsIgnoreCase("SphericalGeography")) {
            return "ST_AsEWKB(to_geometry(" + reference + "))";
        }
        throw new IllegalArgumentException("Not a geospatial type: " + type);
    }

    static Object readObject(byte[] ewkb, Type type)
    {
        if (ewkb == null) {
            return null;
        }
        try {
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            type.writeSlice(builder, Slices.wrappedBuffer(ewkb));
            return type.getObject(builder.build(), 0);
        }
        catch (RuntimeException e) {
            throw new TrinoException(JDBC_ERROR, "Invalid geospatial EWKB transport value for " + type, e);
        }
    }

    static void writeHexToBlock(String hex, Type type, BlockBuilder builder)
    {
        try {
            Slice ewkb = Slices.wrappedBuffer(HEX_FORMAT.parseHex(hex));
            // Validate before appending so malformed EWKB fails at the read boundary.
            BlockBuilder validation = type.createBlockBuilder(null, 1);
            type.writeSlice(validation, ewkb);
            type.getObject(validation.build(), 0);
            type.writeSlice(builder, ewkb);
        }
        catch (RuntimeException e) {
            throw new TrinoException(JDBC_ERROR, "Invalid geospatial EWKB transport value for " + type, e);
        }
    }
}
