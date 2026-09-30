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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestGeospatialTransport
{
    private static final String POINT_SRID_EWKB = "0101000020E6100000000000000000F03F0000000000000040";

    @Test
    void testTransportClassificationAndExpressions()
    {
        Type geometry = geospatialType("Geometry");
        Type spherical = geospatialType("SphericalGeography");

        assertThat(TrinoTypeClassifier.transportKind(geometry)).isEqualTo(TrinoTypeClassifier.TransportKind.GEOSPATIAL_EWKB);
        assertThat(TrinoTypeClassifier.transportKind(spherical)).isEqualTo(TrinoTypeClassifier.TransportKind.GEOSPATIAL_EWKB);
        assertThat(TrinoTypeClassifier.requiresJsonTransport(new ArrayType(geometry))).isTrue();
        assertThat(TrinoTypeClassifier.requiresJsonTransport(new ArrayType(spherical))).isTrue();
        assertThat(GeospatialTransport.ewkbExpression("g", geometry)).isEqualTo("ST_AsEWKB(g)");
        assertThat(GeospatialTransport.ewkbExpression("s", spherical)).isEqualTo("ST_AsEWKB(to_geometry(s))");
    }

    @Test
    void testNestedEwkbDecodingAndNull()
            throws SQLException
    {
        Type geometry = geospatialType("Geometry");
        Block block = JsonTransportCodec.readJsonArray(resultSet("[\"" + POINT_SRID_EWKB + "\", null]"), 1, new ArrayType(geometry));

        assertThat(geometry.getSlice(block, 0).getBytes()).isEqualTo(HexFormat.of().parseHex(POINT_SRID_EWKB));
        assertThat(block.isNull(1)).isTrue();
    }

    @Test
    void testMalformedGeospatialPayloadFailsAtReadBoundary()
    {
        Type geometry = geospatialType("Geometry");
        assertThatThrownBy(() -> GeospatialTransport.readObject(new byte[] {0}, geometry))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(JDBC_ERROR.toErrorCode()));
        for (String payload : new String[] {"not-hex", "00"}) {
            assertThatThrownBy(() -> JsonTransportCodec.readJsonArray(resultSet("[\"" + payload + "\"]"), 1, new ArrayType(geometry)))
                    .isInstanceOfSatisfying(TrinoException.class, exception -> {
                        assertThat(exception.getErrorCode()).isEqualTo(JDBC_ERROR.toErrorCode());
                        assertThat(exception).hasMessageContaining("Invalid geospatial EWKB transport value");
                    });
        }
    }

    private static Type geospatialType(String name)
    {
        for (Type type : GeospatialTestPlugin.load().getTypes()) {
            if (type.getBaseName().equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new AssertionError("Geospatial test plugin did not register " + name);
    }

    private static ResultSet resultSet(String value)
    {
        return (ResultSet) Proxy.newProxyInstance(
                TestGeospatialTransport.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (_, method, _) -> {
                    if (method.getName().equals("getString")) {
                        return value;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
