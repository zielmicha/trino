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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BigQueryPageSink
        implements ConnectorPageSink
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private final BigQueryClient client;
    private final TableId tableId;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public BigQueryPageSink(BigQueryClient client, SchemaTableName schemaTableName, List<String> columnNames, List<Type> columnTypes)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(schemaTableName, "schemaTableName is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.tableId = TableId.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        InsertAllRequest.Builder batch = InsertAllRequest.newBuilder(tableId);
        for (int position = 0; position < page.getPositionCount(); position++) {
            Map<String, Object> row = new HashMap<>();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                row.put(columnNames.get(channel), getObjectValue(columnTypes.get(channel), page.getBlock(channel), position));
            }
            batch.addRow(row);
        }

        client.insert(batch.build());
        return NOT_BLOCKED;
    }

    @Nullable
    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        // TODO: Add support for decimal, time, timestamp, timestamp with time zone, geography, array, map, row type
        if (type.equals(BooleanType.BOOLEAN)) {
            return type.getBoolean(block, position);
        }
        if (type.equals(TinyintType.TINYINT)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (type.equals(IntegerType.INTEGER)) {
            return toIntExact(type.getLong(block, position));
        }
        if (type.equals(BigintType.BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return Base64.getEncoder().encodeToString(type.getSlice(block, position).getBytes());
        }
        if (type.equals(DateType.DATE)) {
            long days = type.getLong(block, position);
            return DATE_FORMATTER.format(LocalDate.ofEpochDay(days));
        }

        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
