using System.Collections;
using System.Data.Common;
using System.Text;

using JetBrains.Annotations;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

using PhenX.EntityFrameworkCore.BulkInsert.Metadata;
using PhenX.EntityFrameworkCore.BulkInsert.Options;

namespace PhenX.EntityFrameworkCore.BulkInsert.Sqlite;

[UsedImplicitly]
internal class SqliteBulkInsertProvider(ILogger<SqliteBulkInsertProvider>? logger) : BulkInsertProviderBase<SqliteDialectBuilder, BulkInsertOptions>(logger)
{
    private const int MaxParams = 1000;

    /// <inheritdoc />
    protected override string BulkInsertId => "rowid";

    /// <inheritdoc />
    protected override string AddTableCopyBulkInsertId => "--"; // No need to add an ID column in SQLite

    /// <inheritdoc />
    protected override string GetTempTableName(string tableName) => $"_temp_bulk_insert_{Helpers.RandomString(6)}";

    /// <inheritdoc />
    protected override BulkInsertOptions CreateDefaultOptions() => new()
    {
        BatchSize = 5,
    };

    /// <inheritdoc />
    protected override Task AddBulkInsertIdColumn<T>(
        bool sync,
        DbContext context,
        string tempTableName,
        CancellationToken cancellationToken
    ) where T : class => Task.CompletedTask;

    private static SqliteType GetSqliteType(ColumnMetadata column)
    {
        var storeType = column.Property.GetRelationalTypeMapping().StoreType;

        if (string.Equals(storeType, "INTEGER", StringComparison.OrdinalIgnoreCase))
        {
            return SqliteType.Integer;
        }

        if (string.Equals(storeType, "FLOAT", StringComparison.OrdinalIgnoreCase) || string.Equals(storeType, "REAL", StringComparison.OrdinalIgnoreCase))
        {
            return SqliteType.Real;
        }

        if (string.Equals(storeType, "TEXT", StringComparison.OrdinalIgnoreCase))
        {
            return SqliteType.Text;
        }

        if (string.Equals(storeType, "BLOB", StringComparison.OrdinalIgnoreCase))
        {
            return SqliteType.Blob;
        }

        throw new NotSupportedException($"Invalid store type '{storeType}' for property '{column.PropertyName}'");
    }

    private static DbCommand GetInsertCommand(
        DbContext context,
        string tableName,
        ColumnMetadata[] columns,
        SqliteType[] columnTypes,
        StringBuilder sb,
        int batchSize)
    {
        var command = context.Database.GetDbConnection().CreateCommand();

        sb.Clear();
        sb.AppendLine($"INSERT INTO {tableName} (");
        sb.AppendColumns(columns);
        sb.AppendLine(")");
        sb.AppendLine("VALUES");

        var p = 0;
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append('(');

            var columnIndex = 0;
            for (var index = 0; index < columns.Length; index++)
            {
                var parameterName = $"@p{p++}";
                command.Parameters.Add(new SqliteParameter(parameterName, columnTypes[columnIndex]));

                if (columnIndex > 0)
                {
                    sb.Append(", ");
                }

                sb.Append(parameterName);
                columnIndex++;
            }

            sb.Append(')');
            sb.AppendLine();
        }

        command.CommandText = sb.ToString();
        command.Prepare();

        return command;
    }

    /// <inheritdoc />
    protected override Task DropTempTableAsync(bool sync, DbContext dbContext, string tableName)
    {
        return ExecuteAsync(sync, dbContext, $"DROP TABLE IF EXISTS {tableName}", default);
    }

    /// <inheritdoc />
    protected override async Task<long> BulkInsert<T>(
        bool sync,
        DbContext context,
        TableMetadata tableInfo,
        IAsyncEnumerable<T> entities,
        string tableName,
        IReadOnlyList<ColumnMetadata> columns,
        BulkInsertOptions options,
        CancellationToken ctk
    ) where T : class
    {
        var batchSize = Math.Min(options.BatchSize, MaxParams / columns.Count);

        // The StringBuilder can be reused between the batches.
        var sb = new StringBuilder();

        var columnList = tableInfo.GetColumns(options.CopyGeneratedColumns);
        var columnTypes = columnList.Select(GetSqliteType).ToArray();


        if (sync)
        {
            return BulkInsertSync(batchSize, context, sb, columnList, columnTypes, new SyncEnumerable<T>(entities, ctk), tableName, columns, options, ctk);
        }

        return await BulkInsertAsync(batchSize, context, sb, columnList, columnTypes, entities, tableName, columns, options, ctk);
    }

    private static long BulkInsertSync<T>(
        int batchSize,
        DbContext context,
        StringBuilder sb,
        ColumnMetadata[] columnList,
        SqliteType[] columnTypes,
        IEnumerable<T> entities,
        string tableName,
        IReadOnlyList<ColumnMetadata> columns,
        BulkInsertOptions options,
        CancellationToken ctk) where T : class
    {
        long rowsCopied = 0;

        DbCommand? insertCommand = null;
        try
        {
            foreach (var chunk in entities.Chunk(batchSize))
            {
                ctk.ThrowIfCancellationRequested();

                // Full chunks
                if (chunk.Length == batchSize)
                {
                    insertCommand ??=
                        GetInsertCommand(
                            context,
                            tableName,
                            columnList,
                            columnTypes,
                            sb,
                            batchSize);

                    FillValues(chunk, insertCommand.Parameters, columns, options);
                    insertCommand.ExecuteNonQuery();
                }
                // Last chunk
                else
                {
                    using var partialInsertCommand =
                        GetInsertCommand(
                            context,
                            tableName,
                            columnList,
                            columnTypes,
                            sb,
                            chunk.Length);

                    FillValues(chunk, partialInsertCommand.Parameters, columns, options);
                    partialInsertCommand.ExecuteNonQuery();
                }

                // Notify progress after each chunk
                for (var i = 0; i < chunk.Length; i++)
                {
                    options.HandleOnProgress(ref rowsCopied);
                }
            }
        }
        finally
        {
            insertCommand?.Dispose();
        }

        return rowsCopied;
    }

    private static async Task<long> BulkInsertAsync<T>(
        int batchSize,
        DbContext context,
        StringBuilder sb,
        ColumnMetadata[] columnList,
        SqliteType[] columnTypes,
        IAsyncEnumerable<T> entities,
        string tableName,
        IReadOnlyList<ColumnMetadata> columns,
        BulkInsertOptions options,
        CancellationToken ctk
    ) where T : class
    {
        long rowsCopied = 0;

        DbCommand? insertCommand = null;
        try
        {
            await foreach (var chunk in entities.Chunk(batchSize).WithCancellation(ctk))
            {
                // Full chunks
                if (chunk.Length == batchSize)
                {
                    insertCommand ??=
                        GetInsertCommand(
                            context,
                            tableName,
                            columnList,
                            columnTypes,
                            sb,
                            batchSize);

                    FillValues(chunk, insertCommand.Parameters, columns, options);
                    await insertCommand.ExecuteNonQueryAsync(ctk);
                }
                // Last chunk
                else
                {
                    await using var partialInsertCommand =
                        GetInsertCommand(
                            context,
                            tableName,
                            columnList,
                            columnTypes,
                            sb,
                            chunk.Length);

                    FillValues(chunk, partialInsertCommand.Parameters, columns, options);
                    await partialInsertCommand.ExecuteNonQueryAsync(ctk);
                }

                // Notify progress after each chunk
                for (var i = 0; i < chunk.Length; i++)
                {
                    options.HandleOnProgress(ref rowsCopied);
                }
            }
        }
        finally
        {
            if (insertCommand != null)
            {
                await insertCommand.DisposeAsync();
            }
        }

        return rowsCopied;
    }

    private static void FillValues<T>(
        T[] chunk,
        DbParameterCollection parameters,
        IReadOnlyList<ColumnMetadata> columns,
        BulkInsertOptions options) where T : class
    {
        var p = 0;

        for (var chunkIndex = 0; chunkIndex < chunk.Length; chunkIndex++)
        {
            var entity = chunk[chunkIndex];

            for (var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
            {
                var column = columns[columnIndex];
                var value = column.GetValue(entity, options);
                parameters[p].Value = value;
                p++;
            }
        }
    }
}

internal class SyncEnumerable<T>(IAsyncEnumerable<T> enumerable, CancellationToken ctk) : IEnumerable<T>
{
    public IEnumerator<T> GetEnumerator() => new SyncEnumerator<T>(enumerable.GetAsyncEnumerator(ctk), ctk);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

internal class SyncEnumerator<T>(IAsyncEnumerator<T> enumerator, CancellationToken ctk) : IEnumerator<T>
{
    public bool MoveNext()
    {
        ctk.ThrowIfCancellationRequested();
        return enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult();
    }

    public void Reset() => throw new NotImplementedException();

    public T Current => enumerator.Current;

    object? IEnumerator.Current => Current;

    public void Dispose() => enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
}
