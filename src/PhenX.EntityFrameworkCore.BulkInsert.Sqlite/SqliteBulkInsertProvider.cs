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
internal partial class SqliteBulkInsertProvider(ILogger<SqliteBulkInsertProvider>? logger) : BulkInsertProviderBase<SqliteDialectBuilder, BulkInsertOptions>(logger)
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
        IReadOnlyList<ColumnMetadata> columns,
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
            for (var index = 0; index < columns.Count; index++)
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
    [Zomp.SyncMethodGenerator.CreateSyncVersion(PreserveCancellationToken = true)]
    protected override async Task BulkInsertAsync<T>(
        DbContext context,
        TableMetadata tableInfo,
        IEnumerable<T> entities,
        string tableName,
        IReadOnlyList<ColumnMetadata> columns,
        BulkInsertOptions options,
        CancellationToken ctk
    ) where T : class
    {
        var batchSize = Math.Min(options.BatchSize, MaxParams / columns.Count);

        long rowsCopied = 0;

        // The StringBuilder can be reused between the batches.
        var sb = new StringBuilder();

        var columnList = tableInfo.GetColumns(options.CopyGeneratedColumns);
        var columnTypes = columnList.Select(GetSqliteType).ToArray();

        DbCommand? insertCommand = null;
        try
        {
            foreach (var chunk in entities.Chunk(batchSize))
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

