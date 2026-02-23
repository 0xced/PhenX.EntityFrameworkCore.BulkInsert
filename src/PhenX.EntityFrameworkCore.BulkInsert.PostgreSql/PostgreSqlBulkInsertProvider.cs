using System.Text;

using JetBrains.Annotations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal.Mapping;

using NpgsqlTypes;

using PhenX.EntityFrameworkCore.BulkInsert.Metadata;

namespace PhenX.EntityFrameworkCore.BulkInsert.PostgreSql;

[UsedImplicitly]
internal partial class PostgreSqlBulkInsertProvider(ILogger<PostgreSqlBulkInsertProvider>? logger) : BulkInsertProviderBase<PostgreSqlDialectBuilder, PostgreSqlBulkInsertOptions>(logger)
{
    //language=sql
    /// <inheritdoc />
    protected override string AddTableCopyBulkInsertId => $"ALTER TABLE {{0}} ADD COLUMN {BulkInsertId} SERIAL PRIMARY KEY;";

    private static string GetBinaryImportCommand(IReadOnlyList<ColumnMetadata> properties, string tableName)
    {
        var sql = new StringBuilder();
        sql.Append($"COPY {tableName} (");
        sql.AppendColumns(properties);
        sql.Append(") FROM STDIN (FORMAT BINARY)");
        return sql.ToString();
    }

    /// <inheritdoc />
    protected override PostgreSqlBulkInsertOptions CreateDefaultOptions() => new()
    {
        Converters = [PostgreSqlGeometryConverter.Instance],
    };

    /// <inheritdoc />
    [Zomp.SyncMethodGenerator.CreateSyncVersion(PreserveCancellationToken = true)]
    protected override async Task BulkInsertAsync<T>(
        DbContext context,
        TableMetadata tableInfo,
        IEnumerable<T> entities,
        string tableName,
        IReadOnlyList<ColumnMetadata> columns,
        PostgreSqlBulkInsertOptions options,
        CancellationToken ctk)
    {
        var connection = (NpgsqlConnection)context.Database.GetDbConnection();
        var command = GetBinaryImportCommand(columns, tableName);

        await using var writer = await connection.BeginBinaryImportAsync(command, ctk);

        // The type mapping can be null for obvious types like string.
        var columnTypes = columns.Select(c => GetPostgreSqlType(c, options)).ToArray();

        long rowsCopied = 0;
        foreach (var entity in entities)
        {
            await writer.StartRowAsync(ctk);

            for (var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
            {
                var value = columns[columnIndex].GetValue(entity, options);

                // Get the actual type, so that the writer can do the conversation to the target type automatically.
                var type = columnTypes[columnIndex];

                if (type != null)
                {
                    await writer.WriteAsync(value, type.Value, ctk);
                }
                else
                {
                    await writer.WriteAsync(value, ctk);
                }
            }

            options.HandleOnProgress(ref rowsCopied);
        }

        await writer.CompleteAsync(ctk);
    }

    private static NpgsqlDbType? GetPostgreSqlType(ColumnMetadata column, PostgreSqlBulkInsertOptions options)
    {
        var typeProviders = options.TypeProviders;
        if (typeProviders is { Count: > 0 })
        {
            foreach (var typeProvider in typeProviders)
            {
                if (typeProvider.TryGetType(column.Property, out var type))
                {
                    return type;
                }
            }
        }

        var mapping = column.Property.GetRelationalTypeMapping() as NpgsqlTypeMapping;

        return mapping?.NpgsqlDbType;
    }
}
