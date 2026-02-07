using System.Collections;
using System.Data.Common;

using PhenX.EntityFrameworkCore.BulkInsert.Metadata;
using PhenX.EntityFrameworkCore.BulkInsert.Options;

namespace PhenX.EntityFrameworkCore.BulkInsert;

internal sealed class EnumerableDataReader<T>(
    IAsyncEnumerable<T> rows,
    IReadOnlyList<ColumnMetadata> columns,
    BulkInsertOptions options) : DbDataReader
{
    private readonly IAsyncEnumerator<T> _enumerator = rows.GetAsyncEnumerator();
    private readonly Dictionary<string, int> _ordinalMap =
        columns
            .Select((c, i) => (Column: c, Index: i))
            .ToDictionary(
                p => p.Column.PropertyName,
                p => p.Index
            );
    private int _recordsAffected;

    public override object GetValue(int i)
    {
        var current = _enumerator.Current;
        if (current == null)
        {
            return DBNull.Value;
        }

        return columns[i].GetValue(current, options);
    }

    public override int GetValues(object[] values)
    {
        var current = _enumerator.Current;
        if (current == null)
        {
            return 0;
        }

        for (var i = 0; i < columns.Count; i++)
        {
            values[i] = columns[i].GetValue(current, options);
        }

        return columns.Count;
    }

    public override bool Read()
    {
        var moreRows = _enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult();
        if (moreRows)
        {
            _recordsAffected++;
        }
        return moreRows;
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var moreRows = await _enumerator.MoveNextAsync();
        if (moreRows)
        {
            _recordsAffected++;
        }
        return moreRows;
    }

    public override IEnumerator GetEnumerator() => throw new NotImplementedException();

    public override Type GetFieldType(int i) => columns[i].ClrType;

    public override int GetOrdinal(string name) => _ordinalMap.GetValueOrDefault(name, -1);

    public override int FieldCount => columns.Count;

    public override bool HasRows => throw new NotImplementedException();

    public override int Depth => 0;

    public override int RecordsAffected => _recordsAffected;

    public override bool IsClosed => false;

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await _enumerator.DisposeAsync();
    }

    public override bool NextResult() => throw new NotImplementedException();

    public override bool IsDBNull(int i) => GetValue(i) is DBNull;

    public override object this[int i] => throw new NotImplementedException();

    public override object this[string name] => throw new NotImplementedException();

    public override string GetString(int i) => throw new NotImplementedException();

    public override bool GetBoolean(int i) => throw new NotImplementedException();

    public override byte GetByte(int i) => throw new NotImplementedException();

    public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();

    public override char GetChar(int i) => throw new NotImplementedException();

    public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();

    public override string GetDataTypeName(int i) => throw new NotImplementedException();

    public override DateTime GetDateTime(int i) => throw new NotImplementedException();

    public override decimal GetDecimal(int i) => throw new NotImplementedException();

    public override double GetDouble(int i) => throw new NotImplementedException();

    public override float GetFloat(int i) => throw new NotImplementedException();

    public override Guid GetGuid(int i) => throw new NotImplementedException();

    public override short GetInt16(int i) => throw new NotImplementedException();

    public override int GetInt32(int i) => throw new NotImplementedException();

    public override long GetInt64(int i) => throw new NotImplementedException();

    public override string GetName(int i) => throw new NotImplementedException();
}
