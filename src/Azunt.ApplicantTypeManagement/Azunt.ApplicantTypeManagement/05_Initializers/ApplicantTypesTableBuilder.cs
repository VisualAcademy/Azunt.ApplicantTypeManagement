using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azunt.ApplicantTypeManagement;

public sealed class ApplicantTypesTableBuilder
{
    private readonly string _masterConnectionString;
    private readonly ILogger<ApplicantTypesTableBuilder> _logger;

    public int CommandTimeoutSeconds { get; init; } = 60;
    public string TenantsQuerySql { get; init; } = "SELECT ConnectionString FROM dbo.Tenants";
    public string SchemaName { get; init; } = "dbo";
    public string TableName { get; init; } = "ApplicantTypes";

    public ApplicantTypesTableBuilder(string masterConnectionString, ILogger<ApplicantTypesTableBuilder> logger)
    {
        _masterConnectionString = masterConnectionString ?? throw new ArgumentNullException(nameof(masterConnectionString));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task BuildTenantDatabasesAsync(CancellationToken ct = default) =>
        BuildAsync(getTenants: true, ct);

    public Task BuildMasterDatabaseAsync(CancellationToken ct = default) =>
        BuildAsync(getTenants: false, ct);

    private async Task BuildAsync(bool getTenants, CancellationToken ct)
    {
        var targets = getTenants ? await GetTenantConnectionStringsAsync(ct) : new List<string> { _masterConnectionString };

        foreach (var connStr in targets)
        {
            var dbName = new SqlConnectionStringBuilder(connStr).InitialCatalog;
            try
            {
                await EnsureApplicantTypesTableAsync(connStr, ct);
                _logger.LogInformation("ApplicantTypes table processed (DB: {Db})", dbName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing DB: {Db}", dbName);
            }
        }
    }

    private async Task<List<string>> GetTenantConnectionStringsAsync(CancellationToken ct)
    {
        var result = new List<string>();
        await using var connection = new SqlConnection(_masterConnectionString);
        await connection.OpenAsync(ct);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = TenantsQuerySql;
        cmd.CommandTimeout = CommandTimeoutSeconds;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cs = reader["ConnectionString"]?.ToString();
            if (!string.IsNullOrWhiteSpace(cs)) result.Add(cs!);
        }
        return result;
    }

    private async Task EnsureApplicantTypesTableAsync(string connectionString, CancellationToken ct)
    {
        var fullName = $"[{SchemaName}].[{TableName}]";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        // 1) 테이블 존재 여부: 스키마 포함 OBJECT_ID 사용
        var exists = await ExecuteScalarIntAsync(connection,
            $"SELECT CASE WHEN OBJECT_ID(N'{fullName}', N'U') IS NULL THEN 0 ELSE 1 END",
            ct) == 1;

        if (!exists)
        {
            // 2) 생성 (권장 스키마: Name 200, CreatedAt 정밀도 7)
            var createSql = $@"
CREATE TABLE {fullName}(
  [Id]        BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_{TableName} PRIMARY KEY,
  [Active]    BIT NULL CONSTRAINT DF_{TableName}_Active DEFAULT(1),
  [CreatedAt] DATETIMEOFFSET(7) NULL CONSTRAINT DF_{TableName}_CreatedAt DEFAULT SYSDATETIMEOFFSET(),
  [CreatedBy] NVARCHAR(255) NULL,
  [Name]      NVARCHAR(200) NULL
);";
            await ExecuteNonQueryAsync(connection, createSql, ct);
            _logger.LogInformation("{Table} created.", fullName);

            // 데이터 위생: 공백 금지 + 유니크
            string hygieneSql = $@"
ALTER TABLE {fullName}
  ADD CONSTRAINT CK_{TableName}_Name_NotBlank CHECK (LEN(LTRIM(RTRIM(ISNULL([Name], '')))) > 0);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_{TableName}_Name' AND object_id = OBJECT_ID(N'{fullName}'))
  CREATE UNIQUE INDEX UX_{TableName}_Name ON {fullName}([Name]);";
            await ExecuteNonQueryAsync(connection, hygieneSql, ct);
        }
        else
        {
            // 3) 누락 컬럼 보충 (정의 일치)
            var expected = new Dictionary<string, string>
            {
                ["Active"] = "BIT NULL CONSTRAINT DF_{T}_Active DEFAULT(1)",
                ["CreatedAt"] = "DATETIMEOFFSET(7) NULL CONSTRAINT DF_{T}_CreatedAt DEFAULT SYSDATETIMEOFFSET()",
                ["CreatedBy"] = "NVARCHAR(255) NULL",
                ["Name"] = "NVARCHAR(200) NULL"
            };

            foreach (var kv in expected)
            {
                var col = kv.Key;
                var spec = kv.Value.Replace("{T}", TableName);

                var colExists = await ExecuteScalarIntAsync(connection, $@"
SELECT CASE WHEN EXISTS (
  SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @col
) THEN 1 ELSE 0 END", ct,
                    ("@schema", SchemaName), ("@table", TableName), ("@col", col)) == 1;

                if (colExists) continue;

                await ExecuteNonQueryAsync(connection, $"ALTER TABLE {fullName} ADD [{col}] {spec};", ct);
                _logger.LogInformation("Column added: {Col} ({Spec})", col, spec);
            }

            // 위생 제약/인덱스 보장
            string hygieneSql = $@"
IF NOT EXISTS (
  SELECT 1 FROM sys.check_constraints 
  WHERE name = 'CK_{TableName}_Name_NotBlank' AND parent_object_id = OBJECT_ID(N'{fullName}')
)
  ALTER TABLE {fullName}
    ADD CONSTRAINT CK_{TableName}_Name_NotBlank CHECK (LEN(LTRIM(RTRIM(ISNULL([Name], '')))) > 0);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_{TableName}_Name' AND object_id = OBJECT_ID(N'{fullName}'))
  CREATE UNIQUE INDEX UX_{TableName}_Name ON {fullName}([Name]);";
            await ExecuteNonQueryAsync(connection, hygieneSql, ct);
        }

        // 4) 초기 시드 (없는 경우만)
        var count = await ExecuteScalarIntAsync(connection, $"SELECT COUNT(1) FROM {fullName}", ct);
        if (count == 0)
        {
            var seedSql = $@"
INSERT INTO {fullName} (Active, CreatedAt, CreatedBy, Name)
VALUES
 (1, SYSDATETIMEOFFSET(), 'System', N'Vendor'),
 (1, SYSDATETIMEOFFSET(), 'System', N'Employee');";
            var inserted = await ExecuteNonQueryAsync(connection, seedSql, ct);
            _logger.LogInformation("{Inserted} default ApplicantTypes inserted.", inserted);
        }
    }

    private static async Task<int> ExecuteScalarIntAsync(SqlConnection conn, string sql, CancellationToken ct,
        params (string name, object? value)[] parameters)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 60;
        foreach (var p in parameters)
            cmd.Parameters.AddWithValue(p.name, p.value ?? DBNull.Value);
        var obj = await cmd.ExecuteScalarAsync(ct);
        return (obj == null || obj == DBNull.Value) ? 0 : Convert.ToInt32(obj);
    }

    private async Task<int> ExecuteNonQueryAsync(SqlConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = CommandTimeoutSeconds;
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task RunAsync(IServiceProvider services, bool forMaster, CancellationToken ct = default)
    {
        var logger = services.GetRequiredService<ILogger<ApplicantTypesTableBuilder>>();
        var config = services.GetRequiredService<IConfiguration>();
        var masterConnectionString = config.GetConnectionString("DefaultConnection");

        if (string.IsNullOrWhiteSpace(masterConnectionString))
            throw new InvalidOperationException("DefaultConnection is not configured in appsettings.json.");

        var builder = new ApplicantTypesTableBuilder(masterConnectionString, logger);
        if (forMaster)
            await builder.BuildMasterDatabaseAsync(ct);
        else
            await builder.BuildTenantDatabasesAsync(ct);
    }
}
